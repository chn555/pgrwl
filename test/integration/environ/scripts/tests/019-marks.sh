#!/usr/bin/env bash
set -euo pipefail
. /var/lib/postgresql/scripts/tests/utils.sh

###############################################################################
# Test: marks API and restore to a mark (S3, gzip + AES)
#
# What this verifies:
#
#   1. POST /api/v1/marks answers 201 once the switched segment is in the bucket.
#   2. A second mark on an idle instance (nothing written since the first
#      switch) still closes a fresh segment, so its LSN is reachable too.
#   3. Mark objects land at marks/<id>.json and GET /api/v1/marks lists them
#      in order.
#   4. WAL written after the marks is archived too, so recovery has something
#      beyond the target and must stop at the mark rather than at end of WAL.
#   5. PGDATA is lost; the base backup is restored with
#      recovery_target_lsn = <idle mark lsn>; recovery reaches the target,
#      promotes, and the cluster holds exactly the rows written before it.
#
###############################################################################

API_ADDR="${API_ADDR:-http://127.0.0.1:7070}"
MARKS_URL="${API_ADDR}/api/v1/marks"

x_remake_config() {
  cat <<EOF > "/tmp/config.json"
{
  "main": {
     "listen_port": 7070,
     "directory": "${WAL_PATH}"
  },
  "receiver": {
     "slot": "pgrwl_v5",
     "no_loop": true,
     "uploader": {
       "sync_interval": "2s",
       "max_concurrency": 4
     }
  },
  "log": {
    "level": "${LOG_LEVEL_DEFAULT}",
    "format": "${LOG_FORMAT_DEFAULT}",
    "add_source": true
  },
  "backup": {
    "cron": "*/50 * * * *"
  },
  "marks": {
    "wal_timeout": "60s"
  },
  "storage": {
    "name": "s3",
    "compression": {
      "algo": "gzip"
    },
    "encryption": {
      "algo": "aes-256-gcm",
      "pass": "qwerty123"
    },
    "s3": {
      "url": "https://minio:9000",
      "access_key_id": "minioadmin",
      "secret_access_key": "minioadmin123",
      "bucket": "${TEST_NAME}",
      "region": "main",
      "use_path_style": true,
      "disable_ssl": true
    }
  }
}
EOF
}

x_json_field() {
  python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}

x_http_json() {
  local method="$1" url="$2" body_file="$3"
  shift 3
  curl -sS -X "${method}" -H "Content-Type: application/json" -o "${body_file}" -w "%{http_code}" "$@" "${url}"
}

x_wait_object_in_bucket() {
  local name="$1" timeout="${2:-60}" i
  for ((i = 0; i < timeout; i++)); do
    if [[ -n "$(minio-mc find "local/${TEST_NAME}" --name "${name}" --insecure 2>/dev/null)" ]]; then
      return 0
    fi
    sleep 1
  done
  log_fatal "object ${name} did not appear in bucket ${TEST_NAME} within ${timeout}s"
}

x_test_marks() {
  echo_delim "cleanup"
  x_remake_dirs
  x_remake_config

  echo_delim "start cluster"
  xpg_rebuild
  xpg_start
  xpg_recreate_slots

  echo_delim "start receiver / stream daemon"
  x_start_receiver "/tmp/config.json"
  if ! x_wait_http_ok "${API_ADDR}/healthz" 60; then
    cat "${LOG_FILE}" || true
    log_fatal "stream API did not become ready"
  fi

  echo_delim "creating backup"
  /usr/local/bin/pgrwl backup -c "/tmp/config.json"

  echo_delim "rows before the mark"
  psql -v ON_ERROR_STOP=1 -c "create table public.marked(n int);"
  psql -v ON_ERROR_STOP=1 -c "insert into public.marked select generate_series(1, 100);"

  echo_delim "create a mark"
  local code
  code="$(x_http_json POST "${MARKS_URL}" "/tmp/mark.json" -d '{"label": "after-100"}')"
  cat "/tmp/mark.json"
  if [[ "${code}" != "201" ]]; then
    cat "${LOG_FILE}" || true
    log_fatal "expected POST /api/v1/marks to return 201, got ${code}"
  fi

  local mark_id mark_lsn mark_wal mark_label
  mark_id="$(x_json_field /tmp/mark.json id)"
  mark_lsn="$(x_json_field /tmp/mark.json lsn)"
  mark_wal="$(x_json_field /tmp/mark.json wal_file)"
  mark_label="$(x_json_field /tmp/mark.json label)"
  log_info "mark id=${mark_id} lsn=${mark_lsn} wal=${mark_wal}"
  [[ "${mark_id}" == *"-${mark_wal}" ]] || log_fatal "mark id ${mark_id} does not end with ${mark_wal}"
  [[ "${mark_label}" == "after-100" ]] || log_fatal "mark label is ${mark_label}"

  echo_delim "mark and its segment are in the bucket"
  x_wait_object_in_bucket "${mark_wal}*" 5
  x_wait_object_in_bucket "${mark_id}.json" 5

  echo_delim "create a second mark while the instance is idle"
  sleep 1
  code="$(x_http_json POST "${MARKS_URL}" "/tmp/idle-mark.json")"
  cat "/tmp/idle-mark.json"
  [[ "${code}" == "201" ]] || log_fatal "expected the idle POST /api/v1/marks to return 201, got ${code}"

  local idle_id idle_lsn idle_wal
  idle_id="$(x_json_field /tmp/idle-mark.json id)"
  idle_lsn="$(x_json_field /tmp/idle-mark.json lsn)"
  idle_wal="$(x_json_field /tmp/idle-mark.json wal_file)"
  log_info "idle mark id=${idle_id} lsn=${idle_lsn} wal=${idle_wal}"
  [[ "${idle_wal}" > "${mark_wal}" ]] || log_fatal "idle mark reused segment ${idle_wal}; expected one after ${mark_wal}"
  x_wait_object_in_bucket "${idle_wal}*" 5

  echo_delim "list marks"
  code="$(x_http_json GET "${MARKS_URL}" "/tmp/marks.json")"
  cat "/tmp/marks.json"
  [[ "${code}" == "200" ]] || log_fatal "expected GET /api/v1/marks to return 200, got ${code}"
  python3 - "/tmp/marks.json" "${mark_id}" "${idle_id}" <<'PY'
import json, sys
marks = json.load(open(sys.argv[1]))
assert [m["id"] for m in marks] == sys.argv[2:], marks
PY

  echo_delim "rows after the mark, archived before the restore"
  psql -v ON_ERROR_STOP=1 -c "insert into public.marked select generate_series(101, 300);"
  local after_wal
  after_wal="$(psql -At -c "select pg_walfile_name(pg_current_wal_insert_lsn());")"
  psql -v ON_ERROR_STOP=1 -c "select pg_switch_wal();" >/dev/null
  x_wait_object_in_bucket "${after_wal}*" 60

  echo_delim "teardown"
  x_stop_receiver
  xpg_teardown

  echo_delim "restoring backup to the idle mark"
  /usr/local/bin/pgrwl restore --dest="${PGDATA}" -c "/tmp/config.json"
  chmod 0750 "${PGDATA}"
  chown -R postgres:postgres "${PGDATA}"
  touch "${PGDATA}/recovery.signal"

  xpg_config
  cat <<EOF >>"${PG_CFG}"
restore_command = 'pgrwl restore-command --serve-addr=127.0.0.1:7070 %f %p'
recovery_target_lsn = '${idle_lsn}'
recovery_target_action = 'promote'
EOF

  echo_delim "running wal fetcher"
  x_start_serving "/tmp/config.json"

  echo_delim "running cluster"
  xpg_start
  xpg_wait_is_in_recovery
  cat /var/log/postgresql/pg.log

  echo_delim "verify the restored state"
  grep -q "recovery stopping after WAL location (LSN) \"${idle_lsn}\"" /var/log/postgresql/pg.log \
    || log_fatal "recovery did not stop at ${idle_lsn}"
  if grep -q "recovery ended before configured recovery target was reached" /var/log/postgresql/pg.log; then
    log_fatal "recovery ran out of WAL before the mark"
  fi

  local rows timeline
  rows="$(psql -At -c "select count(*) from public.marked;")"
  timeline="$(psql -At -c "select timeline_id from pg_control_checkpoint();")"
  log_info "rows=${rows} timeline=${timeline}"
  [[ "${rows}" == "100" ]] || log_fatal "expected 100 rows at the idle mark, got ${rows}"
  [[ "${timeline}" == "2" ]] || log_fatal "expected timeline 2 after promote, got ${timeline}"

  echo_delim "check logs"
  x_search_errors_in_logs_or_fatal
  x_print_ok
}

x_test_marks "$@"
