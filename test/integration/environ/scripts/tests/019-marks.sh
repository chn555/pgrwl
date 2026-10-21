#!/usr/bin/env bash
set -euo pipefail
. /var/lib/postgresql/scripts/tests/utils.sh

###############################################################################
# Test: marks API, retention of marks, and restore to a mark (S3 via its own
# toxiproxy proxy, gzip + AES)
#
# What this verifies:
#
#   1. POST /api/v1/marks answers 201 once the switched segment is in the bucket.
#   2. A second POST while a mark is in progress answers 409; the first still
#      completes. MinIO is slowed through toxiproxy so the window is wide.
#   3. A mark on an idle instance (nothing written since the last switch)
#      still closes a fresh segment, so its LSN is reachable too.
#   4. Mark objects are plain JSON at marks/<id>.json although WAL is stored
#      gzip + AES, and GET /api/v1/marks lists them in id order with labels.
#   5. Recovery-window retention, run by a manual basebackup, deletes the mark
#      whose WAL it deleted and keeps the marks whose WAL it kept.
#   6. With a segment between the newest base backup and the mark gone from
#      the bucket, POST /api/v1/marks answers 409 wal_gap naming it; a base
#      backup taken after the hole cures it and the next mark answers 201.
#   7. PGDATA is lost; that base backup is restored with
#      recovery_target_lsn = <cured mark lsn>; recovery reaches the target,
#      promotes, and the cluster holds exactly the rows written before it.
#
###############################################################################

# This test owns its own toxiproxy proxy so the latency toxic does not touch
# 012's shared minio_s3 proxy when the suite runs in parallel. 012's reset at
# its start still clears every toxic, but it runs seconds before ours is added.
export TOXIPROXY_MINIO_PROXY="minio_s3_marks"
export TOXIPROXY_MINIO_LISTEN="0.0.0.0:9006"
MINIO_PROXY_URL="https://toxiproxy:9006"

API_ADDR="${API_ADDR:-http://127.0.0.1:7070}"
MARKS_URL="${API_ADDR}/api/v1/marks"
BACKUPS_URL="${API_ADDR}/api/v1/backups"
BASEBACKUP_START_URL="${API_ADDR}/api/v1/basebackup"
BASEBACKUP_STATUS_URL="${API_ADDR}/api/v1/basebackup/status"
RETENTION_WINDOW_SECONDS=3

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
    "cron": "59 23 31 12 *"
  },
  "retention": {
    "enable": true,
    "type": "recovery_window",
    "value": "${RETENTION_WINDOW_SECONDS}s",
    "keep_last": 1
  },
  "marks": {
    "wal_timeout": "30s"
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
      "url": "${MINIO_PROXY_URL}",
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
  python3 -c 'import json, sys; print(json.load(open(sys.argv[1])).get(sys.argv[2], ""))' "$1" "$2"
}

x_http_json() {
  local method="$1" url="$2" body_file="$3"
  shift 3
  curl -sS -X "${method}" -H "Content-Type: application/json" -o "${body_file}" -w "%{http_code}" "$@" "${url}"
}

x_bucket_find() {
  minio-mc find "local/${TEST_NAME}" --name "$1" --insecure 2>/dev/null || true
}

x_wait_object_in_bucket() {
  local name="$1" timeout="${2:-60}" i
  for ((i = 0; i < timeout; i++)); do
    if [[ -n "$(x_bucket_find "${name}")" ]]; then
      return 0
    fi
    sleep 1
  done
  log_fatal "object ${name} did not appear in bucket ${TEST_NAME} within ${timeout}s"
}

x_toxiproxy_setup_marks_proxy() {
  x_wait_toxiproxy_up
  x_toxiproxy_delete_minio_proxy
  x_toxiproxy_create_minio_proxy
  x_toxiproxy_enable_minio
}

x_toxiproxy_slow_minio() {
  log_info "adding ${1}ms of latency to minio through toxiproxy"
  curl -fsS -X POST "${TOXIPROXY_API}/proxies/${TOXIPROXY_MINIO_PROXY}/toxics" \
    -H 'Content-Type: application/json' \
    -d "{\"name\": \"slow\", \"type\": \"latency\", \"stream\": \"upstream\", \"attributes\": {\"latency\": ${1}}}" >/dev/null
}

x_toxiproxy_unslow_minio() {
  log_info "removing the latency toxic"
  curl -fsS -X DELETE "${TOXIPROXY_API}/proxies/${TOXIPROXY_MINIO_PROXY}/toxics/slow" >/dev/null
}

# x_create_mark POSTs a mark with the given label (empty for none) into the
# given file and fails unless the daemon answers 201.
x_create_mark() {
  local label="$1" out="$2" code
  if [[ -n "${label}" ]]; then
    code="$(x_http_json POST "${MARKS_URL}" "${out}" -d "{\"label\": \"${label}\"}")"
  else
    code="$(x_http_json POST "${MARKS_URL}" "${out}")"
  fi
  cat "${out}"
  echo
  if [[ "${code}" != "201" ]]; then
    cat "${LOG_FILE}" || true
    log_fatal "expected POST /api/v1/marks to return 201, got ${code}"
  fi
}

x_wait_basebackup_finished() {
  local timeout="${1:-180}" i status
  for ((i = 1; i <= timeout; i++)); do
    if [[ "$(x_http_json GET "${BASEBACKUP_STATUS_URL}" "/tmp/basebackup-status.json")" == "200" ]]; then
      status="$(x_json_field /tmp/basebackup-status.json status)"
      case "${status}" in
        succeeded) return 0 ;;
        failed) cat /tmp/basebackup-status.json; log_fatal "basebackup failed" ;;
      esac
    fi
    sleep 1
  done
  log_fatal "basebackup did not finish in ${timeout}s"
}

x_test_marks() {
  echo_delim "cleanup"
  x_remake_dirs
  x_remake_config
  x_toxiproxy_setup_marks_proxy

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

  echo_delim "first base backup"
  /usr/local/bin/pgrwl backup -c "/tmp/config.json"

  echo_delim "rows before the early mark"
  psql -v ON_ERROR_STOP=1 -c "create table public.marked(n int);"
  psql -v ON_ERROR_STOP=1 -c "insert into public.marked select generate_series(1, 50);"

  echo_delim "early mark, and a concurrent POST that must get 409"
  x_toxiproxy_slow_minio 1000
  x_create_mark "early" "/tmp/early-mark.json" &
  local first_pid=$!
  sleep 0.5
  local code
  code="$(x_http_json POST "${MARKS_URL}" "/tmp/conflict.json")"
  cat "/tmp/conflict.json"
  echo
  [[ "${code}" == "409" ]] || log_fatal "expected a concurrent POST /api/v1/marks to return 409, got ${code}"
  wait "${first_pid}"
  x_toxiproxy_unslow_minio

  local early_id early_wal
  early_id="$(x_json_field /tmp/early-mark.json id)"
  early_wal="$(x_json_field /tmp/early-mark.json wal_file)"
  log_info "early mark id=${early_id} wal=${early_wal}"

  echo_delim "second base backup, so retention can anchor past the early mark"
  /usr/local/bin/pgrwl backup -c "/tmp/config.json"
  local backup_started_at
  backup_started_at="$(date +%s)"

  echo_delim "rows before the mark to restore to"
  psql -v ON_ERROR_STOP=1 -c "insert into public.marked select generate_series(51, 100);"
  x_create_mark "after-100" "/tmp/mark.json"

  local mark_id mark_lsn mark_wal
  mark_id="$(x_json_field /tmp/mark.json id)"
  mark_lsn="$(x_json_field /tmp/mark.json lsn)"
  mark_wal="$(x_json_field /tmp/mark.json wal_file)"
  log_info "mark id=${mark_id} lsn=${mark_lsn} wal=${mark_wal}"
  [[ "${mark_id}" == *"-${mark_wal}" ]] || log_fatal "mark id ${mark_id} does not end with ${mark_wal}"

  echo_delim "idle mark: nothing written since the last switch"
  sleep 1
  x_create_mark "" "/tmp/idle-mark.json"

  local idle_id idle_lsn idle_wal
  idle_id="$(x_json_field /tmp/idle-mark.json id)"
  idle_lsn="$(x_json_field /tmp/idle-mark.json lsn)"
  idle_wal="$(x_json_field /tmp/idle-mark.json wal_file)"
  log_info "idle mark id=${idle_id} lsn=${idle_lsn} wal=${idle_wal}"
  [[ "${idle_wal}" > "${mark_wal}" ]] || log_fatal "idle mark reused segment ${idle_wal}; expected one after ${mark_wal}"

  echo_delim "marks are plain JSON in the bucket while WAL is gzip + AES"
  x_wait_object_in_bucket "${mark_wal}*" 5
  x_wait_object_in_bucket "${idle_wal}*" 5
  local wal_key mark_key
  wal_key="$(x_bucket_find "${mark_wal}*")"
  mark_key="$(x_bucket_find "${mark_id}.json")"
  log_info "wal key=${wal_key} mark key=${mark_key}"
  [[ "${wal_key}" == *"/${mark_wal}.gz.aes" ]] || log_fatal "WAL object ${wal_key} is not gzip + AES"
  [[ "${mark_key}" == *"/marks/${mark_id}.json" ]] || log_fatal "mark object ${mark_key} is not marks/<id>.json"
  minio-mc cat "${mark_key}" --insecure > /tmp/mark-from-bucket.json
  cat /tmp/mark-from-bucket.json
  echo
  [[ "$(x_json_field /tmp/mark-from-bucket.json lsn)" == "${mark_lsn}" ]] \
    || log_fatal "mark object in the bucket is not the plain JSON the API returned"

  echo_delim "list marks: id order, labels"
  code="$(x_http_json GET "${MARKS_URL}" "/tmp/marks.json")"
  cat "/tmp/marks.json"
  echo
  [[ "${code}" == "200" ]] || log_fatal "expected GET /api/v1/marks to return 200, got ${code}"
  python3 - "/tmp/marks.json" "${early_id}" "${mark_id}" "${idle_id}" <<'PY'
import json, sys
marks = json.load(open(sys.argv[1]))
assert [m["id"] for m in marks] == sys.argv[2:], marks
assert [m["id"] for m in marks] == sorted(m["id"] for m in marks), marks
assert [m.get("label") for m in marks] == ["early", "after-100", None], marks
PY

  echo_delim "rows after the marks, archived before the restore"
  psql -v ON_ERROR_STOP=1 -c "insert into public.marked select generate_series(101, 300);"
  local after_wal
  after_wal="$(psql -At -c "select pg_walfile_name(pg_current_wal_insert_lsn());")"
  psql -v ON_ERROR_STOP=1 -c "select pg_switch_wal();" >/dev/null
  x_wait_object_in_bucket "${after_wal}*" 60

  echo_delim "retention through a manual basebackup"
  local restore_backup_id
  code="$(x_http_json GET "${BACKUPS_URL}" "/tmp/backups.json")"
  [[ "${code}" == "200" ]] || log_fatal "expected GET /api/v1/backups to return 200, got ${code}"
  restore_backup_id="$(python3 -c 'import json, sys; print(max(b["label"] for b in json.load(open(sys.argv[1])) if b["status"] == "completed").removeprefix("pgrwl_"))' /tmp/backups.json)"
  log_info "second backup id=${restore_backup_id}"
  # The second backup has to fall outside the recovery window to become the anchor.
  local elapsed
  elapsed=$(( $(date +%s) - backup_started_at ))
  if (( elapsed <= RETENTION_WINDOW_SECONDS )); then
    sleep $(( RETENTION_WINDOW_SECONDS - elapsed + 1 ))
  fi
  code="$(x_http_json POST "${BASEBACKUP_START_URL}" "/tmp/basebackup-start.json")"
  [[ "${code}" == "200" ]] || log_fatal "expected POST /api/v1/basebackup to return 200, got ${code}"
  x_wait_basebackup_finished 180

  echo_delim "retention deleted the early mark with its WAL and kept the rest"
  [[ -z "$(x_bucket_find "${early_wal}*")" ]] || log_fatal "retention kept WAL ${early_wal}"
  [[ -z "$(x_bucket_find "${early_id}.json")" ]] || log_fatal "retention kept the early mark although its WAL went"
  [[ -n "$(x_bucket_find "${mark_wal}*")" ]] || log_fatal "retention deleted WAL ${mark_wal}"
  [[ -n "$(x_bucket_find "${mark_id}.json")" ]] || log_fatal "retention deleted the mark whose WAL stayed"
  [[ -n "$(x_bucket_find "${idle_id}.json")" ]] || log_fatal "retention deleted the idle mark whose WAL stayed"
  code="$(x_http_json GET "${MARKS_URL}" "/tmp/marks-after-retention.json")"
  cat "/tmp/marks-after-retention.json"
  echo
  [[ "${code}" == "200" ]] || log_fatal "expected GET /api/v1/marks to return 200, got ${code}"
  python3 - "/tmp/marks-after-retention.json" "${mark_id}" "${idle_id}" <<'PY'
import json, sys
marks = json.load(open(sys.argv[1]))
assert [m["id"] for m in marks] == sys.argv[2:], marks
PY

  echo_delim "a segment after the newest base backup goes missing from the bucket"
  psql -v ON_ERROR_STOP=1 -c "insert into public.marked select generate_series(301, 400);"
  local hole_wal hole_key
  hole_wal="$(psql -At -c "select pg_walfile_name(pg_current_wal_insert_lsn());")"
  psql -v ON_ERROR_STOP=1 -c "select pg_switch_wal();" >/dev/null
  x_wait_object_in_bucket "${hole_wal}*" 60
  hole_key="$(x_bucket_find "${hole_wal}*")"
  log_info "removing ${hole_key}"
  minio-mc rm "${hole_key}" --insecure

  echo_delim "the mark is refused with 409 wal_gap naming the missing segment"
  code="$(x_http_json POST "${MARKS_URL}" "/tmp/gap.json" -d '{"label": "past-the-hole"}')"
  cat "/tmp/gap.json"
  echo
  [[ "${code}" == "409" ]] || log_fatal "expected POST /api/v1/marks past a hole to return 409, got ${code}"
  [[ "$(x_json_field /tmp/gap.json status)" == "wal_gap" ]] || log_fatal "expected status wal_gap"
  [[ "$(x_json_field /tmp/gap.json missing)" == "${hole_wal}" ]] || log_fatal "expected missing ${hole_wal}"
  [[ -z "$(x_bucket_find "*past-the-hole*")" ]] || log_fatal "a refused mark was stored"

  echo_delim "a base backup after the hole cures it"
  code="$(x_http_json POST "${BASEBACKUP_START_URL}" "/tmp/basebackup-start.json")"
  [[ "${code}" == "200" ]] || log_fatal "expected POST /api/v1/basebackup to return 200, got ${code}"
  x_wait_basebackup_finished 180
  local cured_backup_id
  code="$(x_http_json GET "${BACKUPS_URL}" "/tmp/backups.json")"
  [[ "${code}" == "200" ]] || log_fatal "expected GET /api/v1/backups to return 200, got ${code}"
  cured_backup_id="$(python3 -c 'import json, sys; print(max(b["label"] for b in json.load(open(sys.argv[1])) if b["status"] == "completed").removeprefix("pgrwl_"))' /tmp/backups.json)"
  log_info "cured backup id=${cured_backup_id}"
  x_create_mark "cured" "/tmp/cured-mark.json"
  local cured_lsn
  cured_lsn="$(x_json_field /tmp/cured-mark.json lsn)"
  log_info "cured mark lsn=${cured_lsn}"

  echo_delim "teardown"
  x_stop_receiver
  xpg_teardown

  echo_delim "restoring the cured backup to the cured mark"
  /usr/local/bin/pgrwl restore --id="${cured_backup_id}" --dest="${PGDATA}" -c "/tmp/config.json"
  chmod 0750 "${PGDATA}"
  chown -R postgres:postgres "${PGDATA}"
  touch "${PGDATA}/recovery.signal"

  xpg_config
  cat <<EOF >>"${PG_CFG}"
restore_command = 'pgrwl restore-command --serve-addr=127.0.0.1:7070 %f %p'
recovery_target_lsn = '${cured_lsn}'
recovery_target_action = 'promote'
EOF

  echo_delim "running wal fetcher"
  x_start_serving "/tmp/config.json"

  echo_delim "running cluster"
  xpg_start
  xpg_wait_is_in_recovery
  cat /var/log/postgresql/pg.log

  echo_delim "verify the restored state"
  grep -q "recovery stopping after WAL location (LSN) \"${cured_lsn}\"" /var/log/postgresql/pg.log \
    || log_fatal "recovery did not stop at ${cured_lsn}"
  if grep -q "recovery ended before configured recovery target was reached" /var/log/postgresql/pg.log; then
    log_fatal "recovery ran out of WAL before the mark"
  fi

  local rows timeline
  rows="$(psql -At -c "select count(*) from public.marked;")"
  timeline="$(psql -At -c "select timeline_id from pg_control_checkpoint();")"
  log_info "rows=${rows} timeline=${timeline}"
  [[ "${rows}" == "400" ]] || log_fatal "expected 400 rows at the cured mark, got ${rows}"
  [[ "${timeline}" == "2" ]] || log_fatal "expected timeline 2 after promote, got ${timeline}"

  echo_delim "check logs"
  x_search_errors_in_logs_or_fatal
  x_print_ok
}

x_test_marks "$@"
