# REST API

`pgrwl` exposes an HTTP API on the configured `listen_port` in receive mode.

| Method | Path                        | Description                         |
|--------|-----------------------------|-------------------------------------|
| `GET`  | `/healthz`                  | Health check                        |
| `GET`  | `/api/v1/status`            | Receiver status and WAL position    |
| `GET`  | `/api/v1/wals`              | List archived WAL files             |
| `GET`  | `/api/v1/backups`           | List base backups                   |
| `POST` | `/api/v1/basebackup`        | Trigger a manual base backup        |
| `GET`  | `/api/v1/basebackup/status` | Running backup status               |
| `POST` | `/api/v1/marks`             | Create a restore target (a mark)    |
| `GET`  | `/api/v1/marks`             | List marks, oldest first            |
| `GET`  | `/api/v1/redacted-config`   | Active config with secrets redacted |
| `GET`  | `/metrics`                  | Prometheus metrics (if enabled)     |

## Marks

`POST /api/v1/marks` records the current WAL insert position, writes a restore
point, switches WAL, and waits (up to `marks.wal_timeout`) until the closed
segment has reached storage. It answers `201` with the mark, `409` while another
mark is in progress, and `504` if the segment does not arrive in time. The body
is optional: `{"label": "before-upgrade"}`.

```json
{
  "id": "20261019120000-000000010000000000000007",
  "lsn": "0/7000028",
  "wal_file": "000000010000000000000007",
  "timeline": 1,
  "created_at": "2026-10-19T12:00:00Z",
  "label": "before-upgrade"
}
```

To restore to a mark, set `recovery_target_lsn = '<lsn>'` on the restored
cluster. Marks live as plain JSON at `marks/<id>.json` under `main.directory`,
readable with storage credentials alone, and retention deletes a mark together
with the WAL segment it points at.

In serve mode (used during recovery), a single endpoint is available:

| Method | Path                     | Description              |
|--------|--------------------------|--------------------------|
| `GET`  | `/api/v1/wal/{filename}` | Fetch a WAL file by name |
