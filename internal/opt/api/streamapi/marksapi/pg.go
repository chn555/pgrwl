package marksapi

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// The switchResult names the LSN a mark restores to and the archived segment that holds it.
type switchResult struct {
	LSN     string
	WALFile string
}

// The pgSwitchWAL function writes a restore point, then closes the segment so the archive holds a record at or past LSN.
func pgSwitchWAL(ctx context.Context) (*switchResult, error) {
	conn, err := pgconn.Connect(ctx, "application_name=pgrwl-marks")
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	lsn, err := queryString(ctx, conn, "SELECT pg_current_wal_insert_lsn()::text")
	if err != nil {
		return nil, err
	}
	if _, err := queryString(ctx, conn, "SELECT pg_create_restore_point('pgrwl-mark')::text"); err != nil {
		return nil, err
	}
	walFile, err := queryString(ctx, conn, "SELECT pg_walfile_name(pg_switch_wal())")
	if err != nil {
		return nil, err
	}

	return &switchResult{LSN: lsn, WALFile: walFile}, nil
}

// The queryString function runs a single-row, single-column query.
func queryString(ctx context.Context, conn *pgconn.PgConn, sql string) (string, error) {
	res := conn.ExecParams(ctx, sql, nil, nil, nil, nil).Read()
	if res.Err != nil {
		return "", fmt.Errorf("%s: %w", sql, res.Err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		return "", fmt.Errorf("%s: expected one value, got %d rows", sql, len(res.Rows))
	}
	return string(res.Rows[0][0]), nil
}
