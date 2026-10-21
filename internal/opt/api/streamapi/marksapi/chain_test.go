package marksapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pgrwl/pgrwl/internal/opt/basebackup/backupdto"
	st "github.com/pgrwl/pgrwl/internal/opt/shared/storecrypt"
	"github.com/pgrwl/pgrwl/internal/opt/supervisors/backupsv"
)

const testWalSegSz = 16 * 1024 * 1024

// The archive fixture is an in-memory WAL archive beside in-memory base backups.
type archive struct {
	wal     *st.InMemoryStorage
	backups *st.InMemoryStorage
	check   *chainCheck
}

func newArchive(t *testing.T) *archive {
	t.Helper()
	a := &archive{wal: st.NewInMemoryStorage(), backups: st.NewInMemoryStorage()}
	a.check = &chainCheck{
		wal:      a.wal,
		backups:  backupsv.NewBackupStore(&backupsv.BackupSupervisorOpts{BasebackupStor: a.backups}),
		walSegSz: testWalSegSz,
	}
	return a
}

func (a *archive) put(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		require.NoError(t, a.wal.Put(context.Background(), n, bytes.NewReader([]byte(n))))
	}
}

// The base method stores a completed base backup manifest starting at lsn on tli.
func (a *archive) base(t *testing.T, id, lsn string, finished time.Time) {
	t.Helper()
	start, err := pglogrepl.ParseLSN(lsn)
	require.NoError(t, err)
	body, err := json.Marshal(&backupdto.Result{
		StartLSN:   start,
		StopLSN:    start + 0x100,
		TimelineID: 1,
		StartedAt:  finished.Add(-time.Minute),
		FinishedAt: finished,
	})
	require.NoError(t, err)
	require.NoError(t, a.backups.Put(context.Background(), id+"/"+id+".json", bytes.NewReader(body)))
}

func missing(t *testing.T, err error) string {
	t.Helper()
	var gap *GapError
	require.True(t, errors.As(err, &gap), "want a gap, got %v", err)
	assert.ErrorIs(t, err, ErrWALGap)
	return gap.Missing
}

func TestChainPassesOnOneContiguousTimeline(t *testing.T) {
	a := newArchive(t)
	a.base(t, "b1", "0/5000028", time.Now())
	a.put(t, "000000010000000000000005", "000000010000000000000006", "000000010000000000000007", "000000010000000000000008.partial")

	assert.NoError(t, a.check.check(context.Background(), "000000010000000000000007"))
}

func TestChainNamesTheFirstMissingSegment(t *testing.T) {
	a := newArchive(t)
	a.base(t, "b1", "0/5000028", time.Now())
	a.put(t, "000000010000000000000005", "000000010000000000000007")

	assert.Equal(t, "000000010000000000000006", missing(t, a.check.check(context.Background(), "000000010000000000000007")))
}

func TestChainDoesNotCountAPartialSegment(t *testing.T) {
	a := newArchive(t)
	a.base(t, "b1", "0/5000028", time.Now())
	a.put(t, "000000010000000000000005", "000000010000000000000006.partial", "000000010000000000000007")

	assert.Equal(t, "000000010000000000000006", missing(t, a.check.check(context.Background(), "000000010000000000000007")))
}

func TestChainFollowsTheHistoryAcrossATimelineSwitch(t *testing.T) {
	a := newArchive(t)
	a.base(t, "b1", "0/2000028", time.Now())
	// Timeline 1 ended inside segment 5, so timeline 2 owns segment 5 onward.
	require.NoError(t, a.wal.Put(context.Background(), "00000002.history", bytes.NewReader([]byte("1\t0/542EA50\tafter LSN 0/542EA50\n"))))
	a.put(t, "000000010000000000000002", "000000010000000000000003", "000000010000000000000004",
		"000000010000000000000005.partial", "000000020000000000000005", "000000020000000000000006")

	assert.NoError(t, a.check.check(context.Background(), "000000020000000000000006"))
}

func TestChainRefusesWithoutTheHistoryFile(t *testing.T) {
	a := newArchive(t)
	a.base(t, "b1", "0/2000028", time.Now())
	a.put(t, "000000010000000000000002", "000000010000000000000003", "000000010000000000000004",
		"000000020000000000000005", "000000020000000000000006")

	assert.Equal(t, "00000002.history", missing(t, a.check.check(context.Background(), "000000020000000000000006")))
}

func TestChainRefusesTheLostSlotHole(t *testing.T) {
	a := newArchive(t)
	a.base(t, "b1", "0/2000028", time.Now())
	require.NoError(t, a.wal.Put(context.Background(), "00000002.history", bytes.NewReader([]byte("1\t0/542EA50\tafter LSN 0/542EA50\n"))))
	// The receiver resumed at segment 61 after the slot was lost; 06 through 60 never arrived.
	a.put(t, "000000010000000000000002", "000000010000000000000003", "000000010000000000000004",
		"000000020000000000000005", "000000020000000000000006.partial", "000000020000000000000061")

	assert.Equal(t, "000000020000000000000006", missing(t, a.check.check(context.Background(), "000000020000000000000061")))
}

func TestChainUsesTheNewestCompletedBase(t *testing.T) {
	a := newArchive(t)
	a.base(t, "old", "0/2000028", time.Now().Add(-time.Hour))
	a.base(t, "new", "0/7000028", time.Now())
	a.put(t, "000000010000000000000007")

	assert.NoError(t, a.check.check(context.Background(), "000000010000000000000007"))
}

func TestChainRefusesWithoutABase(t *testing.T) {
	a := newArchive(t)
	a.put(t, "000000010000000000000007")

	err := a.check.check(context.Background(), "000000010000000000000007")
	assert.Equal(t, "", missing(t, err))
	assert.Contains(t, err.Error(), "no base backup")
}
