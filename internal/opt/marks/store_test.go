package marks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	st "github.com/pgrwl/pgrwl/internal/opt/shared/storecrypt"
)

func putMark(t *testing.T, s *Store, walFile string, at time.Time) Mark {
	t.Helper()
	m, err := New("0/1", walFile, "", at)
	require.NoError(t, err)
	require.NoError(t, s.Put(context.Background(), &m))
	return m
}

func TestStoreListSortsByID(t *testing.T) {
	backend := st.NewInMemoryStorage()
	s := &Store{Storage: backend}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	later := putMark(t, s, "000000010000000000000007", t0.Add(time.Hour))
	earlier := putMark(t, s, "000000010000000000000003", t0)
	backend.Files["notes.txt"] = []byte("ignored")

	marks, err := s.List(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []Mark{earlier, later}, marks)
}

func TestStoreListIsEmptyNotNil(t *testing.T) {
	s := &Store{Storage: st.NewInMemoryStorage()}

	marks, err := s.List(context.Background())

	require.NoError(t, err)
	assert.NotNil(t, marks)
	assert.Empty(t, marks)
}

func TestStoreDeleteBeforeKeepsTheBoundarySegment(t *testing.T) {
	backend := st.NewInMemoryStorage()
	s := &Store{Storage: backend}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	old := putMark(t, s, "000000010000000000000003", t0)
	boundary := putMark(t, s, "000000010000000000000005", t0.Add(time.Hour))
	newer := putMark(t, s, "000000010000000000000009", t0.Add(2*time.Hour))
	nextTimeline := putMark(t, s, "000000020000000000000001", t0.Add(3*time.Hour))

	deleted, err := s.DeleteBefore(context.Background(), "000000010000000000000005")

	require.NoError(t, err)
	assert.Equal(t, 1, deleted)
	_, hasOld := backend.Files[old.ID+fileExt]
	assert.False(t, hasOld)
	for _, kept := range []Mark{boundary, newer, nextTimeline} {
		_, ok := backend.Files[kept.ID+fileExt]
		assert.True(t, ok, kept.ID)
	}
}
