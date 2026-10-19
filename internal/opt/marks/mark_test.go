package marks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewNamesTheMarkByTimeAndSegment(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 34, 56, 789, time.FixedZone("x", 3*3600))

	m, err := New("0/3000120", "000000020000000000000003", "before-upgrade", now)

	require.NoError(t, err)
	assert.Equal(t, "20261019093456-000000020000000000000003", m.ID)
	assert.Equal(t, uint32(2), m.Timeline)
	assert.Equal(t, time.Date(2026, 10, 19, 9, 34, 56, 0, time.UTC), m.CreatedAt)
	assert.Equal(t, "before-upgrade", m.Label)
}

func TestNewRejectsNonWALNames(t *testing.T) {
	for _, name := range []string{"", "00000002.history", "000000020000000000000003.partial", "zzzzzzzz0000000000000003"} {
		_, err := New("0/0", name, "", time.Now())
		assert.Error(t, err, name)
	}
}

func TestMarkJSONShape(t *testing.T) {
	m, err := New("0/3000120", "000000010000000000000003", "", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	require.NoError(t, err)

	body, err := json.Marshal(m)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"id": "20260102030405-000000010000000000000003",
		"lsn": "0/3000120",
		"wal_file": "000000010000000000000003",
		"timeline": 1,
		"created_at": "2026-01-02T03:04:05Z"
	}`, string(body))
}
