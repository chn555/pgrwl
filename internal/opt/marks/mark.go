package marks

import (
	"fmt"
	"time"

	"github.com/pgrwl/pgrwl/internal/core/xlog"
)

// Mark is a restore target: the LSN to recover to and the archived segment that holds it.
type Mark struct {
	ID        string    `json:"id"`
	LSN       string    `json:"lsn"`
	WALFile   string    `json:"wal_file"`
	Timeline  uint32    `json:"timeline"`
	CreatedAt time.Time `json:"created_at"`
	Label     string    `json:"label,omitempty"`
}

// New builds a mark whose ID sorts by creation time, then by segment name.
func New(lsn, walFile, label string, now time.Time) (Mark, error) {
	if !xlog.IsXLogFileName(walFile) {
		return Mark{}, fmt.Errorf("not a WAL file name: %q", walFile)
	}

	var timeline uint32
	if _, err := fmt.Sscanf(walFile[:8], "%08X", &timeline); err != nil {
		return Mark{}, fmt.Errorf("timeline of %q: %w", walFile, err)
	}

	now = now.UTC().Truncate(time.Second)

	return Mark{
		ID:        now.Format("20060102150405") + "-" + walFile,
		LSN:       lsn,
		WALFile:   walFile,
		Timeline:  timeline,
		CreatedAt: now,
		Label:     label,
	}, nil
}
