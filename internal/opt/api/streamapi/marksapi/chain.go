package marksapi

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"

	"github.com/pgrwl/pgrwl/internal/core/conv"
	"github.com/pgrwl/pgrwl/internal/core/xlog"
	"github.com/pgrwl/pgrwl/internal/opt/basebackup/backupdto"
	st "github.com/pgrwl/pgrwl/internal/opt/shared/storecrypt"
)

// ErrWALGap says the archive cannot replay from the newest base backup to the mark.
var ErrWALGap = errors.New("the archive cannot replay to the mark")

// BackupLister reads the base backups the archive holds; backupsv.BackupStore satisfies it.
type BackupLister interface {
	ListBackupDirs(ctx context.Context) (map[string]bool, error)
	ReadManifest(ctx context.Context, backupID string) (*backupdto.Result, error)
}

// A GapError names the first archive file a restore to the mark would ask for and not find.
type GapError struct {
	Missing string
}

func (e *GapError) Error() string {
	if e.Missing == "" {
		return ErrWALGap.Error() + ": no base backup"
	}
	return ErrWALGap.Error() + ": " + e.Missing + " is not in the archive"
}

func (e *GapError) Is(target error) bool { return target == ErrWALGap }

// The chainCheck holds what one check of the archive needs.
type chainCheck struct {
	wal      st.Storage
	backups  BackupLister
	walSegSz uint64
}

// The check method walks the segments a restore from the newest base backup to the
// mark's segment would replay, across timeline switches, and reports the first one missing.
func (c *chainCheck) check(ctx context.Context, markWAL string) error {
	base, err := newestBase(ctx, c.backups)
	if err != nil {
		return err
	}
	if base == nil {
		return &GapError{}
	}
	baseTLI, baseLSN := baseStart(base)

	markTLI, markSeg, err := xlog.XLogFromFileName(markWAL, c.walSegSz)
	if err != nil {
		return err
	}

	have, err := c.archived(ctx)
	if err != nil {
		return err
	}

	// A timeline's history file lists where each ancestor ended; the mark's names them all.
	endLSN := map[uint32]pglogrepl.LSN{}
	for tli := baseTLI + 1; tli <= markTLI; tli++ {
		name := fmt.Sprintf("%08X.history", tli)
		if !have[name] {
			return &GapError{Missing: name}
		}
		if tli == markTLI {
			if endLSN, err = c.history(ctx, name); err != nil {
				return err
			}
		}
	}

	for tli := baseTLI; tli <= markTLI; tli++ {
		first := xlog.XLByteToSeg(uint64(baseLSN), c.walSegSz)
		if tli > baseTLI {
			// PostgreSQL copies the segment holding the switch point onto the new timeline.
			first = xlog.XLByteToSeg(uint64(endLSN[tli-1]), c.walSegSz)
		}
		last := markSeg
		if tli < markTLI {
			end, ok := endLSN[tli]
			if !ok {
				return fmt.Errorf("%08X.history does not say where timeline %d ended", markTLI, tli)
			}
			last = xlog.XLByteToSeg(uint64(end), c.walSegSz) - 1
		}
		for seg := first; seg <= last; seg++ {
			if name := xlog.XLogFileName(tli, seg, c.walSegSz); !have[name] {
				return &GapError{Missing: name}
			}
		}
	}
	return nil
}

// The archived method lists the archive once, by logical name, so a check costs one listing.
func (c *chainCheck) archived(ctx context.Context) (map[string]bool, error) {
	files, err := c.wal.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list the archive: %w", err)
	}
	have := make(map[string]bool, len(files))
	for _, f := range files {
		have[strings.TrimPrefix(f.Path, "/")] = true
	}
	return have, nil
}

// The history method reads a timeline history file into the LSN each listed timeline ended at.
func (c *chainCheck) history(ctx context.Context, name string) (map[uint32]pglogrepl.LSN, error) {
	rc, err := c.wal.Get(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	defer rc.Close()

	ends := map[uint32]pglogrepl.LSN{}
	scanner := bufio.NewScanner(rc)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		tli64, err := strconv.ParseUint(fields[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%s: timeline %q: %w", name, fields[0], err)
		}
		tli, err := conv.Uint64ToUint32(tli64)
		if err != nil {
			return nil, fmt.Errorf("%s: timeline %q: %w", name, fields[0], err)
		}
		lsn, err := pglogrepl.ParseLSN(fields[1])
		if err != nil {
			return nil, fmt.Errorf("%s: switch point %q: %w", name, fields[1], err)
		}
		ends[tli] = lsn
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	return ends, nil
}

// The newestBase function picks the completed base backup that finished last; nil when none.
func newestBase(ctx context.Context, backups BackupLister) (*backupdto.Result, error) {
	dirs, err := backups.ListBackupDirs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list base backups: %w", err)
	}

	var newest *backupdto.Result
	for dir := range dirs {
		info, err := backups.ReadManifest(ctx, dir)
		if err != nil || info.FinishedAt.IsZero() || info.StartLSN == 0 {
			continue
		}
		if newest == nil || info.FinishedAt.After(newest.FinishedAt) {
			newest = info
		}
	}
	return newest, nil
}

// The baseStart function returns where a base backup's WAL begins, preferring the
// server's own manifest when it carries a range.
func baseStart(info *backupdto.Result) (uint32, pglogrepl.LSN) {
	tli, lsn := conv.ToUint32(info.TimelineID), info.StartLSN
	if info.Manifest == nil {
		return tli, lsn
	}
	for _, r := range info.Manifest.WALRanges {
		parsed, err := pglogrepl.ParseLSN(r.StartLSN)
		if err != nil || parsed == 0 {
			continue
		}
		if r.Timeline != 0 {
			tli = conv.ToUint32(r.Timeline)
		}
		return tli, parsed
	}
	return tli, lsn
}
