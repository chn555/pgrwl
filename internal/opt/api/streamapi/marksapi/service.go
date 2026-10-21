package marksapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/pgrwl/pgrwl/internal/opt/marks"
	st "github.com/pgrwl/pgrwl/internal/opt/shared/storecrypt"
)

var (
	ErrMarkInProgress = errors.New("a mark is already in progress")
	ErrWALTimeout     = errors.New("timed out waiting for the WAL segment to reach storage")
)

const walPollInterval = time.Second

type Service interface {
	Create(ctx context.Context, label string) (*marks.Mark, error)
	List(ctx context.Context) ([]marks.Mark, error)
}

var _ Service = &svc{}

type svc struct {
	l          *slog.Logger
	store      *marks.Store
	walStor    st.Storage
	backups    BackupLister
	walTimeout time.Duration
	walPoll    time.Duration
	switchWAL  func(ctx context.Context) (*switchResult, error)
	busy       atomic.Bool
}

func NewService(opts *Opts) Service {
	return &svc{
		l:          slog.With("component", "marks"),
		store:      opts.Store,
		walStor:    opts.WALStor,
		backups:    opts.Backups,
		walTimeout: opts.WALTimeout,
		walPoll:    walPollInterval,
		switchWAL:  pgSwitchWAL,
	}
}

// Create switches WAL, waits until the closed segment is archived and the archive can
// replay to it from the newest base backup, then stores the mark.
func (s *svc) Create(ctx context.Context, label string) (*marks.Mark, error) {
	if !s.busy.CompareAndSwap(false, true) {
		return nil, ErrMarkInProgress
	}
	defer s.busy.Store(false)

	sw, err := s.switchWAL(ctx)
	if err != nil {
		return nil, fmt.Errorf("switch WAL: %w", err)
	}

	// Both waits share one timeout; the mark's own upload runs on the caller's context.
	waitCtx, cancel := context.WithTimeout(ctx, s.walTimeout)
	defer cancel()

	if err := s.waitForWAL(waitCtx, sw.WALFile); err != nil {
		return nil, err
	}
	if err := s.waitForChain(waitCtx, sw); err != nil {
		return nil, err
	}

	m, err := marks.New(sw.LSN, sw.WALFile, label, time.Now())
	if err != nil {
		return nil, err
	}
	if err := s.store.Put(ctx, &m); err != nil {
		return nil, fmt.Errorf("store mark: %w", err)
	}

	s.l.Info("mark created",
		slog.String("id", m.ID),
		slog.String("lsn", m.LSN),
		slog.String("wal_file", m.WALFile),
	)
	return &m, nil
}

func (s *svc) List(ctx context.Context) ([]marks.Mark, error) {
	return s.store.List(ctx)
}

// The waitForChain method polls the chain check until it passes, since the segments
// before the mark's may still be uploading, and reports a gap that outlasts the timeout.
func (s *svc) waitForChain(ctx context.Context, sw *switchResult) error {
	chain := &chainCheck{wal: s.walStor, backups: s.backups, walSegSz: sw.WalSegSz}
	tick := time.NewTicker(s.walPoll)
	defer tick.Stop()

	var last *GapError
	for {
		err := chain.check(ctx, sw.WALFile)
		var gap *GapError
		switch {
		case err == nil:
			return nil
		case errors.As(err, &gap) && gap.Missing != "":
			last = gap
		case ctx.Err() != nil && last != nil:
			return last
		default:
			return err
		}

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return last
			}
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// The waitForWAL method polls storage until walFile exists in any compression or encryption variant.
func (s *svc) waitForWAL(ctx context.Context, walFile string) error {
	tick := time.NewTicker(s.walPoll)
	defer tick.Stop()

	for {
		ok, err := s.walStor.Exists(ctx, walFile)
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("check %s in storage: %w", walFile, err)
		}
		if ok {
			return nil
		}

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("%w: %s", ErrWALTimeout, walFile)
			}
			return ctx.Err()
		case <-tick.C:
		}
	}
}
