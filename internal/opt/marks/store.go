package marks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	st "github.com/pgrwl/pgrwl/internal/opt/shared/storecrypt"
)

// Subpath is the directory under main.directory that holds marks.
const Subpath = "marks"

const fileExt = ".json"

// Store keeps marks as plain JSON objects so a restore can read them with nothing but storage credentials.
type Store struct {
	Storage st.Storage
}

// Put writes the mark as <id>.json.
func (s *Store) Put(ctx context.Context, m *Mark) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return s.Storage.Put(ctx, m.ID+fileExt, bytes.NewReader(body))
}

// List returns every mark sorted by ID ascending, which is creation order.
func (s *Store) List(ctx context.Context) ([]Mark, error) {
	infos, err := s.Storage.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list marks: %w", err)
	}

	marks := make([]Mark, 0, len(infos))
	for _, fi := range infos {
		if !strings.HasSuffix(fi.Path, fileExt) {
			continue
		}
		m, err := s.read(ctx, fi.Path)
		if err != nil {
			return nil, err
		}
		marks = append(marks, m)
	}

	slices.SortFunc(marks, func(a, b Mark) int { return strings.Compare(a.ID, b.ID) })
	return marks, nil
}

// DeleteBefore removes every mark whose segment sorts before keepFromWAL and returns how many it removed.
func (s *Store) DeleteBefore(ctx context.Context, keepFromWAL string) (int, error) {
	marks, err := s.List(ctx)
	if err != nil {
		return 0, err
	}

	deleted := 0
	for _, m := range marks {
		if strings.ToUpper(m.WALFile) >= strings.ToUpper(keepFromWAL) {
			continue
		}
		if err := s.Storage.Delete(ctx, m.ID+fileExt); err != nil {
			return deleted, fmt.Errorf("delete mark %s: %w", m.ID, err)
		}
		deleted++
	}
	return deleted, nil
}

// The read method decodes one stored mark.
func (s *Store) read(ctx context.Context, path string) (Mark, error) {
	rc, err := s.Storage.Get(ctx, path)
	if err != nil {
		return Mark{}, fmt.Errorf("read mark %s: %w", path, err)
	}
	defer rc.Close()

	var m Mark
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return Mark{}, fmt.Errorf("decode mark %s: %w", path, err)
	}
	return m, nil
}
