package marksapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pgrwl/pgrwl/internal/opt/basebackup/backupdto"
	"github.com/pgrwl/pgrwl/internal/opt/marks"
	st "github.com/pgrwl/pgrwl/internal/opt/shared/storecrypt"
	"github.com/pgrwl/pgrwl/internal/opt/supervisors/backupsv"
)

const testWAL = "000000010000000000000007"

// The fixture wires a handler to in-memory WAL, mark and base backup storage and a
// scripted WAL switch; the base backup starts in the test segment, so it alone is needed.
type fixture struct {
	handler  *Handler
	svc      *svc
	walFiles *st.InMemoryStorage
	marks    *st.InMemoryStorage
	backups  *st.InMemoryStorage
}

func newFixture(t *testing.T, switchWAL func(ctx context.Context) (*switchResult, error)) *fixture {
	t.Helper()

	walFiles := st.NewInMemoryStorage()
	// Exists only consults which variants are configured, so an empty gzip pair is enough.
	walStor, err := st.NewVariadicStorage(walFiles, st.Algorithms{Gzip: &st.CodecPair{}}, "")
	require.NoError(t, err)

	markFiles := st.NewInMemoryStorage()
	backupFiles := st.NewInMemoryStorage()
	base, err := json.Marshal(&backupdto.Result{StartLSN: 0x7000028, StopLSN: 0x7000100, TimelineID: 1, FinishedAt: time.Now()})
	require.NoError(t, err)
	require.NoError(t, backupFiles.Put(context.Background(), "b1/b1.json", bytes.NewReader(base)))

	s := NewService(&Opts{
		Store:      &marks.Store{Storage: markFiles},
		WALStor:    walStor,
		Backups:    backupsv.NewBackupStore(&backupsv.BackupSupervisorOpts{BasebackupStor: backupFiles}),
		WALTimeout: 200 * time.Millisecond,
	})
	impl, ok := s.(*svc)
	require.True(t, ok)
	impl.walPoll = 5 * time.Millisecond
	impl.switchWAL = switchWAL

	return &fixture{handler: NewHandler(s, impl.walTimeout), svc: impl, walFiles: walFiles, marks: markFiles, backups: backupFiles}
}

// The switchToTestWAL stub stands in for pgSwitchWAL and always closes the same segment.
func switchToTestWAL(context.Context) (*switchResult, error) {
	return &switchResult{LSN: "0/7000028", WALFile: testWAL, WalSegSz: testWalSegSz}, nil
}

func (f *fixture) post(body string) *httptest.ResponseRecorder {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(http.MethodPost, "/api/v1/marks", http.NoBody)
	} else {
		r = httptest.NewRequest(http.MethodPost, "/api/v1/marks", strings.NewReader(body))
	}
	w := httptest.NewRecorder()
	f.handler.Create(w, r)
	return w
}

func (f *fixture) get() *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.handler.List(w, httptest.NewRequest(http.MethodGet, "/api/v1/marks", http.NoBody))
	return w
}

func TestCreateWaitsForTheSegmentThenStoresTheMark(t *testing.T) {
	f := newFixture(t, switchToTestWAL)
	go func() {
		time.Sleep(20 * time.Millisecond)
		assert.NoError(t, f.walFiles.Put(context.Background(), testWAL+".gz", bytes.NewReader([]byte("wal"))))
	}()

	w := f.post(`{"label": "before-upgrade"}`)

	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var m marks.Mark
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &m))
	assert.Equal(t, "0/7000028", m.LSN)
	assert.Equal(t, testWAL, m.WALFile)
	assert.Equal(t, uint32(1), m.Timeline)
	assert.Equal(t, "before-upgrade", m.Label)
	assert.True(t, strings.HasSuffix(m.ID, "-"+testWAL), m.ID)
	_, stored := f.marks.Files[m.ID+".json"]
	assert.True(t, stored)
}

func TestCreateAcceptsAnEmptyBody(t *testing.T) {
	f := newFixture(t, switchToTestWAL)
	require.NoError(t, f.walFiles.Put(context.Background(), testWAL, bytes.NewReader([]byte("wal"))))

	w := f.post("")

	assert.Equal(t, http.StatusCreated, w.Code, w.Body.String())
}

func TestCreateRejectsMalformedBody(t *testing.T) {
	f := newFixture(t, switchToTestWAL)

	w := f.post(`{"label": `)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Empty(t, f.marks.Files)
}

func TestCreateRejectsOversizedBodiesAndLabels(t *testing.T) {
	f := newFixture(t, switchToTestWAL)
	require.NoError(t, f.walFiles.Put(context.Background(), testWAL, bytes.NewReader([]byte("wal"))))

	assert.Equal(t, http.StatusBadRequest, f.post(`{"label": "`+strings.Repeat("x", maxLabelLength+1)+`"}`).Code)
	assert.Equal(t, http.StatusBadRequest, f.post(`{"pad": "`+strings.Repeat("x", maxBodyBytes)+`"}`).Code)
	assert.Empty(t, f.marks.Files)

	assert.Equal(t, http.StatusCreated, f.post(`{"label": "`+strings.Repeat("x", maxLabelLength)+`"}`).Code)
}

func TestCreateTimesOutWhenTheSegmentNeverArrives(t *testing.T) {
	f := newFixture(t, switchToTestWAL)

	w := f.post("")

	assert.Equal(t, http.StatusGatewayTimeout, w.Code)
	assert.Contains(t, w.Body.String(), `"status":"timeout"`)
	assert.Empty(t, f.marks.Files)
}

func TestCreateRefusesAMarkTheArchiveCannotReplayTo(t *testing.T) {
	f := newFixture(t, func(context.Context) (*switchResult, error) {
		return &switchResult{LSN: "0/9000028", WALFile: "000000010000000000000009", WalSegSz: testWalSegSz}, nil
	})
	require.NoError(t, f.walFiles.Put(context.Background(), testWAL, bytes.NewReader([]byte("wal"))))
	require.NoError(t, f.walFiles.Put(context.Background(), "000000010000000000000009", bytes.NewReader([]byte("wal"))))

	w := f.post("")

	assert.Equal(t, http.StatusConflict, w.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "wal_gap", body["status"])
	assert.Equal(t, "000000010000000000000008", body["missing"])
	assert.Contains(t, body["error"], "000000010000000000000008")
	assert.Empty(t, f.marks.Files)
}

func TestCreateRefusesAMarkWithoutABaseBackup(t *testing.T) {
	f := newFixture(t, switchToTestWAL)
	require.NoError(t, f.walFiles.Put(context.Background(), testWAL, bytes.NewReader([]byte("wal"))))
	require.NoError(t, f.backups.Delete(context.Background(), "b1/b1.json"))

	w := f.post("")

	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), `"status":"wal_gap"`)
	assert.NotContains(t, w.Body.String(), `"missing"`)
	assert.Empty(t, f.marks.Files)
}

func TestCreateWaitsForTheSegmentsBeforeTheMark(t *testing.T) {
	f := newFixture(t, func(context.Context) (*switchResult, error) {
		return &switchResult{LSN: "0/8000028", WALFile: "000000010000000000000008", WalSegSz: testWalSegSz}, nil
	})
	require.NoError(t, f.walFiles.Put(context.Background(), "000000010000000000000008", bytes.NewReader([]byte("wal"))))
	go func() {
		time.Sleep(20 * time.Millisecond)
		assert.NoError(t, f.walFiles.Put(context.Background(), testWAL, bytes.NewReader([]byte("wal"))))
	}()

	assert.Equal(t, http.StatusCreated, f.post("").Code, "the base's segment arrived after the mark's")
}

func TestCreateReportsASwitchFailure(t *testing.T) {
	f := newFixture(t, func(context.Context) (*switchResult, error) {
		return nil, errors.New("permission denied for function pg_switch_wal")
	})

	w := f.post("")

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "pg_switch_wal")
}

func TestCreateRefusesASecondMarkWhileOneIsInProgress(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	var enteredOnce sync.Once
	f := newFixture(t, func(context.Context) (*switchResult, error) {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return &switchResult{LSN: "0/7000028", WALFile: testWAL, WalSegSz: testWalSegSz}, nil
	})
	require.NoError(t, f.walFiles.Put(context.Background(), testWAL, bytes.NewReader([]byte("wal"))))

	first := make(chan *httptest.ResponseRecorder)
	go func() { first <- f.post("") }()
	<-entered

	second := f.post("")
	assert.Equal(t, http.StatusConflict, second.Code)
	assert.Contains(t, second.Body.String(), `"status":"in_progress"`)

	close(release)
	assert.Equal(t, http.StatusCreated, (<-first).Code)

	assert.Equal(t, http.StatusCreated, f.post("").Code, "the slot frees once the first mark completes")
}

func TestListReturnsMarksOldestFirst(t *testing.T) {
	f := newFixture(t, nil)
	store := &marks.Store{Storage: f.marks}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, walFile := range []string{"000000010000000000000009", "000000010000000000000003"} {
		m, err := marks.New("0/1", walFile, "", t0.Add(-time.Duration(i)*time.Hour))
		require.NoError(t, err)
		require.NoError(t, store.Put(context.Background(), &m))
	}

	w := f.get()

	require.Equal(t, http.StatusOK, w.Code)
	var got []marks.Mark
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got, 2)
	assert.Equal(t, "000000010000000000000003", got[0].WALFile)
	assert.Equal(t, "000000010000000000000009", got[1].WALFile)
}

func TestListIsAnEmptyArrayWithoutMarks(t *testing.T) {
	f := newFixture(t, nil)

	w := f.get()

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "[]\n", w.Body.String())
}
