package marksapi

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/pgrwl/pgrwl/internal/opt/shared/x/httpx"
)

// Request bodies carry at most a label, so anything larger is rejected before it is read.
const (
	maxBodyBytes   = 4 << 10
	maxLabelLength = 256
)

// The responseGrace is added to the WAL timeout so a 504 still reaches the client.
const responseGrace = 5 * time.Second

type Handler struct {
	Service    Service
	WALTimeout time.Duration
}

func NewHandler(s Service, walTimeout time.Duration) *Handler {
	return &Handler{
		Service:    s,
		WALTimeout: walTimeout,
	}
}

// The createRequest is the optional body of POST /api/v1/marks.
type createRequest struct {
	Label string `json:"label"`
}

func (c *Handler) Create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	// A mark outlives the server's write timeout while it waits for the segment, so push the deadline out.
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(c.WALTimeout + responseGrace)); err != nil {
		slog.Warn("cannot extend the write deadline for a mark", slog.Any("err", err))
	}

	var req createRequest
	err := httpx.ReadJSON(r, &req)
	if err != nil && !errors.Is(err, io.EOF) {
		writeBadRequest(w, err)
		return
	}
	if len(req.Label) > maxLabelLength {
		writeBadRequest(w, fmt.Errorf("label longer than %d bytes", maxLabelLength))
		return
	}

	m, err := c.Service.Create(r.Context(), req.Label)
	if err != nil {
		writeCreateError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, m)
}

func (c *Handler) List(w http.ResponseWriter, r *http.Request) {
	ms, err := c.Service.List(r.Context())
	if err != nil {
		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, ms)
}

func writeBadRequest(w http.ResponseWriter, err error) {
	httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
		"status": "bad_request",
		"error":  err.Error(),
	})
}

func writeCreateError(w http.ResponseWriter, err error) {
	body := map[string]any{"error": err.Error()}
	code := http.StatusInternalServerError

	var gap *GapError
	switch {
	case errors.Is(err, ErrMarkInProgress):
		body["status"], code = "in_progress", http.StatusConflict

	case errors.As(err, &gap):
		body["status"], code = "wal_gap", http.StatusConflict
		if gap.Missing != "" {
			body["missing"] = gap.Missing
		}

	case errors.Is(err, ErrWALTimeout):
		body["status"], code = "timeout", http.StatusGatewayTimeout

	default:
		slog.Error("mark creation failed", slog.Any("err", err))
		body["status"] = "error"
	}
	httpx.WriteJSON(w, code, body)
}
