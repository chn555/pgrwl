package marksapi

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/pgrwl/pgrwl/internal/opt/shared/x/httpx"
)

// Request bodies carry at most a label, so anything larger is rejected before it is read.
const (
	maxBodyBytes   = 4 << 10
	maxLabelLength = 256
)

type Handler struct {
	Service Service
}

func NewHandler(s Service) *Handler {
	return &Handler{
		Service: s,
	}
}

// The createRequest is the optional body of POST /api/v1/marks.
type createRequest struct {
	Label string `json:"label"`
}

func (c *Handler) Create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

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
	switch {
	case errors.Is(err, ErrMarkInProgress):
		httpx.WriteJSON(w, http.StatusConflict, map[string]any{
			"status": "in_progress",
			"error":  err.Error(),
		})

	case errors.Is(err, ErrWALTimeout):
		httpx.WriteJSON(w, http.StatusGatewayTimeout, map[string]any{
			"status": "timeout",
			"error":  err.Error(),
		})

	default:
		slog.Error("mark creation failed", slog.Any("err", err))

		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
			"status": "error",
			"error":  err.Error(),
		})
	}
}
