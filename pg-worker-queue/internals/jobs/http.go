package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

type JobStore interface {
	Create(context.Context, CreateParams) (Job, error)
	Get(context.Context, int64) (Job, error)
}

// NewHandler serves job creation and lookup; database operations have a timeout.
func NewHandler(store JobStore) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var params CreateParams
		if err := decoder.Decode(&params); err != nil {
			writeDecodeError(w, err)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			if err == nil {
				err = fmt.Errorf("multiple JSON values")
			}
			writeDecodeError(w, err)
			return
		}
		if len(params.Payload) == 0 || bytes.Equal(bytes.TrimSpace(params.Payload), []byte("null")) {
			writeError(w, http.StatusBadRequest, "payload is required and must not be null")
			return
		}
		if params.MaxAttempt != nil && *params.MaxAttempt <= 0 {
			writeError(w, http.StatusBadRequest, "max_attempt must be greater than zero")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		job, err := store.Create(ctx, params)
		if err != nil {
			log.Printf("create job: %v", err)
			writeError(w, http.StatusInternalServerError, "could not create job")
			return
		}
		w.Header().Set("Location", fmt.Sprintf("/jobs/%d", job.ID))
		writeJSON(w, http.StatusCreated, job)
	})
	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil || id <= 0 {
			writeError(w, http.StatusBadRequest, "id must be a positive integer")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		job, err := store.Get(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		if err != nil {
			log.Printf("get job %d: %v", id, err)
			writeError(w, http.StatusInternalServerError, "could not fetch job")
			return
		}
		writeJSON(w, http.StatusOK, job)
	})
	return mux
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var sizeError *http.MaxBytesError
	if errors.As(err, &sizeError) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body must not exceed 1 MiB")
		return
	}
	writeError(w, http.StatusBadRequest, "invalid job JSON; use payload, max_attempt, and available_at (RFC3339)")
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write JSON response: %v", err)
	}
}
