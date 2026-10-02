package jobs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Run against a database with schema.sql applied by setting TEST_DATABASE_URL.
func TestJobsAPIIntegration(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration checks")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(NewStore(pool))
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body)).WithContext(ctx)
		req.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(recorder, req)
		return recorder
	}

	for _, tc := range []struct {
		name, body string
		maxAttempt int32
		scheduled  bool
	}{
		{"defaults", `{"payload":{"type":"integration_check"}}`, 3, false},
		{"scheduled", `{"payload":{"type":"integration_check"},"max_attempt":5,"available_at":"2030-01-01T00:00:00Z"}`, 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := request(http.MethodPost, "/jobs", tc.body)
			if response.Code != http.StatusCreated {
				t.Fatalf("POST: status %d, body %s", response.Code, response.Body.String())
			}
			var created Job
			if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", created.ID); err != nil {
					t.Errorf("clean up test job: %v", err)
				}
			}()
			if created.ID <= 0 || created.Status != "pending" || created.Attempts != 0 || created.MaxAttempt != tc.maxAttempt || created.CreatedAt.IsZero() || created.AvailableAt.IsZero() || created.LockedAt != nil || created.LockedBy != nil || created.CompletedAt != nil {
				t.Fatalf("unexpected inserted job: %+v", created)
			}
			if tc.scheduled && !created.AvailableAt.Equal(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)) {
				t.Fatalf("unexpected available_at: %s", created.AvailableAt)
			}
			path := "/jobs/" + strconv.FormatInt(created.ID, 10)
			if response.Header().Get("Location") != path {
				t.Fatalf("unexpected Location: %s", response.Header().Get("Location"))
			}
			response = request(http.MethodGet, path, "")
			if response.Code != http.StatusOK {
				t.Fatalf("GET: status %d, body %s", response.Code, response.Body.String())
			}
			var fetched Job
			if err := json.Unmarshal(response.Body.Bytes(), &fetched); err != nil {
				t.Fatal(err)
			}
			if fetched.ID != created.ID || string(fetched.Payload) != string(created.Payload) || fetched.Status != created.Status || fetched.MaxAttempt != created.MaxAttempt || !fetched.AvailableAt.Equal(created.AvailableAt) {
				t.Fatalf("GET did not return persisted job: %+v", fetched)
			}
		})
	}

	for _, tc := range []struct {
		name, method, path, body string
		status                   int
	}{
		{"missing payload", "POST", "/jobs", `{}`, 400},
		{"null payload", "POST", "/jobs", `{"payload":null}`, 400},
		{"invalid attempts", "POST", "/jobs", `{"payload":{},"max_attempt":0}`, 400},
		{"invalid timestamp", "POST", "/jobs", `{"payload":{},"available_at":"tomorrow"}`, 400},
		{"unknown field", "POST", "/jobs", `{"payload":{},"status":"completed"}`, 400},
		{"trailing JSON", "POST", "/jobs", `{"payload":{}} {}`, 400},
		{"malformed JSON", "POST", "/jobs", `{"payload":`, 400},
		{"oversized body", "POST", "/jobs", `{"payload":"` + strings.Repeat("x", 1<<20) + `"}`, 413},
		{"invalid ID", "GET", "/jobs/abc", "", 400},
		{"zero ID", "GET", "/jobs/0", "", 400},
		{"missing job", "GET", "/jobs/9223372036854775807", "", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := request(tc.method, tc.path, tc.body)
			if response.Code != tc.status {
				t.Fatalf("status %d, want %d; body %s", response.Code, tc.status, response.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Fatalf("expected JSON error: %s", response.Body.String())
			}
		})
	}
}
