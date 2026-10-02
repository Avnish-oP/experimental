# PostgreSQL worker queue

## Database setup

The parent folder's `docker-compose.yml` defines PostgreSQL 17 in the
`pg-lock-lab` container, with database `locks` exposed on host port `5433`.
Start that service and apply the schema:

The current Compose mount uses `./init.sql` as a file, but that path is a
directory. Before initializing a fresh database, change that mount to
`./pg-worker-queue/internals/db/schema.sql:/docker-entrypoint-initdb.d/init.sql:ro`.
For the existing initialized container, `docker compose -f ../docker-compose.yml
start postgres` restarts it; apply the schema with the command below.

```sh
docker compose -f ../docker-compose.yml up -d postgres
export DATABASE_URL='postgres://postgres:postgres@localhost:5433/locks?sslmode=disable'
docker compose -f ../docker-compose.yml exec -T postgres psql -U postgres -d locks -v ON_ERROR_STOP=1 < internals/db/schema.sql
```

Alternatively, if `psql` is installed on your host:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f internals/db/schema.sql
```

The SQL file creates the `jobs` table and indexes. New jobs default to `pending`,
zero attempts, a maximum of three attempts, and immediate availability. The
worker will manage the `processing`, `completed`, and `failed` transitions,
retry limits, and lock fields. All timestamps use `TIMESTAMPTZ`.

## Go connection

Import `pg-worker-queue/internals/db` and connect at application startup:

```go
pool, err := db.Connect(context.Background())
if err != nil {
    log.Fatal(err)
}
defer pool.Close()
```

`Connect` reads `DATABASE_URL`, opens a pgx connection pool, and pings PostgreSQL
with a five-second timeout. Reuse the pool across your API or worker goroutines.
The schema is applied separately using the SQL command above.

`.env.example` documents the connection URL; Go does not load `.env` files
automatically, so export `DATABASE_URL` in the environment before starting your
application. Use `sslmode=disable` for your local Docker database only; configure
TLS as required for other environments.

Driver documentation: [pgxpool](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool).

## Run the API

With `DATABASE_URL` exported as above:

```sh
go run ./api
```

The API listens on `:8080` by default. Set `HTTP_ADDR` to change the address.

Create a job (`201 Created` with the inserted job and a `Location` header):

```sh
curl -i -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"type":"send_email","to":"person@example.com"},"max_attempt":3}'
```

`payload` is required and may contain any non-null JSON value. `max_attempt`
is optional and must be a positive integer; it defaults to 3. `available_at`
is optional and accepts an RFC3339 timestamp (for example,
`2026-10-03T10:00:00Z`); it defaults to the insertion time. New jobs always
start as `pending` with zero attempts. Request bodies are limited to 1 MiB.

Fetch a job by ID (`200 OK`):

```sh
curl -i http://localhost:8080/jobs/1
```

Invalid input returns `400`, oversized bodies return `413`, missing jobs return
`404`, and database errors return `500`. Error bodies contain an `error` string.
Fetching a job does not claim or lock it. Workers claim jobs directly through
PostgreSQL using the atomic operation below.

## Run workers

With `DATABASE_URL` exported as above:

```sh
go run ./workers -id worker-group-1 -concurrency 4 -poll-interval 1s
```

Each goroutine gets a distinct `locked_by` value such as `worker-group-1/1`.
Use a different group ID for each process, or omit `-id` to use the hostname
and process ID. Defaults are one worker and a one-second idle polling interval.
Ctrl+C or SIGTERM stops workers and closes the database pool.

`Store.Claim` begins a transaction at READ COMMITTED isolation and runs
`internals/jobs/claim.sql`. It selects one `pending` job whose `available_at`
has arrived and whose `attempts` is below `max_attempt`, ordered by
`available_at` and `id`. `FOR UPDATE SKIP LOCKED` skips rows already locked by
another transaction. The same statement changes the selected job to
`processing`, sets `locked_at` and `locked_by`, and increments `attempts`.
It also sets `lease_expires_at` to the database time plus the lease duration.
The statement also inserts a `job_executions` row containing the job ID,
worker ID, attempt number, status, and start time. The method returns the
updated job only after committing; errors roll back both the job update and
execution record. An empty queue returns `jobs.ErrNoAvailableJobs`.

`job_executions` enforces one record per `(job_id, attempt)` and at most one
`processing` execution per job. Completion and recovery update both tables
atomically, finishing the execution before a new attempt can be claimed.
Execution records are created for new claims;
claims made before this table was introduced have no execution record.

Inspect the recorded claims in PostgreSQL:

```sql
SELECT job_id, worker_id, attempt, status, started_at, finished_at
FROM job_executions
ORDER BY id;
```

After each claim commits, the worker simulates payload processing for three
seconds, then marks both the job and execution `completed`. Other workers
continue independently, and shutdown can interrupt the simulation. Replace
this delay with actual payload execution in `workers/main.go`.

## Leases, heartbeats, and recovery

The worker defaults to a 10-second lease and renews it every two seconds while
processing. Renewal must match the job ID, worker ID, and attempt number, with
an unexpired lease and `processing` status. A renewal failure stops that worker's
simulation; expiration and recovery handle its unfinished claim.

Each worker process runs a recovery loop every second. `recover.sql` locks up
to 100 expired jobs with `FOR UPDATE SKIP LOCKED`, marks their executions
`failed` with `error_message = 'lease expired'`, and clears their lock and lease
fields. Jobs with remaining attempts become `pending` after a one-second retry
delay; exhausted jobs become `failed`. Recovery preserves `attempts` and the
next claim increments it. Multiple recovery loops can safely run together.

`Store.Complete` uses the same ownership and expiration checks and commits
both completion updates together. An old attempt cannot renew or complete a
newer attempt, even when its worker ID is reused. These checks protect database
updates; external actions still need idempotency to make repeated work safe.

Configure the lab timings:

```sh
go run ./workers -id lab -concurrency 5 \
  -lease-duration 10s -heartbeat-interval 2s \
  -recovery-interval 1s -retry-delay 1s -processing-duration 3s
```

Use `-processing-duration 12s` to observe heartbeats keeping a job alive beyond
its original 10-second lease. Heartbeat intervals must be shorter than leases.
Database timestamps determine lease validity. Applying `schema.sql` upgrades
existing tables and gives older `processing` jobs a lease based on their original
`locked_at`, allowing abandoned claims from earlier experiments to be recovered.

Retries use the same `available_at` ordering as new jobs. With a large ready
backlog, recovery makes a job eligible again but it may wait behind older jobs.
On shutdown, interrupted work is left for lease recovery rather than marked
completed. Business-error handling and exponential retry backoff are future work.

## Verification

```sh
go test ./...
go vet ./...
TEST_DATABASE_URL="$DATABASE_URL" go test ./internals/jobs -v
```

The API integration test requires the schema to be applied. It creates
temporary jobs and deletes them afterward; generated IDs are still consumed.
Claim tests create an isolated temporary schema and remove it afterward.
They check eligibility, ordering, skipping locked rows, rollback of both
tables, and rollback when execution recording fails. The concurrency check
starts 32 workers against 1,000 test jobs and verifies exactly 1,000 execution
records, 1,000 distinct jobs, no duplicate claims, and matching worker IDs,
attempts, and timestamps. Additional checks verify heartbeat extensions,
expiration, retry delays, final-attempt failure, stale-owner rejection,
completion, and concurrent recovery. These claim tests do not modify application jobs.
