# AI Gateway - Go Implementation Plan

## 1. Goal and boundaries

Build a high-throughput HTTP gateway that forwards AI requests to configured external providers. It must select an eligible `account -> provider -> API key -> model` route in round-robin order, record usage, and temporarily disable a key for one hour after a rate-limit response.

PostgreSQL is the durable source of truth. Redis holds shared cached routing data, while immutable in-memory snapshots provide the request-path routing data with no database lookup.

The first delivery intentionally exposes its HTTP APIs without application authentication, as requested. It must be deployed only behind a trusted private network, IP allow-list, or reverse proxy because the admin API controls provider credentials.

## 2. Proposed stack

- Go 1.23+ with `net/http` and `chi` for a small, fast HTTP layer.
- `pgx/v5` + `pgxpool` for PostgreSQL.
- `go-redis/v9` for Redis.
- Structured JSON logging with `log/slog`, written to daily rotated files.
- `prometheus/client_golang` for operational metrics.
- `httptest`, Testcontainers (PostgreSQL + Redis), and race-detector tests.

Configuration is supplied only through environment variables or a non-committed local `.env` file. Existing connection and API-key text files are reference material only and must not be copied into source code, migrations, logs, or Git.

## 3. Architecture

```text
Client
  │
  ├─ Public proxy API (/v1/...)
  │    ├─ in-memory routing snapshot
  │    ├─ atomic round-robin selection
  │    ├─ provider adapter
  │    └─ outbound HTTP client
  │
  └─ Admin CRUD API (/admin/...)
       ├─ PostgreSQL transaction (source of truth)
       ├─ Redis invalidation/refresh
       └─ snapshot rebuild notification

PostgreSQL --> Redis routing cache --> in-memory immutable snapshot
                 ^                         |
                 +---- error/suspension ---+
```

### Request-path rules

1. Load the current immutable snapshot atomically; never query PostgreSQL on a normal proxy request.
2. Resolve the requested logical model and filter to enabled, non-suspended routes.
3. Choose one route using an atomic counter per logical model (or requested route group).
4. Proxy the request through that provider's adapter with the chosen API key/model.
5. Update counters asynchronously and emit structured success/failure logs.
6. On a recognized rate-limit response, set `suspended_until = now + 1 hour` in PostgreSQL, refresh/invalidate Redis, and publish a reload event. The next request must exclude that key immediately from the local snapshot.
7. For transient non-rate-limit failures, optionally try each remaining eligible route once, bounded by a configurable maximum. Do not retry non-idempotent requests unless the upstream response proves no generation began.

Round-robin state is process-local for lowest request latency. That creates approximately even distribution per instance, rather than a globally strict sequence. If strict cross-instance ordering is required, use Redis `INCR` per routing group; make this a configuration option because it adds network latency to every request.

## 4. Data model and migrations

Create versioned SQL migrations. Use UTC timestamps, foreign keys, check constraints, and indexes on all routing filters.

| Table | Purpose | Key fields |
|---|---|---|
| `accounts` | Gmail/account owner grouping | `id`, `email`, `enabled`, timestamps |
| `providers` | Provider configuration below an account | `id`, `account_id`, `name`, `base_url`, `adapter_type`, `enabled`, timestamps |
| `api_keys` | Encrypted upstream credentials and availability | `id`, `provider_id`, `label`, `secret_ciphertext`, `enabled`, `suspended_until`, `usage_count`, `last_used_at` |
| `models` | Model offered by a key | `id`, `api_key_id`, `logical_name`, `upstream_model`, `enabled`, `usage_count`, `last_used_at` |
| `request_attempts` (optional) | Auditable, sampled request outcomes | route IDs, request ID, status, latency, error class, timestamp |
| `schema_migrations` | Migration tool state | version/checksum/applied timestamp |

Important constraints and indexes:

- Unique `accounts.email`; unique provider name per account; unique `(api_key_id, logical_name)` model mapping.
- Partial routing indexes for enabled providers/keys/models and `suspended_until`.
- Do not store plaintext API keys. Encrypt each secret with an application-managed AEAD key from `KEY_ENCRYPTION_KEY`; decrypt only long enough to make the outbound request.
- Use a database transaction and `FOR UPDATE` when changing suspension state so competing requests cannot regress it.

## 5. Cache and synchronization design

### Redis keys

- `gateway:routing:v1` — serialized versioned routing document.
- `gateway:routing:version` — monotonically increasing version.
- `gateway:events` — Redis Pub/Sub reload notifications.
- `gateway:key:{id}:suspended_until` — optional short-lived hot marker for rapid cross-instance removal.

### Refresh behavior

- On startup: PostgreSQL → Redis (if missing/stale) → local snapshot.
- On every CRUD write or rate-limit suspension: commit PostgreSQL first, rebuild/write Redis, increment version, publish event, swap the local snapshot.
- Every instance subscribes to `gateway:events`, rebuilds from Redis, validates the version, and atomically swaps the snapshot.
- Add a periodic reconciliation job (for example every 60 seconds) to recover from missed Pub/Sub events and Redis restarts.
- If Redis is unavailable, keep serving the last valid local snapshot and periodically reload directly from PostgreSQL. Report this through health/metrics; never start with an empty cache unless PostgreSQL also has no routes.

## 6. HTTP API contract

Version all APIs and return JSON errors with `code`, `message`, `request_id`, and optional safe `details`.

### Admin CRUD

- `POST/GET/PATCH/DELETE /admin/v1/accounts`
- `POST/GET/PATCH/DELETE /admin/v1/accounts/{accountID}/providers`
- `POST/GET/PATCH/DELETE /admin/v1/providers/{providerID}/keys`
- `POST/GET/PATCH/DELETE /admin/v1/keys/{keyID}/models`
- `POST /admin/v1/routing/reload` — rebuild caches after validation.
- `POST /admin/v1/keys/{keyID}/resume` — explicitly clear a suspension.
- `GET /admin/v1/routes` — safe view of effective routing state, never secrets.

Create/update payloads must accept credentials only on write. API responses show a key label and a masked fingerprint, never the secret. Deletion should initially be a soft disable so routing immediately excludes the entity and audit history remains intact.

### Proxy API

- `POST /v1/inference` — inference request/response, including streaming where the adapter supports it. The client sends messages only; the configured default model is selected server-side.
- `GET /healthz` — process alive.
- `GET /readyz` — PostgreSQL reachable and a valid routing snapshot loaded.
- `GET /metrics` — Prometheus scrape endpoint (restrict at deployment layer).

Model selection is by `model` in the incoming OpenAI-compatible request. A logical model can map to one or multiple upstream models across providers. Return a clear `no_eligible_route` error if every route is disabled or suspended.

## 7. Provider compatibility strategy

"Any provider" requires an adapter system, not one hard-coded request format. Implement the following interface:

```go
type ProviderAdapter interface {
    Validate(ProviderConfig) error
    BuildRequest(ctx context.Context, input ProxyRequest, route Route) (*http.Request, error)
    ClassifyResponse(status int, headers http.Header, body []byte) ErrorClass
    TranslateResponse(upstream *http.Response) (*ProxyResponse, error)
}
```

Delivery order:

1. `openai_compatible` adapter (configurable base URL, auth header, model field) for the majority of providers.
2. Provider-specific adapters only when their protocol differs materially (for example, Anthropic-style message APIs or custom auth).
3. Register adapters by `adapter_type`; reject unknown types during admin writes.

Rate-limit classification must cover HTTP 429 and documented provider-specific quota error codes. Honor safe `Retry-After` metadata for logging, while the configured suspension is always one hour unless later made provider-configurable.

## 8. Package layout

```text
cmd/gateway/main.go
internal/config/       environment parsing and validation
internal/domain/       entities, routing rules, interfaces
internal/store/        PostgreSQL repositories and migrations
internal/cache/        Redis cache, invalidation, Pub/Sub
internal/router/       snapshot builder and atomic selector
internal/provider/     adapter registry and outbound HTTP client
internal/api/          handlers, DTOs, middleware, validation
internal/observability/logging, metrics, request IDs, daily rotation
internal/service/      CRUD and proxy orchestration
migrations/            numbered SQL migrations
tests/                 integration and end-to-end tests
```

Keep handlers thin; all business decisions live in services/domain packages. Use explicit interfaces only at external boundaries (database, cache, upstream transport) to make tests simple without excessive abstraction.

## 9. Observability and operations

- Daily rolling JSON log files, retained for a configurable number of days. Also log to stdout for container platforms.
- Every proxy attempt logs request ID, account/provider/key/model IDs or labels, upstream status, error class, latency, retry attempt, and selected route. Never log API keys or full prompt/response bodies by default.
- Metrics: requests by provider/model/status, selection counts, key suspension counts, upstream latency histogram, cache version/reload failures, snapshot route count, and no-route failures.
- Outbound client: shared tuned `http.Transport`, context deadlines, response-body size limits for non-streaming responses, keep-alives, and no unbounded retries.
- Graceful shutdown: stop accepting requests, drain active requests within a timeout, close Redis and PostgreSQL pools, flush logs.

## 10. Implementation milestones

1. **Bootstrap and configuration**
   - Initialize module, lint/test commands, Docker Compose for local PostgreSQL/Redis, `.env.example`, and secret-safe `.gitignore`.
   - Add config validation and health endpoints.

2. **Database foundation**
   - Implement migrations, repositories, encryption-at-rest, and CRUD services.
   - Add integration tests for constraints, masked responses, and transactional updates.

3. **Routing and caching**
   - Build PostgreSQL/Redis/local snapshot synchronization.
   - Implement atomic round-robin selection, exclusion logic, Pub/Sub refresh, and reconciliation.
   - Add race and multi-instance behavior tests.

4. **Provider proxy**
   - Implement the OpenAI-compatible adapter, shared outbound transport, normal and SSE streaming forwarding.
   - Add error classification, retry policy, rate-limit suspension, and cache update verification.

5. **Production readiness**
   - Add daily logs, metrics, readiness behavior, load tests, OpenAPI specification, and deployment manifests.
   - Load the provided key inventory through a one-time, secret-safe admin/import workflow; verify no secrets are committed or printed.

## 11. Acceptance criteria

- CRUD changes appear in PostgreSQL, Redis, and all in-memory instances without restart.
- Repeated requests for the same logical model rotate across eligible routes correctly under concurrency and never select a disabled/suspended route.
- A simulated rate limit suspends only the selected key for one hour, persists the state, and removes it from cache/snapshots immediately.
- Proxy requests are compatible with the documented OpenAI-style endpoint and preserve streaming behavior.
- API-key values never appear in responses, logs, metrics, test fixtures, Git history, or configuration committed to the repository.
- `go test ./...`, `go test -race ./...`, linting, and integration tests pass; a benchmark documents routing overhead.

## 12. Decisions to confirm before implementation

1. Which exact providers and models are required for the initial release, and are they all OpenAI-compatible?
2. Should user-provided model names be logical aliases, upstream model names, or both?
3. Is local per-instance round-robin acceptable, or is globally strict ordering across replicas required?
4. What network boundary will protect the intentionally unauthenticated admin and proxy APIs?
5. Should usage counters be exact synchronous values (higher database cost) or periodically batched metrics (higher throughput)?
