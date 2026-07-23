# AI Gateway

Run locally:

```powershell
go run ./cmd/gateway
```

The default provider URL is `mock://...`, so imported inventory can be smoke-tested without spending provider quota. Set a provider `base_url` to use a real OpenAI-compatible upstream. Secrets are accepted only on key creation and are never returned by the API.

Inference is exposed at `POST /v1/inference`. Clients send `messages` and optional `stream`; they do not send a model. The server selects the next eligible configured route globally using round-robin order.

Configuration is loaded from `.env` in the working directory, or from `ENV_FILE`. Copy `.env.example` to `.env` and fill in the PostgreSQL/Redis credentials. The service no longer reads connection credentials from text files. If Redis is unreachable, PostgreSQL remains authoritative and the process serves its last in-memory snapshot while reporting the degraded dependency in logs.

Accounts, providers, API keys, and models are loaded from PostgreSQL at startup. No key or model inventory files are required.

The requested Bynara accounts are provisioned idempotently when
`BYNARA_CONNECTO_API_KEY` and `BYNARA_SELLERS_API_KEY` are present in the
environment. Both keys route `mistral-large` and `nemotron-3-ultra` through
`https://router.bynara.id/v1`. Secrets remain environment-only.

Any upstream network or HTTP error removes the selected API key from routing
for 30 minutes. The error, key ID, and cooldown deadline are recorded without
logging credentials. The key automatically becomes eligible again after the
cooldown.

Logs are written as daily JSON files outside the project by default (`../logs/`) using names such as `gateway-2026-07-13.log`. Override the directory with `LOG_DIR`. Logs are also written to stdout, never contain API-key secrets, and retain at most 14 UTC daily files.
