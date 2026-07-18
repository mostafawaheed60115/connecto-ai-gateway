# AI Gateway

Run locally:

```powershell
go run ./cmd/gateway
```

The default provider URL is `mock://...`, so imported inventory can be smoke-tested without spending provider quota. Set a provider `base_url` to use a real OpenAI-compatible upstream. Secrets are accepted only on key creation and are never returned by the API.

Inference is exposed at `POST /v1/inference`. Clients send `messages` and optional `stream`; they do not send a model. The server selects the next eligible configured route globally using round-robin order.

Configuration is loaded from `.env` in the working directory, or from `ENV_FILE`. Copy `.env.example` to `.env` and fill in the PostgreSQL/Redis credentials. The service no longer reads connection credentials from text files. If Redis is unreachable, PostgreSQL remains authoritative and the process serves its last in-memory snapshot while reporting the degraded dependency in logs.

Accounts, providers, API keys, and models are loaded from PostgreSQL at startup. No key or model inventory files are required.

Logs are written as daily JSON files outside the project by default (`../logs/`) using names such as `gateway-2026-07-13.log`. Override the directory with `LOG_DIR`. Logs are also written to stdout and never contain API-key secrets.
