# Redis store

Redis cache and Pub/Sub ownership belongs in this package. The service boundary is defined in `internal/services` so the HTTP and routing layers do not depend on Redis APIs.
