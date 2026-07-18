# Gateway entrypoint

The production entrypoint belongs in `main.go` in this directory. It should only load configuration, construct PostgreSQL/Redis stores and services, register HTTP routes, and start the server.
