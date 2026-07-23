# VPS deployment

Create the application directory on the VPS and keep its `.env` file there. The
GitHub Actions deployment updates tracked files but does not touch `.env`.

```bash
cd /home/mahmoud/opt/ai-gateway
go build -o gateway ./cmd/gateway
mkdir -p logs
chmod 600 .env
sudo systemctl daemon-reload
sudo systemctl enable --now ai-gateway
sudo systemctl status ai-gateway
```

Configure these GitHub repository secrets:

- `APP_DIR`: `/home/mahmoud/opt/ai-gateway`
- `VPS_HOST`: the VPS hostname or IP address
- `VPS_PORT`: the SSH port, normally `22`
- `VPS_USER`: `mahmoud`
- `SSH_PRIVATE_KEY`: the complete private deployment key, including its header
  and footer
- `BYNARA_CONNECTO_API_KEY`: the Bynara key for `connecto.meets@gmail.com`
- `BYNARA_SELLERS_API_KEY`: the Bynara key for `sellers.connecto@gmail.com`

The `mahmoud` user must be able to restart and inspect only this service without
an interactive password. Add this with `sudo visudo` (adjust the systemctl path
if `command -v systemctl` reports a different path):

```sudoers
mahmoud ALL=(root) NOPASSWD: /usr/bin/systemctl restart ai-gateway, /usr/bin/systemctl is-active --quiet ai-gateway
```

GitHub Actions runs the tests and creates a stripped Linux AMD64 binary on the
hosted runner. It uploads that artifact to the VPS, replaces the executable
atomically, securely merges the Bynara secrets into the VPS-only `.env`, restarts
the service, and requires `/healthz` to return successfully. The VPS therefore
does not spend CPU and memory compiling the application.

The service listens on `0.0.0.0:4173` when `ADDR=0.0.0.0:4173` is set. PostgreSQL contains the accounts, providers, keys, and models; no key/model files are required. Daily logs are written under the configured application directory.
