# DSS FIELD · ATMOS engine 22.0

One engine, every DSS event, two products: `status.json` (nowcast) and `field.json` (7-day, ensemble).

    pip install -r requirements.txt
    python -m pytest -q
    python -m atmos.run --event events/mq.yaml --products status,field            # local, no publish
    python -m atmos.run --event events/terranorte.yaml --products field --publish  # FTPS

Secrets (GitHub Actions): FTP_HOST, FTP_USER, FTP_PASS, AEMET_API_KEY (optional), HEALTHCHECK_URL.
Set FTP_ALLOW_PLAIN=1 only if the host refuses TLS; the log says UNENCRYPTED when it falls back.

Layout: `atmos/sources` (Open-Meteo, IPMA, AEMET, NASA) · `atmos/physics` (EEI, heat, storm, microclimate)
· `atmos/consensus.py` · `atmos/products` (status, field) · `atmos/publish` (atomic FTPS, heartbeat)
· `atmos/render` (optional PNG) · `events/*.yaml` · `tests/`.

Contract for consumers: ATMOS_CONTRACT.md.

## Scheduling (read this)

GitHub `schedule` is best-effort: on this repository it fires every 2–5 h, not every 10 min.
The engine is triggered from outside. One-time setup:

1. GitHub → Settings → Developer settings → Fine-grained personal access token:
   repository `mq-atmos-station`, permission **Contents: read and write** (needed for `repository_dispatch`). Copy the token.
2. cron-job.org (free) → Create cronjob:
   - URL `https://api.github.com/repos/fernandocesar2300-a11y/mq-atmos-station/dispatches`
   - Schedule: every 10 minutes
   - Request method POST, body `{"event_type":"atmos-run"}`
   - Headers: `Authorization: Bearer <token>` · `Accept: application/vnd.github+json` · `User-Agent: dss-atmos-pinger`
3. Save. Each ping starts one run (≈1 min). `concurrency` in the workflow prevents overlap.

Healthchecks.io (`HEALTHCHECK_URL` secret) is the independent watchdog: if no run completes for 20 min, it alerts.
