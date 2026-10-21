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
