# DSS FIELD · ATMOS · JSON CONTRACT · schema_version 3

Two products, one engine. Consumers check `schema_version` and refuse anything else.
Every product carries `degraded` (bool); when true, `timestamp_utc` is the ORIGINAL
reading and `degraded_at_utc` the failed run. Never render degraded data as live.

## status.json · nowcast (+0 / +3 / +6 h)

| Field | Type | Meaning |
|---|---|---|
| `run_id` | str | Unique per run. Use it as cache-buster for images (`?v=run_id`) |
| `timestamp_utc` / `timestamp_local` | ISO | Reading time. Frontend: LIVE < 15 min, STALE ≥ 15, neutralise alerts ≥ 30 |
| `summary.alert_rank` | int | **Source of truth for severity.** 0 GO · 40 caution · 60 warning · 70 storm/hail possible · 80 severe · 90 extreme |
| `summary.alert_level` | str | Label matching `alert_rank` |
| `summary.worst_sector` | str | Sector carrying `alert_rank` |
| `summary.min_mri` / `max_wind` / `max_gust` | num | Across sectors evaluated |
| `summary.storm_level` | NONE/LOW/MODERATE/HIGH/SEVERE/EXTREME | Worst storm tier |
| `summary.heat_level` | NONE/HEAT CAUTION/HEAT WARNING/HEAT DANGER | Worst heat tier (riding window only) |
| `summary.sectors_ok` / `sectors_expected` / `sectors_failed[]` | | **Consumers must not assume all sectors are present** |
| `summary.divergent_sectors[]` | | `consensus.status == DIVERGENT` |
| `consensus_engine.models_identical_detected` | bool | If true, `consensus.status` is INVALID everywhere: one model, no cross-check |
| `sectors[].current.status_rank` | int | Same scale as `alert_rank` |
| `sectors[].current.eei` | int | MQ Rider Index (°). Ghost Rider pace input |
| `sectors[].current.gust` | km/h | Exposure-corrected gust |
| `sectors[].current.lightning_potential` | J/kg | ICON Lightning Potential Index. ≥1 possible · ≥5 likely · ≥10 high. NOT a percentage |
| `sectors[].current.heat.wbgt_c` | °C | Always present; label only inside window |
| `sectors[].consensus.status` | AGREE/UNCERTAIN/DIVERGENT/INVALID | |
| `sectors[].forecast_3h` / `forecast_6h` | | temp, wind, gust, rain, code, eei, lightning_potential |

## field.json · 7-day planning view (DSS Field widget)

| Field | Meaning |
|---|---|
| `event_date`, `window`, `thresholds` | From events/<slug>.yaml |
| `points[].days[].is_event_day` | Highlight |
| `points[].days[].astro` | civil_dawn, sunrise, sunset, civil_dusk (local HH:MM). Lights control = civil_dusk |
| `points[].days[].summary` | temp_min/max, rain_total, gust_max, eei_min, wbgt_max, p_rain_max, p_gust_50_max, p_cold_max, p_heat_max, consensus_worst |
| `points[].days[].hours[]` | Hourly rows inside the window: temp, wind, gust, rain, hum, code, eei, eei_level, wbgt, consensus |
| `hours[].ensemble` | `n_members`, `p_rain`, `p_gust_50`, `p_cold`, `p_heat` (member fractions 0–1), `temp_p10/p90`, `gust_p10/p90`, `rain_p90`. Absent if the ensemble call failed |
| `points[].ensemble_available` | bool |

Probabilities are ECMWF ensemble member fractions, exposure-corrected for gusts.
They are the honest replacement for the old "divergence index": spread measured
on 51 members of one model, not the disagreement of two deterministic runs.

## Health

`HEALTHCHECK_URL` receives `/start` at run begin, a plain ping on success,
`/fail` on failure. No ping for 20 min = the engine is down. Configure the
alert on the healthchecks side, not in the frontend.
