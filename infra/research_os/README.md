# Factor Lab Research OS local infrastructure

This Compose stack is the local, research-only deployment: PostgreSQL 16,
MinIO, Alembic, Dagster and the loopback WebUI. It has no broker integration,
real-money order route or live-trading switch.

## Runtime and credentials

The Windows runtime is rooted at `H:/Program Data/factor-lab-runtime`:

- `data` and `artifacts` are mounted at fixed container paths under
  `/opt/factor-lab/runtime`.
- `secrets/settings` contains only source/model secrets and is mounted
  read-only at `/run/secrets` for workers. Infrastructure passwords are
  mounted separately at `/run/infra-secrets`.
- The WebUI never receives `/run/secrets`, MinIO credentials, vendor `*_FILE`
  variables, or the migration-owner PostgreSQL login. It receives only a
  dedicated read-only database password and the writable
  `secrets/settings` editor mount.
- PostgreSQL and MinIO retain their named volumes. Docker Desktop's data disk
  is on H:, avoiding unsupported direct PostgreSQL-on-NTFS binds.

`infra/research_os/.env` contains only paths, non-secret identifiers and
`*_FILE` references. Each secret file must be a regular, non-symlink, one-line
UTF-8 file. Never place a token or password in Compose environment, a profile
JSON document or a checked-in config. Source/model profiles use
`secret://NAME`.

WebUI profile settings live in
`artifacts/settings/webui.env`. The entrypoint deliberately does not export
those values: each new application-service resource reads the allow-listed
file again, so an atomic WebUI update is not shadowed by stale process env.
The full artifacts mount is read-only in the WebUI; only the nested
`artifacts/settings` bind is writable. Credential references are type-scoped:
source profiles may reference only their own `source-TYPE-*` (or canonical
source) secret and model profiles only `llm-*`.

Before recreating an existing stack, create `secrets/settings` and
`artifacts/settings`, move only Tushare, Diemeng and LLM secret files into the
former, and create a new one-line `webui_postgres_password` distinct from the
migration-owner password. Set `RESEARCH_OS_SETTINGS_SECRETS_ROOT`,
`RESEARCH_OS_WEBUI_SETTINGS_ROOT`, `RESEARCH_OS_WEBUI_POSTGRES_USER` and
`RESEARCH_OS_WEBUI_POSTGRES_PASSWORD_FILE` from `.env.example`. The one-shot
`webui-db-bootstrap` service idempotently creates/rotates the login, revokes
write privileges, grants current/future table SELECT, and verifies a read-only
session before the WebUI starts.

## Validate and start

Use only `configs/research_os_orchestration.production.json`; example configs,
Windows host paths inside JSON and arbitrary file-driven inputs are rejected.

```powershell
docker build -f infra/research_os/Dockerfile.dagster `
  -t factor-lab-research-os:local .

docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml config --quiet

docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml up -d --force-recreate
```

Published ports are loopback-only: WebUI `8765`, Dagster `8766`, PostgreSQL
`15432`, MinIO API `9000`, and MinIO console `9001`. Port `15432` replaces
`5433`, which was observed inside an active Windows Hyper-V exclusion range;
Compose still fails closed if the replacement host port cannot be bound.

Changing PostgreSQL or MinIO credentials requires coordinated server rotation
and service recreation. Record PostgreSQL logical hashes and the MinIO object
manifest before and after; never delete volumes. Source/model credential files
are directory binds and are resolved by newly constructed services.

## Readiness and provenance

Image construction pins every base image by digest, inventories `src`,
`configs`, `uv.lock` and `infra/research_os`, and embeds an immutable source
bundle manifest. Formal epoch admission additionally records the Docker
daemon-inspected image ID, RepoDigest and immutable base-image digest.

Production readiness is intentionally separate from container health:

- after source credentials have moved to reviewed `secret://` file bindings,
  the non-forward physical engineering canary and its dual-source calendar
  capability probe may temporarily use the currently valid credentials;
- authoritative historical backfill normally requires verified vendor-side
  rotation of credentials exposed by old containers/repositories. For this
  local research-only deployment, the operator may instead record the closed
  `retained_unrotated_operator_accepted` waiver. The waiver explicitly remains
  `not_rotated`, binds the exact reviewed `secret://` reference, records an
  aware acceptance time and fixed local-only reason, and is ineffective unless
  that provider's reviewed HTTPS transport is also verified. The checked-in
  records bind Tushare to `https://api.tushare.pro/dataapi` and Diemeng to
  `https://data.diemeng.chat/api`;
- formal forward activation additionally requires daemon-inspected OCI proof
  and an accepted open-execution adapter, followed by a PostgreSQL-persisted
  readiness audit covering the real capability probe, accepted Gold/full
  matrix, restore drill and soak;
- formal opening collection uses Tushare `rt_min` through the reviewed direct
  HTTPS route. The static contract is structurally capable but remains
  `runtime_probe_gated`; only a live 09:30--09:35 session-bound observation can
  create accepted execution capability. Configuration JSON can never
  self-approve a formal epoch.

Credential retention is a recorded operator risk decision, not vendor rotation
evidence. It does not make a key private again, weaken field/data-quality gates,
or admit an unencrypted source route. Removing the exact secret binding or the
HTTPS transport proof automatically restores the rotation/readiness blocker.

After deploying a validated release, persist the selected credential decision
once from inside the worker environment:

```powershell
docker exec factor-lab-research-os-dagster-code-server-1 `
  /usr/local/bin/factor-lab-entrypoint factor-lab readiness attest-credential-use
```

The emitted and persisted record contains only the credential name, decision
kind and hashes; it never contains credential material. Run the live execution
probe separately during the next accepted 09:30--09:35 trading window.

No evidence epoch or true forward evidence is created by starting the stack.
The first forward session must come from the accepted Gold trading calendar
after all formal blockers are removed; it must never be guessed.

Run host Docker attestation outside the workload container. The application
accepts either `FACTOR_LAB_POSTGRES_PASSWORD_FILE` or the Compose-side
`RESEARCH_OS_POSTGRES_PASSWORD_FILE`; if both are present they must resolve to
the same file. The one-line password is passed only as a database-driver
connection argument and is never embedded in the URL or printed by doctor.

## Orchestration safety

All schedules and sensors ship `STOPPED`. A static SSE calendar asset first
reconciles Tushare and Diemeng calendars into accepted Silver/PG ledger rows,
then registers only proven open sessions as Dagster dynamic partitions.
Expected source/reconciliation failures flow to the risk guard; unexpected
daily failures become typed data incidents.

Enable readiness/canary work explicitly after review. Do not turn on the daily,
weekly, monthly, quarterly or recovery automation merely because containers
are healthy.

## Inspect and stop

```powershell
docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml ps

docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml logs dagster-daemon

docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml down
```

Plain `down` preserves named volumes. `down -v` is deliberately absent because
it destroys the authoritative local catalog and object store.
