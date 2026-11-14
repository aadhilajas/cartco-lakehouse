# Preserved baseline entrypoints

The earlier REST-catalog/PyArrow application remains available through these
root configuration files:

- [docker-compose.yml](../../docker-compose.yml): original application and CI stack.
- [compose.recovery.yml](../../compose.recovery.yml): isolated restored-copy checks.
- [compose.worker-drill.yml](../../compose.worker-drill.yml): recovery-only worker drill overlay.

They intentionally remain at repository root. Existing `docker compose` commands,
relative bind mounts and combined `-f` overrides keep their established meaning.
Copying them here would create duplicate editable specifications; changing their
base directory could redirect bind mounts. This folder is a navigation boundary,
not another competing stack definition.

The root configurations now reference the shared Dockerfiles under `infra/shared`.
Their rendered service/environment/port/network/volume models were compared
before and after the layout change, allowing only reviewed Dockerfile path moves.
They do not use the current application's local stores or constitute cloud hosting.

Start the current application with its [local runbook](../../docs/assessed-runbook.md).
Historical baseline behavior is documented [separately](../../docs/history/README.md).
