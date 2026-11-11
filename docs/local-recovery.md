# Local recovery and startup hardening

The SQLite/file-warehouse recovery tool covers the isolated local integration
environment. It does not back up the REST service, MinIO, Airflow Postgres or
Marquez. Use the SQLAlchemy 2 environment in `requirements/file-integration.txt`;
do not upgrade Airflow's SQLAlchemy to run this tool.

Stop every writer before creating a backup. The tool also holds a SQLite write
transaction, inventories source files before and after copying, and refuses an
active publication lock. This is not distributed fencing: filesystem writers
must actually remain stopped. A failed candidate has no final manifest and must
not be used for recovery.

```text
python scripts/local_recovery.py backup --catalog-db <absolute-db> --warehouse <absolute-warehouse> --runtime-root <absolute-runtime> --catalog-name <exact-name> --output <new-backup-directory> --writers-stopped
python scripts/local_recovery.py verify --backup <backup-directory>
python scripts/local_recovery.py restore --backup <backup-directory> --catalog-db <original-db> --warehouse <original-warehouse> --runtime-root <original-runtime> --writers-stopped
```

The complete catalog, warehouse and runtime files are copied, including retained
snapshots, raw deliveries, receipts, inventory evidence, Gold artifacts and local
observability records. The manifest records sizes/SHA256 checksums plus table
UUIDs, schemas, current/retained snapshot IDs and current row multiset hashes.
Gold publication checksums are validated. Hashes detect accidental corruption;
this is not a signed or tamper-proof backup. Store a separate protected copy for
actual disaster recovery.

Restore requires all three original targets to be absent. Preserve damaged data
separately and verify the backup before recovery; the tool never deletes or
overwrites those targets. Iceberg metadata and runtime receipts contain absolute
paths, so automatic relocation is explicitly unsupported. A failed partial
restore remains for investigation; keep writers stopped until the audit passes.
Test recovery periodically in a disposable environment, preserving the original
data before simulating loss. The integration suite exercises historical snapshot
reads, receipt replay and five-artifact publication after recovery.

## Container catalog persistence

Compose now mounts `iceberg-catalog-data` at the pinned image user's existing
home and sets `CATALOG_URI=jdbc:sqlite:/home/iceberg/catalog.db`. MinIO object
files alone are insufficient to recover JDBC catalog table pointers. The pinned
amd64 image config confirms `JdbcCatalog`, the SQLite default, and user
`iceberg:iceberg`. This change is prepared, not live-container verified.

**Existing installations:** preserve/export the catalog database from the old
container writable layer before switching URI. Starting with an empty named
volume does not migrate existing pointers. Never use `docker compose down -v`
against data you intend to retain.

Live acceptance still required: capture all table UUIDs, current/retained
snapshot IDs and row hashes through REST, recreate only `iceberg-rest` without
removing volumes, then compare through fresh catalog handles. Separately test
coordinated recovery of the JDBC database, MinIO objects, runtime data and service
databases. The local backup tool cannot substitute for this check.

## Airflow initialization

The initializer now runs `airflow db migrate`, requires a valid JSON user list,
preserves an existing user and propagates migration/list/create failures. It no
longer masks every error with `|| true`. It retains the official image entrypoint
for arbitrary Airflow UID setup. Defaults remain local demo credentials; the
script accepts `CARTCO_AIRFLOW_ADMIN_USERNAME`, `_PASSWORD` and `_EMAIL` in its
container environment. Existing users are preserved without changing roles or
passwords. A name match does not verify administrator privilege. No password
argument or captured CLI output is printed on failure. Actual pinned Airflow CLI
and container-entrypoint acceptance still require Docker.
