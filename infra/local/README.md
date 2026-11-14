# Current local application

`stack.json` defines the bounded synthetic protocol/Spark/Nessie application used
by scheduled acceptance and temporary GitHub publication runs. It declares nine
volumes, eight active by default, and 19 default services. The optional Spark smoke
profile is a separate finite acceptance tool.

Run `python scripts/acceptance/run_assessed_stack.py` from the repository root.
Windows and Terraform options are in the [runbook](../../docs/assessed-runbook.md).
Dockerfiles and Grafana/DAG assets specific to this application live here;
shared Java/Spark locks, MinIO, Airflow base and Marquez inputs live in `../shared`.

Existing names such as `cartco-assessed`, `assessed_commerce_release` and
`nessie.cartco_assessed` are retained resource/data identities. The directory
rename does not authorize volume renaming, state resets or deleting older data.
The public Vercel site serves exported results; it does not run these services.
