# Final local walkthrough

Use a disposable demo stack with the explicit fixture settings in the README.
Keep existing data and volumes intact. The versioned DAG is paused/manual by
default; delivered inputs are the default outside an explicitly synthetic demo.

1. Build/start Compose. Show `docker compose ps`, successful initialization and
   healthy Airflow/MinIO/Postgres. Explain JDBC catalog persistence.
2. Run `scripts/check_dags.py` in the Airflow image. Show four real DAGs and
   their preserved serialization graphs.
3. Run the six-stage versioned DAG on three frozen source deliveries. Show raw
   hashes, source versions, decimal state and matching completion receipts.
4. Show the immutable Gold release, pinned snapshot and CURRENT pointer.
   Explain completed-sales filters, exact cents and optional inventory evidence.
5. For new releases, inspect `quality.great_expectations` in manifest.json.
   For a retained older release, run `scripts/check_output_quality.py` against
   its pinned state snapshot. A retrospective check does not rewrite old files.
6. Open Marquez at localhost:3000. Inspect source normalization and Gold column
   dependencies, schema, snapshot and row counts. Compare backend graph results
   with the local immutable intended-event/client-delivery journal.
7. Replay the same run. Demonstrate unchanged data/snapshots and publication.
   Explain that new business deliveries and cached replay are different cases.
8. Review the recorded catalog recreation, archive inventories and restored
   service boot. On isolated copies, show the fresh synthetic POS task kill,
   retry in a new process, identical pre/post-retry catalog and retained alert.
9. Finish with the test report and scope ADR. State the PyArrow compute decision
   and the unexecuted cloud/remote-publication/video milestones clearly.

`bash scripts/ci_baseline.sh` runs the pipeline/replay/live-test sequence on a
disposable stack. Recovery helpers and acceptance DAGs are documented in
`docs/local-recovery.md`; normal Compose does not mount the fault-injection DAG.
Never run the task-kill acceptance DAG against original or production volumes.

Suggested recording length: 8-12 minutes. Narration should identify synthetic
fixtures and the exact failure case demonstrated. Publish a real video URL only
after recording and uploading are authorized and completed.
