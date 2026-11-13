# Local completion worklist

Authority: user instruction on 12 November 2026: finish all work that can be done
locally, then guide the user through integration into the existing GitHub repo
and hosting. Publication is deferred to that manual walkthrough.

- [x] Isolated Spark/Iceberg/Nessie compatibility and Compose/Terraform recreation.
- [x] Four bounded protocol connectors, immutable evidence and failure/replay proof.
- [x] Actual Spark Silver/Gold, date-partitioned Bronze and catalog metadata (native runtime).
- [ ] Meaningful DAGs, DQ/lineage integration and Grafana health panels.
- [x] Coherent snapshot exporter and locally built/tested dashboard.
- [ ] Local deployment/recovery/acceptance runner and retained results.
- [x] Current README, diagrams, ADRs and project walkthrough/postmortem (verification limits explicit).
- [ ] Verified Git bundle, integration instructions and hosting walkthrough.

## Verified progress at 13 November checkpoint

- 296 unit checks passed; one Windows FIFO platform skip.
- Three real Spark tests passed against the independent Python/PyArrow reference.
- Four real local protocols passed: paginated HTTP GraphQL/SP-API, SSH/SFTP and
  a two-partition Kafka broker. Kafka replay, retained landing receipts and
  refusal of a broker checkpoint without its local durable landing were checked.
- A real Spark Bronze/Silver/Gold candidate was written through Nessie/MinIO;
  five catalogued Gold outputs passed exact reconciliation and seven GE suites.
- A genuine GE failure and a financial mismatch retained the prior publication;
  a bad export retained its valid snapshot.
- Marquez verified output schemas for 44 fields and 34 nonconstant derivations.
- The built React dashboard passed browser checks for channel filtering, mobile
  overflow, lineage/quality views, failed-refresh retention and read-only routes.
- Three Terraform mock plans passed, including the expanded assessed specification.

Still required: build/run the expanded assessed container image and four DAGs,
verify live Grafana/read-only DB access, complete the compaction/replay/recreation
checks, finish CI and project documentation, then refresh commits/bundle and the
manual GitHub/hosting guides. The new host runner is implemented but has not run.

Environment checkpoint: Windows relocated the workspace into OneDrive Documents.
The Python environments were repaired at the new path. Local execution then
became unavailable before shell launch with `MXC launcher: native MXC is
unavailable on this Windows build`. This is a tool/runtime blocker, not a passed
project check. Do not mark the remaining checklist complete from prepared code.

Update: shell execution returned after the runtime reset. Folder/socket access
needed to be restored for the relocated workspace. Continue the targeted checks;
the temporary launcher failure is retained here as diagnostic history.

Scope correction from the user: focus on the project only. Resume, personal
interview package, company targets and personal submission material are excluded.
User-dependent project items remain explicit: real owner sign-off, account
verification, free hosting capacity and remote CI. Do not invent these facts.

Keep the original baseline and volumes. New work uses isolated assessed-stack
resources. Do not count mock protocols as merchant onboarding, mocked Kafka
objects as broker acceptance, or a written recording script as a recorded video.

The repeating-decimal inventory regression passed in actual Spark (including
1/3 rounded once to 0.333333333333). Source bundle hashes now fence container
acceptance against stale code. The recreated-catalog and coordinated cold restore
checks are implemented and await host execution. Native compaction was interrupted
by unavailable local catalog service; no successful compaction result is claimed.
