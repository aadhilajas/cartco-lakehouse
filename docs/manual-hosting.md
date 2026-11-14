# Manual hosting walkthrough

For the selected no-card public demonstration using Vercel and on-demand GitHub
Actions, follow [the Vercel demo guide](vercel-demo-guide.md). The server guide
below describes a separate persistent-backend option, not the selected demo route.

Reviewed 13 November 2026. Budget remains zero. This guide prepares the sequence;
no cloud account, VM, paid service, public URL or successful hosted deployment is
claimed. Finish local acceptance and GitHub integration first.

## 1. Establish a real free backend target

We need a Linux Docker host with persistent disk and enough measured memory for
the complete assessed stack. A static frontend service does not host Kafka,
Nessie/PostgreSQL, MinIO and the scheduled Spark pipeline.

Oracle A1 is a candidate, not a reservation. Its current official Always Free
allowance is equivalent to 2 OCPUs/12 GB for A1, with 200 GB combined free block
storage including boot volumes. Capacity is not guaranteed. Signup involves
identity/card verification and may create a temporary authorization hold; handle
it privately. Do not upgrade to a paid account or use trial-only resources.
See [Always Free resources](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm)
and [signup FAQ](https://www.oracle.com/cloud/free/faq/).

**Architecture gate:** the qualified local stack is amd64. The pinned upstream
Marquez API/web images advertise amd64 only. Other ARM-capable image manifests
do not establish ARM execution compatibility. Do not rent an ARM VM or silently
emulate these components and call it verified. A1 needs a reviewed/tested ARM
Marquez build or another justified compatible deployment before selection.
An actually available institutional amd64 Linux host is another zero-spend route.
If neither account/capacity nor a compatible host is available, hosting remains
an unmet requirement; do not replace it with a misleading local-only claim.

When a host becomes available, record its actual architecture, CPU/RAM, disk,
free-eligibility label, region and retention limits. Measure this project's real
peak during acceptance before declaring the shape adequate. The earlier 2.24 GiB
sample was a tiny compatibility probe, not the expanded-stack sizing result.

## 2. Secure the host before installing the project

Create a private SSH key on your computer and keep the private key there. Add
only its public key to the server. Do not paste credentials into chat or Git.
Allow SSH from your own IP; expose neither Docker's API nor the data/admin ports.

Install Docker Engine and Compose using the provider/OS's official instructions.
Use a normal non-root operator account with the required Docker access. Keep
persistent storage on the host disk; do not put the object store or databases on
an ephemeral container filesystem.

## 3. Check out the reviewed GitHub version

On the server, clone the existing repository, then select the actual reviewed
merge commit/tag. Record `git rev-parse HEAD`. Do not deploy an unreviewed local
ZIP over a previous checkout. Keep runtime secrets in an ignored, access-limited
server file, not in the repository or a frontend build variable.

For an initial private smoke run, keep all Compose port bindings on loopback.
Before making anything public, replace the local fixture passwords consistently
across PostgreSQL clients, Airflow administrator, MinIO, Marquez and Grafana.
Preserve already-initialized database credentials or deliberately rotate them;
changing a container environment variable alone does not change a stored PG role.
The checked-in fixture specification is not a public-production secret profile.

## 4. Run the same acceptance on the server

```bash
python3 scripts/acceptance/run_assessed_stack.py
```

Use the server's actual Python 3.11+ and Docker access. Keep its full generated
evidence. Verify the four DAG runs, seven quality suites, persisted lineage,
Grafana datasource and published snapshot. Compare the hosted release ID and
tables with the actual backend; do not manually type attractive sample metrics.

The Terraform alternative is documented in `assessed-runbook.md`. Run Terraform
on the chosen Docker host with its isolated assessed workspace/state, and inspect
its real plan before apply. It provisions containers on an existing host; it
does not create a free cloud VM or guarantee provider quota.

## 5. Verify protected operator access first

From your computer, forward administrative ports through SSH:

```bash
ssh -L 18080:127.0.0.1:18080 -L 13030:127.0.0.1:13030 -L 18100:127.0.0.1:18100 YOUR_USER@YOUR_SERVER_IP
```

Use the forwarded Airflow, Grafana and dashboard URLs locally. Keep that SSH
session open. Replace the placeholders with your real user/server; they are not
working project URLs. Confirm login, pipeline execution and snapshot refresh.
This verifies a hosted backend, even though admin interfaces remain private.

## 6. Publish only the read-only dashboard over HTTPS

Use an existing or genuinely free hostname you control; do not buy a domain under
the zero budget. Point its DNS record at the actual host. Install Caddy using its
official instructions and adapt `infra/hosting/Caddyfile.example`. It reverse
proxies only the dashboard/API on 127.0.0.1:18100. Caddy's
[reverse-proxy guide](https://caddyserver.com/docs/quick-starts/reverse-proxy)
explains the required running upstream and hostname-based HTTPS configuration.

Allow public 80/443 as needed for HTTPS/certificate issuance. Keep 18080, 13030,
13000, 15000, 19000, 19001, 19120 and database/Kafka/SFTP ports private. Do not
publish an unauthenticated pipeline trigger, SSH executor or Docker socket.

From a separate browser/network, verify:

- HTTPS works without certificate warnings.
- Business, Engineering and Lineage load the genuine release.
- Source/batch, compute and export times remain distinguishable.
- Failed refresh retains the last valid data; unknown/zero are distinct.
- POST/write routes are refused and internal admin services are unreachable.

Do not add a public URL to the README until it has been tested this way.

## 7. Optional separate static frontend

The built React app can also use a published `snapshot.json` by setting
`VITE_SNAPSHOT_URL=/snapshot.json` at build time. Copy only the actual privacy-safe
export into `dashboard/public/snapshot.json`, then build. Never copy the private
candidate, raw protocol archive, customer IDs or credentials into public assets.

For a personal project, evaluate the current
[Vercel Hobby plan](https://vercel.com/docs/plans/hobby) before choosing it. If
used, configure the dashboard root, `npm ci && npm run build`, and `dist` output.
This does not remove the separately required hosted backend. A static snapshot
is labelled and timestamped as a snapshot, not falsely presented as a live query.
Updating it needs an actual successful backend release and a new safe export.

## 8. Persistence, backup and final evidence

Retain the object store, Nessie database, Kafka log/checkpoints, Airflow/Marquez
metadata and runtime/publication state together. Stop/quiesce writers before a
coordinated backup. A copy of source code or Git bundle is not a data backup.
Do not reuse the earlier SQLite/file recovery proof as evidence for this stack.

Exercise container recreation and host restart on the hosted stack. Verify exact
table UUIDs, snapshot identities, rows and release pointer afterward. Restore a
backup into isolated new resources before claiming recovery acceptance. Keep
actual logs, versions, URLs and checksums. Free capacity/reclaim policies still
require a recoverable local copy and a deployment fallback.

Only after these checks should we mark hosting complete and prepare the final
project release. No cloud purchase, account upgrade, form submission or credential
sharing is part of this guide.
