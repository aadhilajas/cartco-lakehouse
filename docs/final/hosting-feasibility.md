> Historical planning/handoff document. These dated decisions and pending items
> record an earlier project stage. For current implementation and the selected
> public demo hosting model, use the [documentation index](../README.md) and
> [status ledger](../local-completion-checklist.md).

# Zero-budget hosted backend feasibility

Reviewed: 2026-11-11. Decision status: candidate research only; no deployment.

## Fixed requirements

All four protocol connectors, Spark medallion compute, persistent Iceberg storage
and a listed catalog, orchestration, DQ, lineage and Grafana remain required.
A hosted dashboard alone does not satisfy the hosted backend requirement.
The user authorizes publishing reviewed code/dashboard, with zero paid spending
and no existing server. No paid account upgrade or billable overage is authorized.

## Provider evidence

- [Oracle Always Free documentation](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm):
  current A1 allowance for Always Free tenancies is 2 OCPUs/12 GB RAM, with 200 GB
  total free block storage including boot volumes. Capacity can be unavailable;
  idle instances may be reclaimed. Do not repeat older 4 OCPU/24 GB claims.
- [Oracle signup FAQ](https://www.oracle.com/cloud/free/faq/): account signup needs
  identity/card verification and can create a temporary bank authorization hold.
  User must handle signup privately; a free account is not assumed available.
- [Render free service limits](https://render.com/docs/free): services sleep after
  inactivity and lose local files on restart/redeploy; free persistent disks are
  unavailable. This is not a selected host for persistent MinIO/catalog data.

## Phase 1 checks before selection

1. Keep the existing x86 baseline untouched. Test ARM availability for every pinned
   image and Spark/Java/Python/native dependency, or build verified ARM variants.
2. Measure idle and peak memory/CPU/disk on the bounded demo dataset. Candidate
   12 GB capacity is a hypothesis, not proof that the whole stack fits.
3. Use sequential batch execution where suitable, without removing required
   backend services or substituting a fake public API for the real pipeline.
4. Validate persistent volumes, restart/replay, backup/restore, HTTPS and access
   control. Keep storage credentials, administrative UIs and write endpoints private.
5. Verify actual free quotas in the account before provisioning. Use only free
   eligible resources; no paid upgrade or trial-dependent deployment. Record the
   actual region, architecture, limits, URLs and acceptance evidence afterward.
6. If free capacity/account signup fails, seek a genuinely available institutional
   server or evaluator guidance. Hosting stays incomplete until genuinely working.
   Local work and the reproducible baseline continue independently.

No cloud account, VM, public endpoint or provider compatibility has been verified
by this research. The November schedule is a target, not a hosting guarantee.

## Registry audit update, 2026-11-12

The pinned Marquez API and web images have amd64 manifests only. Nessie,
PostgreSQL, Python/Airflow and MinIO build bases advertise arm64, which is not
ARM execution proof. See ../../infra/compatibility/image-platforms.json. An ARM Marquez
build/repack and actual testing are required before selecting Oracle A1; no
emulation performance or full-stack fit is assumed. The x86 local compatibility
run is independent and does not close this hosting gate.

## Local measurement update, 2026-11-12

Compose and Terraform both passed real amd64 Spark/MinIO/Nessie compatibility and
storage/catalog recreation. Thirteen concurrent sample times during the Compose
probe recorded a peak container sum of 2,409,522,791 bytes (about 2.24 GiB),
excluding host OS/cache and unsampled peaks. The five-row fixture is compatibility
evidence, not whole-application capacity qualification. Account, regional free
capacity, ARM Marquez/runtime execution and secure hosted deployment remain open.
