# ADR-013: Isolated Spark and persistent Nessie foundation

Date: 2026-11-12
Status: accepted for local amd64 Compose and Terraform; hosting/ARM qualification pending

## Context

The supplied B1 assignment requires PySpark, Terraform and a listed catalog. The
working PyArrow/PyIceberg + REST/JDBC baseline is valuable regression evidence,
but does not satisfy those requirements. Its data must not be migrated or removed
as a side effect of introducing the assessed runtime. Budget is zero and no
server/account capacity has been verified. The viva is 1 December 2026.

## Decision

Use a separate Spark image with Spark 3.5.6, Scala 2.12, Python 3.11 and Java 17.
Pin Iceberg 1.12.0 runtime/AWS jars with SHA-512 checksums, and keep this runtime
separate from Airflow's hash-locked Python/SQLAlchemy environment. Use local[2]
for bounded batch demonstrations; no standalone Spark cluster is necessary.
The specific image combination passed all nine real compatibility checks under
both Compose and Terraform on 12 November 2026; see the infrastructure ledger.

Select Nessie 0.110.0 as the locally accepted listed catalog. Store its version
history in a dedicated PostgreSQL database through JDBC2. Spark uses Iceberg's
NessieCatalog and S3FileIO with MinIO. Nessie's optional SQL extensions are not
needed for this basic table path. Iceberg's packaged Nessie client version is
0.108.8; API v2 interoperability with the selected server passed the real probe
and exact checkpoint comparison after storage/catalog container recreation.

Use one JSON service specification for both Compose and Terraform. Terraform
provisions images, network, named volumes and containers on an existing Docker
host. It does not claim to create a free cloud VM. Resource names differ between
baseline, Compose and Terraform; host ports intentionally overlap between the
two new alternatives so only one can run at a time. Terraform volumes have
prevent_destroy protection. Neither entrypoint deletes baseline resources.

## Alternatives

- Keep generic REST/JDBC only: preserves the existing application but does not
  satisfy the named-catalog requirement.
- Hive Metastore: a valid fallback if the Nessie spike fails, with more Hadoop/
  metastore configuration to validate. Do not deploy both for presentation.
- Glue: requires a cloud account and a separate cost/credential assessment.
- Spark 4 migration or Kubernetes: adds compatibility work beyond this bounded
  assignment. Neither is required to demonstrate distributed compute concepts.
- Embed Spark into the current Airflow environment immediately: couples JVM and
  Python dependencies before the compute combination has been verified.

## Consequences and evidence boundaries

The new foundation does not migrate existing Bronze/Silver/Gold tables. Four
protocol connectors, assessed commerce Spark models and 3-5 meaningful production
DAGs remain later phases. The paused diagnostic DAG is not counted as a business
pipeline deliverable.

Acceptance creates a unique synthetic Iceberg table and proves decimal sums,
MERGE, date partitioning, additive schema evolution and snapshot time travel.
After MinIO, PostgreSQL and Nessie container recreation, it compares the table
UUID, metadata pointer, retained snapshots, schema, properties and exact row hash.
Mock Terraform plans validate wiring; they are not proof of terraform apply or
container behavior.

The registry audit found the pinned Marquez API/web images are amd64-only. The
other audited base images advertise ARM support, which is not ARM execution proof.
A free Oracle ARM VM therefore remains a candidate with unresolved Marquez build,
capacity, account and measured-memory gates. No emulated stack is assumed viable.

## Primary references

- [Spark 3.5.6 runtime support](https://spark.apache.org/docs/3.5.6/)
- [Nessie with Spark and Iceberg](https://projectnessie.org/iceberg/spark/)
- [Nessie JDBC2 persistence settings](https://projectnessie.org/nessie-0.110.0/configuration/)
- [Pinned Iceberg Nessie dependency POM](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-nessie/1.12.0/iceberg-nessie-1.12.0.pom)
- [Terraform Docker provider](https://registry.terraform.io/providers/kreuzwerker/docker/3.6.2/docs)
