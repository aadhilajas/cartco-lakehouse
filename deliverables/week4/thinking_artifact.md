# What made the lakehouse trustworthy

The hardest part was defining what a successful run actually proves. A green
Airflow task does not prove that all source deliveries are present, that a retry
has not duplicated a commit, or that a lineage backend indexed the event.

We made each boundary explicit. Raw deliveries retain their original bytes and
contracts. State merges use source versions and preserve tombstones. Gold pins
one state snapshot and checks completion receipts for every required channel.
Exact decimal reconciliation and GE suites both run before new publication.
A consumer sees one atomic pointer after complete immutable files are ready.

Recovery testing exposed a useful mistake in the acceptance checker. It assumed
one Iceberg snapshot per source merge. PyIceberg 0.7.1 actually records delete
and append snapshots for an overwrite of a populated partition, within one
transaction. The fix was not to loosen the assertion: we checked parent history,
operations, batch IDs and receipt snapshot IDs, and added a real-file regression.

The worker drill separated business completion from task completion. POS ingestion
committed and wrote its receipt, then the test task was killed. Airflow retried
in a different process and reused that receipt. An exact catalog comparison
before the kill and after the retry showed no additional commit. One retry alert
remains visible; declaring the history clean would hide useful incident evidence.

We retained PyArrow/PyIceberg for the local implementation because those are the
actual tested execution APIs. Spark remains a documented design deviation and a
future capacity decision. Adding tools merely to match a diagram would weaken
reproducibility without proving a workload requirement.

The result is a scoped local system with reproducible evidence. A tested local
restore is not an offsite backup strategy; one post-commit kill is not arbitrary
mid-commit recovery; schema facets alone are not column derivations; and a saved
client event is not backend indexing. Keeping those distinctions explicit made
the final implementation and its remaining boundaries easier to review.
