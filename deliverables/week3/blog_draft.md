# Blog draft — "Three version mismatches, one lakehouse" (working title)

Status: draft outline with real content filled in per section, not yet
polished into final prose. Based entirely on what actually happened in this
repo (see `README.md` Known Limitations, `docs/lineage.md`, `docs/test_report.md`
for the source material this draws from).

## Angle

Every write-up about building a Bronze/Silver/GE pipeline makes it sound
like you pick the tools, write the transform, and it works. It didn't, three
separate times, for three unrelated reasons — and all three were caused by
the same mistake: trusting a library's documented API instead of the
version actually installed. This post is about that pattern, using the
three real bugs from this project as evidence.

## Outline

**1. The setup (2-3 sentences, not the focus)**
Bronze/GE/Silver pipeline for one channel (Shopify) of a multi-channel
retail lakehouse. Airflow + Iceberg + MinIO + Great Expectations + a manual
OpenLineage → Marquez integration. Link back to the architecture diagram.

**2. Bug #1 — Great Expectations' Fluent API doesn't match its own current docs**

`transforms/ge_bronze_shopify_checkpoint.py` was originally written against
the GX Core 1.0+ pattern (`context.data_sources`,
`add_batch_definition_whole_dataframe()`, `batch.validate(expectation)`) —
which reads as the current way to do it if you're looking at
current-version docs. The actual pinned version in this repo is
`great_expectations==0.18.19`, which has none of that. It has
`context.sources.add_pandas()` → `add_dataframe_asset()` →
`build_batch_request()` → `context.get_validator()` → one
`validator.expect_<...>()` call per expectation. Found by running
`python -c "import great_expectations as gx; ..."` inside the actual
container and inspecting what was really there, not by reading more docs.

**3. Bug #2 — a return-type change in a pinned dependency**

`pyarrow==15.0.2`'s `ChunkedArray.combine_chunks()` returns a flat `Array`
directly. The dedup logic in `transforms/silver_shopify_orders.py` was
written assuming it returned a single-chunk `ChunkedArray` (true in older
pyarrow versions) and called `.chunk(0)` on the result, which doesn't exist
on a plain `Array`. One-line fix, but it only showed up as an
`AttributeError` in a live Airflow task log — nothing about it was visible
by reading the transform code in isolation.

**4. Bug #3 — the same mistake, one more time, in the lineage work**

Pinned `openlineage-python==1.50.0` for the new Marquez integration, wrote
`lineage/emitter.py` against the `openlineage.client.event_v2` module
documented on openlineage.io today. Two problems: that version conflicted
with `apache-airflow-providers-openlineage`'s own transitive pin
(`1.11.3`, already present in the base Airflow image), and `event_v2`
doesn't exist on `1.11.3` at all — its `RunEvent`/`Job`/`Run`/`Dataset`
classes live in `openlineage.client.run` instead. Re-pinned to match what
was actually already resolved, re-verified the import live in the
container before writing another line of code against it.

**5. The actual lesson**

Not "read the docs more carefully" — the docs were fine, for a different
version. The fix that actually worked every time was: `pip show <package>`
or a one-line live import check inside the real runtime *before* writing
code against an API, not after it breaks. That's now a standing rule for
this project (see the repo's own guidance on this), and it's the reason
all three of these bugs have a paper trail of "verified against the
installed version" in their commit history instead of "assumed from
memory."

**6. What I'd do differently next time**

Pin dependency versions in one place and check for conflicts *before*
adding a new library, not after `pip install` prints a resolver warning
(which is exactly what caught the `openlineage-python` conflict — it
almost got missed). Possibly worth a small `pip check` step in CI so a
future dependency bump can't silently reintroduce this class of bug.

## Not yet written

The actual prose. This is deliberately still an outline with real
technical content per bullet, not a finished post — turning it into
publishable prose is Week 4 work, not Week 3.
