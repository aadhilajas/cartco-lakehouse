import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  prepare,
  verifyPublication,
} from "../../scripts/prepare-publication.mjs";
function fixture() {
  return {
    schema_version: 1,
    is_synthetic: true,
    release_id: "r" + "a".repeat(32),
    daily_revenue: [
      {
        channel: "pos",
        order_date: "2026-11-14",
        order_count: 1,
        total_revenue: "9999999999999999.99",
      },
    ],
    channels: [],
    inventory: [],
    quality: {
      success: true,
      suites: Array.from({ length: 7 }, (_, i) => ({
        name: String(i),
        success: true,
      })),
    },
    catalog: { silver: { snapshot_id: "9223372036854775807" } },
    lineage: [],
    compute: {},
    execution: {},
    inventory_period: {},
    lineage_transport: { status: "verified" },
    source_bundle_sha256: "b".repeat(64),
    exported_at: "2026-11-14T00:00:00Z",
  };
}
test("publication rejects failed suites, missing lineage and rounded snapshot numbers", () => {
  const d = fixture();
  verifyPublication(d);
  d.quality.suites[0].success = false;
  assert.throws(() => verifyPublication(d));
  d.quality.suites[0].success = true;
  d.lineage_transport.status = "unverified";
  assert.throws(() => verifyPublication(d));
  d.lineage_transport.status = "verified";
  d.catalog.silver.snapshot_id = 9223372036854775807;
  assert.throws(() => verifyPublication(d));
});
test("incomplete acceptance cannot replace published results; successful publication preserves exact bytes", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "cartco-publication-"));
  try {
    const dest = path.join(root, "public");
    const { mkdir } = await import("node:fs/promises");
    await mkdir(dest);
    await writeFile(path.join(dest, "snapshot.json"), "previous valid release");
    const raw = JSON.stringify(fixture(), null, 2) + "\n";
    await writeFile(path.join(root, "public-snapshot.json"), raw);
    const provenance = { commit: "c".repeat(40), run_id: "123" };
    await assert.rejects(prepare(root, dest, provenance));
    assert.equal(
      await readFile(path.join(dest, "snapshot.json"), "utf8"),
      "previous valid release",
    );
    await writeFile(
      path.join(root, "SUCCESS.txt"),
      "Assessed scheduled ingestion, Spark medallion, GE, Marquez, Grafana, recreation, replay and dashboard acceptance passed.\n",
    );
    await prepare(root, dest, provenance);
    assert.equal(await readFile(path.join(dest, "snapshot.json"), "utf8"), raw);
    assert.equal(
      JSON.parse(await readFile(path.join(dest, "publication.json"), "utf8"))
        .commit,
      provenance.commit,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
