import { readFile, mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { validate } from "../src/lib/model.js";

export function verifyPublication(data) {
  validate(data);
  if (
    data.quality.suites.length !== 7 ||
    data.quality.suites.some((s) => s.success !== true) ||
    data.lineage_transport.status !== "verified"
  ) {
    throw new Error(
      "Publication requires all seven quality suites and verified lineage",
    );
  }
  if (
    !/^[a-f0-9]{64}$/.test(data.source_bundle_sha256 || "") ||
    !Number.isFinite(Date.parse(data.exported_at))
  ) {
    throw new Error("Publication requires source identity and export time");
  }
  for (const table of Object.values(data.catalog)) {
    if (
      table.snapshot_id !== null &&
      (typeof table.snapshot_id !== "string" ||
        !/^\d+$/.test(table.snapshot_id))
    ) {
      throw new Error("Snapshot identities must be exact decimal strings");
    }
  }
  return data;
}

export async function prepare(evidence, destination, provenance) {
  // SUCCESS is written only after recreation, replay, compaction and baseline checks.
  const success = await readFile(path.join(evidence, "SUCCESS.txt"), "utf8");
  if (!success.startsWith("Assessed scheduled ingestion,"))
    throw new Error("Acceptance did not complete");
  const raw = await readFile(
    path.join(evidence, "public-snapshot.json"),
    "utf8",
  );
  const data = verifyPublication(JSON.parse(raw));
  if (
    !/^[a-f0-9]{40}$/.test(provenance.commit || "") ||
    !/^\d+$/.test(provenance.run_id || "")
  )
    throw new Error("Missing GitHub run identity");
  await mkdir(destination, { recursive: true });
  // Copy bytes rather than round-tripping exact decimal/snapshot identifiers.
  await writeFile(path.join(destination, "snapshot.json"), raw);
  await writeFile(
    path.join(destination, "publication.json"),
    JSON.stringify(
      {
        ...provenance,
        release_id: data.release_id,
        exported_at: data.exported_at,
        source_bundle_sha256: data.source_bundle_sha256,
        published_at: new Date().toISOString(),
        mode: "GitHub Actions batch; published snapshot; services stop after the run",
      },
      null,
      2,
    ) + "\n",
  );
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const [evidence, destination] = process.argv.slice(2);
  if (!evidence || !destination)
    throw new Error("Usage: node prepare-publication.mjs EVIDENCE DESTINATION");
  await prepare(evidence, destination, {
    commit: process.env.GITHUB_SHA,
    run_id: process.env.GITHUB_RUN_ID,
    run_url: `https://github.com/${process.env.GITHUB_REPOSITORY}/actions/runs/${process.env.GITHUB_RUN_ID}`,
  });
}
