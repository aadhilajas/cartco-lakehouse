import { readFile } from "node:fs/promises";
import { verifyPublication } from "./prepare-publication.mjs";
const data = verifyPublication(
  JSON.parse(
    await readFile(
      new URL("../public/published/snapshot.json", import.meta.url),
      "utf8",
    ),
  ),
);
const meta = JSON.parse(
  await readFile(
    new URL("../public/published/publication.json", import.meta.url),
    "utf8",
  ),
);
if (
  meta.release_id !== data.release_id ||
  meta.source_bundle_sha256 !== data.source_bundle_sha256
)
  throw new Error("Publication provenance differs from snapshot");
console.log("Validated published release:", data.release_id);
