// Genuine browser-rendered interface states from one accepted snapshot.
// This GIF is an interface demonstration, not a narrated walkthrough recording.
import { chromium } from "playwright";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import path from "node:path";
import net from "node:net";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";

const args = Object.fromEntries(
  process.argv.slice(2).reduce((out, value, index, all) => {
    if (value.startsWith("--")) out.push([value.slice(2), all[index + 1]]);
    return out;
  }, []),
);
for (const key of ["python", "snapshot", "output"])
  if (!args[key]) throw new Error(`--${key} required`);
const sharp = createRequire(import.meta.url)(args.sharp || "sharp");
const repo = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../..",
);
const raw = await readFile(args.snapshot),
  snapshot = JSON.parse(raw);
assert(
  snapshot.quality.success && snapshot.lineage_transport.status === "verified",
);
const probe = net.createServer();
await new Promise((resolve) => probe.listen(0, "127.0.0.1", resolve));
const port = probe.address().port;
await new Promise((resolve) => probe.close(resolve));
const server = spawn(
  path.resolve(args.python),
  [
    path.join(repo, "scripts/runtime/serve_dashboard.py"),
    "--dist",
    path.join(repo, "dashboard/dist"),
    "--snapshot",
    path.resolve(args.snapshot),
    "--port",
    String(port),
  ],
  { cwd: repo, windowsHide: true, stdio: "ignore" },
);
let browser;
try {
  let ready = false;
  for (let n = 0; n < 80; n++) {
    try {
      if ((await fetch(`http://127.0.0.1:${port}/health`)).ok) {
        ready = true;
        break;
      }
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  assert(ready, "Local snapshot server did not start");
  browser = await chromium.launch({
    headless: true,
    ...(args.chrome ? { executablePath: args.chrome } : {}),
  });
  const page = await browser.newPage({
    viewport: { width: 1280, height: 900 },
    deviceScaleFactor: 1,
  });
  const frames = [],
    states = [];
  const capture = async (name) => {
    await page.waitForTimeout(150);
    frames.push(
      await sharp(await page.screenshot())
        .resize(960, 675)
        .removeAlpha()
        .raw()
        .toBuffer(),
    );
    states.push(name);
  };
  await page.goto(`http://127.0.0.1:${port}`, { waitUntil: "networkidle" });
  await page.locator(".release-strip").waitFor();
  await capture("Business: complete accepted synthetic release");
  await page.locator("select").selectOption("shopify");
  assert.equal(
    await page.locator(".metric strong").first().textContent(),
    "₹301.50",
  );
  await capture("Business: actual Shopify filter");
  await page.getByRole("button", { name: "Reset filters" }).click();
  await page.evaluate(() => window.scrollTo(0, 450));
  await capture("Business: daily/channel charts and declared inventory scope");
  await page.getByRole("button", { name: "Engineering", exact: true }).click();
  await page.evaluate(() => window.scrollTo(0, 0));
  await capture("Engineering: real quality suites and Airflow provenance");
  await page.locator("details summary").first().click();
  await capture("Engineering: actual catalog identity and schema");
  await page.getByRole("button", { name: "Lineage", exact: true }).click();
  await page.evaluate(() => window.scrollTo(0, 0));
  await capture("Lineage: verified output field derivations");
  await mkdir(path.dirname(args.output), { recursive: true });
  await sharp(Buffer.concat(frames), {
    raw: {
      width: 960,
      height: 675 * frames.length,
      pageHeight: 675,
      channels: 3,
    },
  })
    .gif({ loop: 0, delay: states.map(() => 2500), colours: 128 })
    .toFile(args.output);
  const metadata = await sharp(args.output, { animated: true }).metadata();
  assert.equal(metadata.pages, states.length);
  await writeFile(
    args.output + ".json",
    JSON.stringify(
      {
        release_id: snapshot.release_id,
        snapshot_sha256: createHash("sha256").update(raw).digest("hex"),
        states,
        frames: metadata.pages,
        frame_delay_ms: 2500,
        kind: "Actual browser states; no audio or narrated recording",
      },
      null,
      2,
    ),
  );
  console.log("Verified browser GIF:", args.output, metadata.pages, "frames");
} finally {
  if (browser) await browser.close();
  server.kill();
}
