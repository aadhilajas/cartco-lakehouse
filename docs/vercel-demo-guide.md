# Public demonstration: Vercel and GitHub Actions

This route publishes a personal, non-commercial demonstration under Vercel Hobby
and standard GitHub-hosted runners in this public repository. Stay within the
included bandwidth, build, function and artifact-storage allowances. No paid
runner, custom domain, Codespace, cloud VM or Vercel Pro subscription is needed.
Provider free plans and limits can change. See the official
[Hobby plan](https://vercel.com/docs/plans/hobby) and
[GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions).

The site remains accessible between runs and displays the last published,
validated synthetic snapshot. Each requested run starts a temporary real Docker
stack on GitHub, runs scheduled ingestion and Spark processing, checks quality,
lineage, Grafana, recreation, replay and compaction, then publishes results.
The temporary stack stops afterward. This is not a persistent hosted lakehouse:
Airflow, Kafka, MinIO, Nessie, Marquez and Grafana are not public online services.
Local persisted data remains separate. The synthetic fixture totals can stay
identical across runs; run provenance and export time establish a new execution.

## 1. Review and merge hosting changes

Work on `hosting/vercel-demo`. Inspect `git status --short` and `git diff --stat`.
Stage only these files, then commit and push from normal PowerShell:

```powershell
git add dashboard .github/workflows/assessed.yml .github/workflows/publish-demo.yml docs/vercel-demo-guide.md docs/manual-hosting.md
git commit -m "Prepare verified snapshot demo for Vercel hosting"
git push -u origin hosting/vercel-demo
```

Create a pull request with base `main`, compare `hosting/vercel-demo`. Suggested
title: **Publish verified pipeline snapshots for Vercel demo**. Explain that
processing runs on demand in GitHub Actions and the site serves saved results.
Wait for required checks, inspect the diff, and merge manually. Then:

```powershell
git switch main
git pull --ff-only origin main
```

## 2. Run the first publication before connecting Vercel

1. Open the repository's **Actions** tab.
2. Select **Publish verified demo** in the sidebar.
3. Click **Run workflow**, select **main**, then click **Run workflow**.
4. Open the new run. Wait for the whole job to be green; first image builds can
   take time. The workflow has a 60-minute limit, not a promise of run duration.
5. Confirm the resulting main-branch commit contains both
   `dashboard/public/published/snapshot.json` and `publication.json`.
6. Open `publication.json`; its `run_url` must link to that successful run.

The workflow can run only on main and requires permission to commit these two
public files. If pushing fails with a permissions error, review **Settings →
Actions → General → Workflow permissions**. The job requests `contents: write`;
repository/organization policy may override this. If main protection requires
pull requests for every change, retain that protection and publish the generated
two files through a reviewed PR instead. Do not disable protections or force-push.
If another main commit arrived during the run, a push rejection is intentional:
rerun from current main after reviewing that change.

Until this first successful publication, the hosted build deliberately fails:
there is no invented fallback snapshot. Failed pipeline checks never overwrite
the last published result. Only the two privacy-safe files are committed; raw
payloads, credentials, volumes and full logs are not website assets.

## 3. Create the Vercel project manually

1. Sign in to [Vercel](https://vercel.com/) using GitHub. Use a personal **Hobby**
   account. Stop if asked to select a paid plan or add a payment method.
2. Choose **Add New → Project** and import `aadhilajas/cartco-lakehouse`.
   If GitHub access is requested, grant this repository only.
3. Set **Root Directory** to `dashboard` and framework to **Vite**.
4. Use Node.js **24.x**, production branch **main**, output directory **dist**.
5. The checked-in `dashboard/vercel.json` supplies these commands:
   - Install: `npm ci --ignore-scripts --no-audit --no-fund`
   - Build: `npm run build:hosted`
6. Leave environment variables empty. This version needs no GitHub personal
   token, Vercel token, database password or backend URL in Vercel.
7. Click **Deploy**. Once ready, open the assigned `.vercel.app` address.

The hosted build validates the published JSON, copies it into the static output,
and selects `/published/snapshot.json` rather than localhost `/api/snapshot`.
Local builds keep their existing read-only backend connection. Vercel config
also supplies response security headers and disables snapshot caching.
See [Vite on Vercel](https://vercel.com/docs/frameworks/frontend/vite).

## 4. Verify the public site

- Open it in an incognito window, then on a phone using mobile data.
- Confirm Business, Engineering and Lineage load without localhost access.
- Check release ID/export time and compare them with the committed snapshot.
- Check channel/date filters and unavailable inventory values.
- Confirm the site says saved results are from an on-demand run. Refresh reloads
  the snapshot; it does not start processing.
- Open `/published/publication.json` on the same website and follow `run_url`.
- With your PC switched off, check that the site still loads.

## 5. Refresh results for a demonstration

Run **Publish verified demo** again from Actions on main. A successful run commits
a new snapshot and provenance; Vercel's Git integration should create a new
production deployment. Check Vercel's deployment status before claiming the site
updated. If no deployment appears, inspect project Git integration/commit-author
access and manually redeploy the latest main commit through Vercel.

No browser pipeline-trigger endpoint is included: the repository owner triggers
runs while signed in to GitHub. Public visitors can view the site and public run
evidence. This avoids distributing credentials or exposing an unrestricted
compute trigger. A protected in-site trigger can be a later separate change.

If new results or a deployment fail, inspect the run/deployment logs and retain
the previous working deployment. Do not replace a failed result with fake metrics.
Evidence artifacts have three-day retention to reduce storage use; download any
needed viva evidence promptly. Monitor usage and remove obsolete artifacts before
exceeding included storage. Free-runner execution does not mean unlimited storage.

## Deployment status

Preparation and local tests do not establish successful remote publication.
Record the first successful run URL, deployed commit, Vercel URL and checks above
only after they actually complete. The full-server alternative remains in
`manual-hosting.md`; it is a different hosting model.
