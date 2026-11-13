# Project presence artifact

Presence Option D is prepared locally: the README, architecture diagrams, actual
browser GIF and evidence-backed implementation can be included in the existing
repository's public release after manual integration and CI. No public release
or hosted URL is claimed yet.

`assets/demo.gif` contains six actual browser-rendered states from the accepted
Airflow-produced synthetic snapshot. It shows the complete business release,
the real Shopify filter (INR 301.50), charts, seven quality suites, Airflow run
provenance, catalog metadata and verified column lineage. The GIF has six frames
at 2.5 seconds each; its snapshot checksum and release ID are retained in
`assets/demo.gif.json`. It is a short interface demonstration, not a narrated
five-minute walkthrough, final pitch or personal interview recording.

`dashboard/capture-demo.mjs` captures the states through Playwright and assembles
them with Sharp. It accepts `--python`, `--snapshot`, `--output`, optional
`--chrome`, and `--sharp` (an installed Sharp package path). Use the built
dashboard and a quality/lineage-verified snapshot. No business values are painted
into screenshots or invented for this artifact.

After manual GitHub integration, add the real release URL and CI run links here.
After hosting qualification, add the genuinely reachable HTTPS dashboard and
backend health URLs. A local path or static snapshot page is not that hosted
backend evidence.
