# Public project presence

Updated **14 November 2026**.

- [Public dashboard](https://cartco-lakehouse.vercel.app/)
- [GitHub repository](https://github.com/aadhilajas/cartco-lakehouse)
- [First publication run](https://github.com/aadhilajas/cartco-lakehouse/actions/runs/43552077988)
- [Verified update run](https://github.com/aadhilajas/cartco-lakehouse/actions/runs/43554685157)
- [Public run provenance](https://cartco-lakehouse.vercel.app/published/publication.json)

Vercel serves saved, validated synthetic results. The current workflow performs
real processing and acceptance on demand in GitHub Actions, then publishes the
snapshot. It does not host public, persistent Airflow/MinIO/Marquez services.
The [deployment checkpoint](validation/public-demo-checkpoint.json) records the
verified run/release/source identities and the scope of endpoint checks.

## Existing visual artifact

`assets/demo.gif` contains six actual browser-rendered states from an earlier
accepted Airflow-produced synthetic snapshot. It includes the business view,
Shopify filter, quality evidence, provenance, catalog and lineage. Its checksum
and original release remain in `assets/demo.gif.json`; it is not relabelled as a
capture of the newer public deployment. Frames are 2.5 seconds each.

`dashboard/capture-demo.mjs` generates the artifact from a built dashboard and
quality/lineage-verified snapshot. Its accepted arguments include `--python`,
`--snapshot`, `--output`, optional `--chrome`, and `--sharp`.

A final narrated presentation has not been recorded in the project checklist.
The GIF, website and [written walkthrough](project-walkthrough.md) are distinct
artifacts and must not be represented as that recording.
