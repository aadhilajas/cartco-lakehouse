# CartCo dashboard

React/Vite website for one validated synthetic commerce release.

```text
src/
  main.jsx             React entrypoint and stylesheet import
  App.jsx              Shared filter/tab state and page composition
  components/          Sidebar, header, cards, empty state and icons
  views/               Business, Engineering and Lineage
  hooks/useSnapshot.js Fetch, validation and last-good-result retention
  lib/                 Exact-money/filter model and deployment configuration
  styles/main.css      Existing responsive styling
scripts/               Publication preparation/checking and GIF capture
tests/unit/            Model and publication gates
tests/browser/         Local and hosted browser acceptance
public/published/      Intentional versioned snapshot and run provenance
```

From the repository root:

```bash
npm --prefix dashboard ci
npm --prefix dashboard test
npm --prefix dashboard run build
npm --prefix dashboard run build:hosted
```

Local mode reads `/api/snapshot` through the read-only local server. Hosted mode
reads `/published/snapshot.json`; it requires matching validated public snapshot
and provenance files. Filter state lives in App so switching views retains it.
The model keeps money as exact strings/BigInt cents and distinguishes unavailable
from zero. Refresh validates new data before replacing the last good snapshot.

The Vercel root remains `dashboard`. Docker copies the whole `src/` tree for the
local dashboard build. No dependency versions changed for the folder refactor.
Node must satisfy `package.json`; CI uses its explicitly selected version.

`tests/browser/check-ui.mjs` accepts `--python`, `--snapshot`, `--output`, optional
`--chrome`, and `--hosted true`. Run it from the repository root after the relevant
build. It uses the same read-only server to serve assets and an approved fixture.
The PR workflow checks both modes. Publication separately checks its freshly
computed snapshot before committing public results.

[Hosting guide](../docs/vercel-demo-guide.md) · [Testing](../tests/README.md)
