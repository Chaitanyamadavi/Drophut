# React + TypeScript + Vite

This template provides a minimal setup to get React working in Vite with HMR and some ESLint rules.

Currently, two official plugins are available:

- [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react) uses [Oxc](https://oxc.rs)
- [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/)

## React Compiler

The React Compiler is not enabled on this template because of its impact on dev & build performances. To add it, see [this documentation](https://react.dev/learn/react-compiler/installation).

## Expanding the ESLint configuration

If you are developing a production application, we recommend updating the configuration to enable type-aware lint rules:

```js
export default defineConfig([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      // Other configs...

      // Remove tseslint.configs.recommended and replace with this
      tseslint.configs.recommendedTypeChecked,
      // Alternatively, use this for stricter rules
      tseslint.configs.strictTypeChecked,
      // Optionally, add this for stylistic rules
      tseslint.configs.stylisticTypeChecked,

      // Other configs...
    ],
    languageOptions: {
      parserOptions: {
        project: ['./tsconfig.node.json', './tsconfig.app.json'],
        tsconfigRootDir: import.meta.dirname,
      },
      // other options...
    },
  },
])

```

You can also install [eslint-plugin-react-x](https://npmx.dev/package/eslint-plugin-react-x) and [eslint-plugin-react-dom](https://npmx.dev/package/eslint-plugin-react-dom) for React-specific lint rules:

```js
// eslint.config.js
import reactX from 'eslint-plugin-react-x'
import reactDom from 'eslint-plugin-react-dom'

export default defineConfig([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      // Other configs...
      // Enable lint rules for React
      reactX.configs['recommended-typescript'],
      // Enable lint rules for React DOM
      reactDom.configs.recommended,
    ],
    languageOptions: {
      parserOptions: {
        project: ['./tsconfig.node.json', './tsconfig.app.json'],
        tsconfigRootDir: import.meta.dirname,
      },
      // other options...
    },
  },
])

```

### Pipe recovery

Deploy the frontend and signaling server together. Both use private per-participant tokens for registration and resume. The DataChannel protocol adds a `file-checkpoint` response to `file-start`. Chunk and completion acknowledgments still travel over the DataChannel. The signaling server handles session membership and WebRTC setup, and file bytes travel directly between devices.

The signaling server keeps each disconnected participant’s place for 90 seconds by default. Set `PIPE_RECONNECT_MS` on the server to change this window. A signaling process restart clears the in-memory pipes. The receiver saves acknowledged batches in IndexedDB. After a reload, the sender can verify and reselect the interrupted file. Other queued file selections live in the sender tab.

Opening Send Files starts the WebSocket handshake while the user picks files, overlapping file selection with a Render Free cold start. An always-on signaling instance removes the idle cold start. Mobile browsers may suspend tabs. Recovery runs when the tab can execute again.

Run `npm test` for signaling regression tests. With desktop Chrome installed, run `node tests/browser-recovery.mjs` for local WebRTC disconnect, reload, and byte comparison tests. Set `CHROME_BIN` if Chrome is installed under a different executable name.

### Homepage statistics

The signaling process also serves the public homepage totals over HTTP. Use Supabase Postgres on Render Free. Local SQLite development requires Node.js 22.13+.

- `STATS_DB_PATH` is the SQLite file path. Its parent directory must exist and be writable. Use a persistent disk and a single backend instance for this SQLite deployment. An ephemeral filesystem, including Render Free's default filesystem, loses the database on replacement.
- `STATS_VISITORS_SINCE` is the display date in `YYYY-MM-DD` format. It defaults to `2026-11-24`. Set it to the actual measurement start date before enabling the database.

For local development, run `STATS_DB_PATH=./drophut-stats.sqlite node server/server.js` alongside `npm run dev`. The first stats request applies [the migration](server/migrations/001-homepage-stats.sql), including the one-time 1,000-file estimate. Keep the same database file between releases. With the database disabled or unavailable, the homepage omits totals and signaling remains available.

The frontend uses the HTTP equivalent of `VITE_SIGNALING_URL` for stats by default. Set `VITE_STATS_URL` to the backend origin if it is hosted separately. The API returns both counts and the visitor start date. It stores anonymous browser IDs and successful transfer IDs for deduplication. Counts use client reports, so they are estimates of usage rather than audited totals. Transfer reports retry briefly while the receiver tab is running. `npm test` checks the stats and signaling from the terminal.

#### Supabase Postgres on Render

1. In the Supabase **SQL Editor**, paste and run the entire [Postgres migration](server/migrations/postgres/001-homepage-stats.sql). It creates a private `drophut_stats` schema with deduplication constraints and seeds the 1,000 baseline once. The migration can be rerun safely. It starts fresh unless you separately import existing event IDs.
2. In the Supabase project dashboard, open **Connect → Session pooler** and copy the URI on port **5432**. Use the host and username shown for your project. Replace the password placeholder with your database password, percent-encoding any characters that have special meaning in a URI. Use TLS with certificate verification. For example:

   ```text
   postgresql://postgres.<PROJECT_REF>:<URL_ENCODED_DATABASE_PASSWORD>@<SESSION_POOLER_HOST>:5432/postgres?sslmode=verify-full
   ```

   The Node backend connects over the Postgres protocol through the Session pooler. The Data API can stay disabled. [Supabase connection guide](https://supabase.com/docs/guides/database/connecting-to-postgres)
3. In Render, open the **Drophut signaling web service → Environment**. Set `DATABASE_URL` to that URI, optionally set `STATS_VISITORS_SINCE`, and deploy the backend build containing the Postgres driver. Keep the URI in the backend environment. [Render environment variable guide](https://render.com/docs/configure-environment-variables)

With `DATABASE_URL` set, the backend uses a lazy Postgres pool of up to three connections. A connection or query failure returns HTTP 503 for stats and leaves WebSocket signaling available. SQLite is available when `STATS_DB_PATH` is set and `DATABASE_URL` is absent.

After deployment, run this read-only check. Expect HTTP 200 and JSON containing `filesTransferred`, `uniqueVisitors`, and `visitorsSince`. A newly migrated database with no activity returns 1000 files and 0 visitors.

```sh
curl --fail-with-body --show-error --include https://drophut.onrender.com/api/stats
```

Run `npm test` for the SQLite and Postgres-outage checks. To also exercise live Postgres, set `TEST_POSTGRES_URL` to a fresh, disposable database named `drophut_test` or `drophut_test_<suffix>`. The test writes sample IDs and leaves them in that disposable database. Append `?sslmode=disable` only for a disposable local server running without TLS.
