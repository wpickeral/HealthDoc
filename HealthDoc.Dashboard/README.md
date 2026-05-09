# HealthDoc Dashboard

Internal React SPA for viewing processed lab results and failed CSV uploads. Authenticates via MSAL (Azure AD) and calls the backend through APIM using a JWT bearer token.

## Project Structure

```
HealthDoc.Dashboard/
├── src/
│   ├── main.tsx                  # Entry point — wraps app in MsalProvider
│   ├── App.tsx                   # Auth gate: shows login page or Dashboard
│   ├── authConfig.ts             # MSAL config, API scope, APIM base URL
│   ├── index.css                 # Global styles
│   │
│   ├── hooks/
│   │   └── useApiToken.ts        # Acquires access token (silent → popup fallback)
│   │
│   └── components/
│       ├── Dashboard.tsx         # Tab shell (Failed Files / Lab Results), shows logged-in user
│       ├── FailedFilesPanel.tsx  # Lists failed CSVs with 1-hour SAS download links
│       └── ResultsPanel.tsx      # Search by Clinic ID, renders processed records table
│
├── .env.example                  # Required env vars — copy to .env and fill in
├── package.json
└── vite.config.ts
```

## Code Orientation

**Entry point flow:** `main.tsx` boots the app and wraps everything in `MsalProvider`, giving every component access to the MSAL instance. `App.tsx` checks `useIsAuthenticated()` — unauthenticated renders the login button, authenticated renders the dashboard. `authConfig.ts` is the single place where MSAL config, the API scope, and the APIM base URL live, all driven by env vars.

**Components:** `FailedFilesPanel` fetches the failed CSV list on mount and renders a table with SAS download links. `ResultsPanel` is user-driven — enter a Clinic ID and hit search to query processed records. Both highlight abnormal results in the table.

**`useApiToken` hook:** Every component that calls the API goes through this hook. It calls `acquireTokenSilent` first — if the token is still cached, no network call is made. It only falls back to `acquireTokenPopup` if the silent call fails (e.g. token expired). This silent-first pattern is the standard MSAL approach for SPAs.

## Local Dev Setup

### Prerequisites

- Node.js 20+
- Azure AD app registrations completed (see [Azure Security section](../README.md#azure-security-az-204--implement-azure-security) in the root README)

### 1. Install dependencies

```bash
npm install
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Then fill in `.env`:

```
VITE_TENANT_ID=<your Azure AD tenant ID>
VITE_SPA_CLIENT_ID=<client ID of the HealthDoc-Dashboard app registration>
VITE_API_CLIENT_ID=<client ID of the HealthDoc-API app registration>
VITE_APIM_BASE_URL=https://apim-healthdoc-dev.azure-api.net/labs
```

All four IDs are found in **Azure Portal → Azure Active Directory → App registrations**.

### 3. Start the dev server

```bash
npm run dev
```

Navigate to `http://localhost:5173`. Click **Sign In** to complete the Azure AD login flow.

> The redirect URI `http://localhost:5173` must be registered on the `HealthDoc-Dashboard` app registration under **Authentication → Single-page application**.

## Auth Flow Summary

1. User clicks Sign In → MSAL opens Azure AD login popup
2. User authenticates → Azure AD issues an access token scoped to `api://<API_CLIENT_ID>/LabResults.Read`
3. Dashboard fetches data → requests include `Authorization: Bearer <token>`
4. APIM validates the JWT via `validate-jwt` policy before forwarding to the Function App
5. Token renewal happens silently in the background via refresh token — user is not re-prompted

## Available Scripts

| Command | Description |
|---|---|
| `npm run dev` | Start dev server with hot module reload at `http://localhost:5173` |
| `npm run build` | Type-check and bundle for production to `dist/` |
| `npm run preview` | Preview the production build locally |
