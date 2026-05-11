import type { Configuration, PopupRequest } from "@azure/msal-browser";

const cfg = window.__config__;

export const msalConfig: Configuration = {
  auth: {
    clientId: cfg.SPA_CLIENT_ID,
    authority: `https://login.microsoftonline.com/${cfg.TENANT_ID}`,
    redirectUri: window.location.origin,
  },
  cache: {
    cacheLocation: "sessionStorage",
    storeAuthStateInCookie: false,
  },
};

export const loginRequest: PopupRequest = {
  scopes: [`api://${cfg.API_CLIENT_ID}/LabResults.Read`],
};

export const APIM_BASE: string = cfg.APIM_BASE_URL;
