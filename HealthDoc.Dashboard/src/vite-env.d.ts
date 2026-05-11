/// <reference types="vite/client" />

interface Window {
  __config__: {
    TENANT_ID: string;
    SPA_CLIENT_ID: string;
    API_CLIENT_ID: string;
    APIM_BASE_URL: string;
  };
}
