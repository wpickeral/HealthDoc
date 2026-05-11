#!/bin/sh
set -e

cat > /usr/share/nginx/html/config.js <<EOF
window.__config__ = {
  TENANT_ID: "${TENANT_ID}",
  SPA_CLIENT_ID: "${SPA_CLIENT_ID}",
  API_CLIENT_ID: "${API_CLIENT_ID}",
  APIM_BASE_URL: "${APIM_BASE_URL}"
};
EOF

exec nginx -g 'daemon off;'
