#!/bin/sh
set -e

# Write runtime config from ACI secure environment variables into a JS file
# that the browser loads before the React bundle (see index.html).
#
# This is the production equivalent of public/config.js used during `npm run dev`.
# Values are injected by ACI at container startup — never baked into the image —
# so the same image can be deployed to any environment by changing the YAML.
cat > /usr/share/nginx/html/config.js <<EOF
window.__config__ = {
  TENANT_ID: "${TENANT_ID}",
  SPA_CLIENT_ID: "${SPA_CLIENT_ID}",
  API_CLIENT_ID: "${API_CLIENT_ID}",
  APIM_BASE_URL: "${APIM_BASE_URL}"
};
EOF

exec nginx -g 'daemon off;'
