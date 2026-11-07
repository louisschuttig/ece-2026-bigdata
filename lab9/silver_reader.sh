#!/usr/bin/env bash
# Lab 9, exercise 2, as root: a catalog role limited to the silver namespace.
# Creates the catalog role silver_reader, the principal role silver_analyst and the principal reader,
# and appends the credentials of reader to /home/onyxia/work/.polaris-credentials.env.
set -euo pipefail

POLARIS_URI="${POLARIS_URI:-http://polaris:8181}"
POLARIS_CATALOG="${POLARIS_CATALOG:-$KUBERNETES_NAMESPACE}"
API="$POLARIS_URI/api/management/v1"
CREDENTIALS_FILE=/home/onyxia/work/.polaris-credentials.env

TOKEN=$(curl -fsS "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL \
  | jq -r .access_token)

# Print the HTTP status of a management call: 201 created, 409 already exists
call() {
  curl -sS -o /dev/null -w "%{http_code} $1 $2\n" -X "$1" "$API$2" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d "$3"
}

# Catalog role: listing and reading, on the silver namespace only
call POST "/catalogs/$POLARIS_CATALOG/catalog-roles" '{"catalogRole":{"name":"silver_reader","properties":{}}}'
for privilege in NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  call PUT "/catalogs/$POLARIS_CATALOG/catalog-roles/silver_reader/grants" \
    "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$privilege\"}}"
done
# Catalog properties, read by the client when it loads the catalog
call PUT "/catalogs/$POLARIS_CATALOG/catalog-roles/silver_reader/grants" \
  '{"grant":{"type":"catalog","privilege":"CATALOG_READ_PROPERTIES"}}'

# Principal role silver_analyst, granted the catalog role
call POST /principal-roles '{"principalRole":{"name":"silver_analyst","properties":{}}}'
call PUT "/principal-roles/silver_analyst/catalog-roles/$POLARIS_CATALOG" '{"catalogRole":{"name":"silver_reader"}}'

# Principal reader: its credentials are only returned at creation
if ! grep -q '^POLARIS_READER_CREDENTIAL=' "$CREDENTIALS_FILE"; then
  curl -fsS -X POST "$API/principals" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"principal":{"name":"reader","properties":{}},"credentialRotationRequired":false}' \
    | jq -r '"POLARIS_READER_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' \
    >> "$CREDENTIALS_FILE"
  echo "Principal 'reader' created, credentials saved in $CREDENTIALS_FILE"
fi
call PUT /principals/reader/principal-roles '{"principalRole":{"name":"silver_analyst"}}'

echo "Grants of silver_reader:"
curl -fsS "$API/catalogs/$POLARIS_CATALOG/catalog-roles/silver_reader/grants" -H "Authorization: Bearer $TOKEN" | jq -c '.grants[]'
echo "Principal roles of reader: $(curl -fsS "$API/principals/reader/principal-roles" -H "Authorization: Bearer $TOKEN" | jq -r '[.roles[].name] | join(", ")')"