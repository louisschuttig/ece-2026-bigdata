#!/usr/bin/env bash
# Lab 10: register lab.silver.users version 1 (the 50 users of the Spark lab) in Polaris as silver.users.
# The cleanup of the Polaris lab dropped polaris.silver.users, and the Trino labs read it.
# Registration only adds a pointer in Polaris: no file is copied or written in the bucket.
set -euo pipefail

POLARIS_URI="${POLARIS_URI:-http://polaris:8181}"
CATALOG="${POLARIS_CATALOG:-$KUBERNETES_NAMESPACE}"
REST="$POLARIS_URI/api/catalog/v1/$CATALOG"
METADATA="s3a://$CATALOG/warehouse/silver/users/metadata/v1.metadata.json"
set -a; . /home/onyxia/work/.polaris-credentials.env; set +a

TOKEN=$(curl -fsS "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials \
  -d client_id="${POLARIS_DEV_CREDENTIAL%%:*}" -d client_secret="${POLARIS_DEV_CREDENTIAL#*:}" \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)

status=$(curl -sS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN" "$REST/namespaces/silver/tables/users")
if [ "$status" = "404" ]; then
  curl -fsS -X POST "$REST/namespaces/silver/register" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"name\":\"users\",\"metadata-location\":\"$METADATA\"}" \
    | jq -c '{"metadata-location": .["metadata-location"], snapshot: .metadata["current-snapshot-id"]}'
else
  echo "silver.users already exists in Polaris (HTTP $status)"
fi
echo "Tables in polaris silver: $(curl -fsS -H "Authorization: Bearer $TOKEN" "$REST/namespaces/silver/tables" | jq -r '[.identifiers[].name] | join(", ")')"