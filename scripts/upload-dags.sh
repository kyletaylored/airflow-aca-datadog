#!/usr/bin/env sh
# Uploads the local DAGs and plugins to the Azure Files share that the Airflow
# Container Apps mount at /opt/airflow/shared. Run automatically by azd as a
# postprovision hook; safe to re-run any time you change DAGs/plugins.
set -eu

# azd exposes bicep outputs to hooks using their original (camelCase) names.
# Fall back to UPPER_SNAKE variants in case a future azd version converts them.
STORAGE_ACCOUNT_NAME="${storageAccountName:-${STORAGE_ACCOUNT_NAME:-}}"
FILE_SHARE_NAME="${fileShareName:-${FILE_SHARE_NAME:-}}"
AIRFLOW_WEB_URL="${airflowWebUrl:-${AIRFLOW_WEB_URL:-}}"

: "${STORAGE_ACCOUNT_NAME:?STORAGE_ACCOUNT_NAME not set (is the deployment finished?)}"
: "${FILE_SHARE_NAME:?FILE_SHARE_NAME not set}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Uploading DAGs and plugins to share '$FILE_SHARE_NAME' on '$STORAGE_ACCOUNT_NAME'..."

KEY="$(az storage account keys list \
  --account-name "$STORAGE_ACCOUNT_NAME" \
  --query '[0].value' -o tsv)"

az storage directory create \
  --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$KEY" \
  --share-name "$FILE_SHARE_NAME" --name dags --only-show-errors 1>/dev/null || true
az storage directory create \
  --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$KEY" \
  --share-name "$FILE_SHARE_NAME" --name plugins --only-show-errors 1>/dev/null || true
# Code for the optional order-stream services (mounted at /opt/contoso).
az storage directory create \
  --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$KEY" \
  --share-name "$FILE_SHARE_NAME" --name services --only-show-errors 1>/dev/null || true

az storage file upload-batch \
  --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$KEY" \
  --destination "$FILE_SHARE_NAME/dags" \
  --source "$ROOT/airflow/dags" \
  --pattern "*.py" --only-show-errors 1>/dev/null

az storage file upload-batch \
  --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$KEY" \
  --destination "$FILE_SHARE_NAME/plugins" \
  --source "$ROOT/airflow/plugins" \
  --pattern "*.py" --only-show-errors 1>/dev/null

az storage file upload-batch \
  --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$KEY" \
  --destination "$FILE_SHARE_NAME/services" \
  --source "$ROOT/services" \
  --pattern "*.py" --only-show-errors 1>/dev/null

echo "Done. Airflow will pick up the DAGs within a minute."
echo "Web UI: ${AIRFLOW_WEB_URL:-see 'azd env get-values'}"
