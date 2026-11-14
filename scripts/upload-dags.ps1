#!/usr/bin/env pwsh
# Uploads the local DAGs and plugins to the Azure Files share that the Airflow
# Container Apps mount at /opt/airflow/shared. Run automatically by azd as a
# postprovision hook; safe to re-run any time you change DAGs/plugins.
$ErrorActionPreference = 'Stop'

# azd exposes bicep outputs to hooks using their original (camelCase) names.
# Fall back to UPPER_SNAKE variants in case a future azd version converts them.
$account = if ($env:storageAccountName) { $env:storageAccountName } else { $env:STORAGE_ACCOUNT_NAME }
$share   = if ($env:fileShareName)      { $env:fileShareName }      else { $env:FILE_SHARE_NAME }
$webUrl  = if ($env:airflowWebUrl)      { $env:airflowWebUrl }      else { $env:AIRFLOW_WEB_URL }

if (-not $account) { throw 'storageAccountName not set (is the deployment finished?)' }
if (-not $share) { throw 'fileShareName not set' }

$root = Split-Path -Parent $PSScriptRoot

Write-Host "Uploading DAGs and plugins to share '$share' on '$account'..."

$key = az storage account keys list --account-name $account --query '[0].value' -o tsv

az storage directory create --account-name $account --account-key $key --share-name $share --name dags --only-show-errors | Out-Null
az storage directory create --account-name $account --account-key $key --share-name $share --name plugins --only-show-errors | Out-Null
# Code for the optional order-stream services (mounted at /opt/contoso).
az storage directory create --account-name $account --account-key $key --share-name $share --name services --only-show-errors | Out-Null

az storage file upload-batch --account-name $account --account-key $key `
  --destination "$share/dags" --source "$root/airflow/dags" --pattern "*.py" --only-show-errors | Out-Null

az storage file upload-batch --account-name $account --account-key $key `
  --destination "$share/plugins" --source "$root/airflow/plugins" --pattern "*.py" --only-show-errors | Out-Null

az storage file upload-batch --account-name $account --account-key $key `
  --destination "$share/services" --source "$root/services" --pattern "*.py" --only-show-errors | Out-Null

Write-Host "Done. Airflow will pick up the DAGs within a minute."
if ($webUrl) { Write-Host "Web UI: $webUrl" }
