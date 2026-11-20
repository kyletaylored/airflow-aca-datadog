# Continuous deployment

This is optional. The [README](../README.md#deployment) covers the first deploy with `azd up`. After that, merging to `main` doesn't deploy anything until you turn on [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml). Once it's on, a push to `main` deploys based on what changed:

| Changed paths | What runs |
|---|---|
| `infra/**`, `azure.yaml` | `azd provision`: the Bicep deployment, then the DAG and plugin upload, then a Postgres restart if a server setting is waiting for one |
| `airflow/dags/**`, `airflow/plugins/**`, `scripts/upload-dags.sh` | The DAG and plugin upload only |
| `airflow/plugins/**` (with either of the above) | After the upload, a restart of the DAG processor, scheduler and triggerer |
| `services/**` | The same upload, then a restart of the order-stream services when `ENABLE_ORDER_STREAM` is `true` |

The Airflow apps import the plugins (the ACA Jobs operator and its trigger) once, when they start. Until they restart, they keep the old operator, and a DAG that passes the operator a new argument fails to import. Restarting the scheduler cuts off the tasks running in its container, which Airflow retries if they have retries left; deferred tasks wait in the triggerer and aren't affected. After uploading changed plugins by hand with `scripts/upload-dags.sh`, restart the same three apps (`aflo` is the default `NAME_PREFIX`):

```bash
RG=$(azd env get-value AZURE_RESOURCE_GROUP)
for app in dag-processor scheduler triggerer; do
  rev=$(az containerapp show -g "$RG" -n "aflo-$app" --query properties.latestRevisionName -o tsv)
  az containerapp revision restart -g "$RG" -n "aflo-$app" --revision "$rev"
done
```

The workflow also passes the repository and the merged commit (`GIT_REPOSITORY_URL` and `GIT_COMMIT_SHA`), so Datadog's [Source Code Integration](https://docs.datadoghq.com/source_code/) links each service to its code. The Airflow apps pick up a new commit only on an `infra` deploy, and the order-stream services only when they restart. A DAG upload leaves the running apps on the commit of the last `infra` deploy. To do the same from your own machine, set both before `azd up`:

```bash
azd env set GIT_REPOSITORY_URL "github.com/<owner>/<repo>"
azd env set GIT_COMMIT_SHA     "$(git rev-parse HEAD)"
```

Before anything reaches Azure, the workflow runs the CI checks (lint, tests and the Bicep build) against the merged commit. If they fail, nothing is deployed.

You can also run it by hand from the Actions tab and pick `dags` or `infra`. A manual run never restarts the order-stream services, and always restarts the three Airflow apps, since it has no diff to tell whether the plugins changed.

The workflow signs in to Azure with OpenID Connect, so no Azure credential is stored in GitHub. It reuses the resource group and azd environment from your first `azd up`. One-time setup, run from the checkout that has that azd environment:

1. **Create an identity GitHub can sign in as.** It needs a federated credential for the repo's `azure` GitHub environment and Contributor on the resource group:

   ```bash
   REPO=<owner>/<repo>
   RG=$(azd env get-value AZURE_RESOURCE_GROUP)
   SUB=$(azd env get-value AZURE_SUBSCRIPTION_ID)
   # The subject GitHub puts in the token. Newer repos use an immutable,
   # ID-based form (repo:owner@123/repo@456), so read it rather than guess.
   PREFIX=$(gh api "repos/${REPO}/actions/oidc/customization/sub" -q '.sub_claim_prefix // empty')
   PREFIX=${PREFIX:-repo:${REPO}}

   APP_ID=$(az ad app create --display-name "$REPO-deploy" --query appId -o tsv)
   az ad sp create --id "$APP_ID"
   az ad app federated-credential create --id "$APP_ID" --parameters "{
     \"name\": \"github-azure-environment\",
     \"issuer\": \"https://token.actions.githubusercontent.com\",
     \"subject\": \"${PREFIX}:environment:azure\",
     \"audiences\": [\"api://AzureADTokenExchange\"]
   }"
   az role assignment create --assignee "$APP_ID" --role Contributor \
     --scope "/subscriptions/$SUB/resourceGroups/$RG"
   ```

2. **Create the `azure` GitHub environment** and copy the settings into it. The secrets are piped from the azd environment, so they never appear in your shell history:

   ```bash
   gh api -X PUT "repos/$REPO/environments/azure"

   gh variable set AZURE_CLIENT_ID --env azure --repo "$REPO" --body "$APP_ID"
   gh variable set AZURE_TENANT_ID --env azure --repo "$REPO" --body "$(az account show --query tenantId -o tsv)"
   for name in AZURE_SUBSCRIPTION_ID AZURE_ENV_NAME AZURE_LOCATION AZURE_RESOURCE_GROUP \
               DD_SITE DD_ENV DEMO_DAG_SCHEDULE RESOURCE_TAGS TELEMETRY_SIGNALS NAME_PREFIX \
               POSTGRES_LOCATION POSTGRES_ZONE POSTGRES_SKU_NAME POSTGRES_SKU_TIER \
               POSTGRES_MAX_CONNECTIONS ENABLE_DATA_PIPELINE ENABLE_ORDER_STREAM ENABLE_METABASE; do
     value=$(azd env get-value "$name" 2>/dev/null) && [ -n "$value" ] &&
       gh variable set "$name" --env azure --repo "$REPO" --body "$value"
   done
   for name in DD_API_KEY POSTGRES_ADMIN_PASSWORD AIRFLOW_ADMIN_PASSWORD AIRFLOW_SECRET_KEY \
               DD_POSTGRES_PASSWORD METABASE_DB_PASSWORD; do
     value=$(azd env get-value "$name" 2>/dev/null) && [ -n "$value" ] &&
       printf '%s' "$value" | gh secret set "$name" --env azure --repo "$REPO"
   done
   ```

   These are all the settings the workflow reads. Anything else falls back to its default on every deploy, so a server you scaled up by hand would be scaled back down, and a custom `POSTGRES_ADMIN_USER` or `AIRFLOW_ADMIN_USER` can't be carried over. The last two secrets are needed only with the optional [data pipeline](data-pipeline-demo.md).

3. **Optional: require approval.** In the repo's Settings → Environments → `azure`, add yourself as a required reviewer. Each deploy then waits for you to approve it.

4. **Turn it on:**

   ```bash
   gh variable set AZURE_DEPLOY_ENABLED --repo "$REPO" --body true
   ```

Things to know:

- **The secrets must keep their original values.** A new Postgres password or Airflow secret key would lock Airflow out of its database and sessions. The workflow fails if any of the four required secrets is missing, or if `ENABLE_DATA_PIPELINE` or `ENABLE_METABASE` is `true` without its password, rather than deploying without them.
- **The pipeline doesn't assign roles.** Contributor can't create role assignments, so the workflow deploys with `ASSIGN_JOBS_ROLE=false`. The managed identity's role from your first `azd up` stays in place.
- **GitHub variables can't be empty.** A variable you don't set uses the parameter file's default. Because of that, `DEMO_DAG_SCHEDULE` can't be set to manual-only (an empty string) through the pipeline.
- **Forks and other repos skip the deploy**, because `AZURE_DEPLOY_ENABLED` isn't set there.
