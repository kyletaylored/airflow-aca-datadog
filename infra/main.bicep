@description('Azure location for all resources.')
param location string = resourceGroup().location

@description('Name prefix used for resources.')
@minLength(3)
@maxLength(12)
param namePrefix string = 'aflo'

@description('Airflow container image (stock image; DAGs/plugins are delivered via a mounted file share).')
param airflowImage string = 'apache/airflow:2.10.2'

@description('Container image for the sample ACA Job that Airflow drives.')
param sampleJobImage string = 'mcr.microsoft.com/k8se/quickstart-jobs:latest'

@description('Postgres administrator login.')
param postgresAdminUser string = 'airflowadmin'

@description('Postgres Flexible Server SKU. Standard_B1ms is the smallest managed option.')
param postgresSkuName string = 'Standard_B1ms'

@description('Postgres Flexible Server tier matching postgresSkuName (Burstable, GeneralPurpose, MemoryOptimized).')
@allowed([ 'Burstable', 'GeneralPurpose', 'MemoryOptimized' ])
param postgresSkuTier string = 'Burstable'

@description('Region for Postgres only. Defaults to the main location; override when the SKU is not offered there (SkuNotAvailable).')
param postgresLocation string = ''

@description('Availability zone for Postgres ("1", "2", "3"). Empty lets Azure choose; pin one when a zone lacks capacity (SkuNotAvailable).')
@allowed([ '', '1', '2', '3' ])
param postgresZone string = ''

@secure()
@description('Postgres administrator password (set via: azd env set POSTGRES_ADMIN_PASSWORD <value>).')
param postgresAdminPassword string

@description('Initial Airflow web UI admin username.')
param airflowAdminUser string = 'airflow'

@secure()
@description('Initial Airflow web UI admin password.')
param airflowAdminPassword string

@description('Whether to create the Contributor role assignment for the managed identity. Requires the deployer to have Owner or User Access Administrator on the scope. Set to "false" if you lack role-assignment rights (common on locked-down/internal subscriptions) and have an admin grant the role separately using the command in the deployment outputs.')
@allowed([ 'true', 'false' ])
param assignJobsRole string = 'true'

@secure()
@description('Datadog API key. When set, enables (1) the ACA managed OpenTelemetry agent exporting traces to Datadog and (2) OpenLineage events to Datadog Data Jobs Monitoring. Leave empty to deploy without Datadog.')
param datadogApiKey string = ''

@description('Datadog site, e.g. datadoghq.com, us3.datadoghq.com, us5.datadoghq.com, datadoghq.eu, ap1.datadoghq.com.')
param datadogSite string = 'datadoghq.com'

@description('Datadog "env" tag on traces, and the OpenLineage namespace (shown as "env" in Data Jobs Monitoring).')
param datadogEnv string = 'airflow-aca'

var suffix = uniqueString(subscription().subscriptionId, resourceGroup().id)
var workspaceName = '${namePrefix}-law-${suffix}'
var managedEnvName = '${namePrefix}-env-${suffix}'
var identityName = '${namePrefix}-id-${suffix}'
var storageAccountName = toLower('st${take(suffix, 22)}')
var shareName = 'airflow-shared'
var pgServerName = '${namePrefix}-pg-${suffix}'
var pgDatabaseName = 'airflow'
// ACA Job names are capped at 32 chars. Keep this well under the limit even
// with a 12-char namePrefix: 12 (prefix) + 5 ('-job-') + 10 (suffix) = 27.
var sampleJobName = '${namePrefix}-job-${take(suffix, 10)}'

var enableDatadog = !empty(datadogApiKey)

var basePipRequirements = 'azure-identity>=1.16.0 requests>=2.31.0'
// Datadog add-ons, installed at container start like the base requirements.
// - OpenLineage provider 2.8.0 = Datadog's recommended version for Airflow 2.10
//   (the stock image ships 1.11.0; the native "datadog" transport needs >= 2.7.3).
// - OTel packages are pinned to the versions already in apache/airflow:2.10.2
//   (constraints-2.10.2) so pip doesn't upgrade the SDK under Airflow.
// - apache-airflow is pinned so the provider upgrade can never drag Airflow along;
//   common-compat/common-sql are pinned to what pip resolves, to skip backtracking.
var datadogPipRequirements = 'apache-airflow==2.10.2 apache-airflow-providers-openlineage==2.8.0 apache-airflow-providers-common-compat==1.9.0 apache-airflow-providers-common-sql==1.29.0 opentelemetry-api==1.27.0 opentelemetry-sdk==1.27.0 opentelemetry-exporter-otlp-proto-grpc==1.27.0 opentelemetry-distro==0.48b0 opentelemetry-instrumentation-requests==0.48b0 opentelemetry-instrumentation-flask==0.48b0'
var pipRequirements = enableDatadog ? '${basePipRequirements} ${datadogPipRequirements}' : basePipRequirements
// Run each Airflow component under OTel auto-instrumentation. It reads the
// OTEL_EXPORTER_OTLP_ENDPOINT/PROTOCOL (gRPC) that ACA injects for the managed agent.
var otelWrap = enableDatadog ? 'opentelemetry-instrument ' : ''
// The webserver is the exception: `airflow webserver` launches gunicorn as a
// *subprocess*, and opentelemetry-instrument deliberately strips itself from
// PYTHONPATH for subprocesses, so the Flask app would go uninstrumented. With
// Datadog on, run gunicorn directly with the same arguments `airflow webserver` uses.
var webServeCmd = enableDatadog
  ? 'exec opentelemetry-instrument gunicorn --workers 4 --worker-class sync --timeout 120 --bind 0.0.0.0:8080 --name airflow-webserver --config python:airflow.www.gunicorn_config --access-logfile - --preload "airflow.www.app:cached_app()"'
  : 'exec airflow webserver'
var sqlAlchemyConn = 'postgresql+psycopg2://${postgresAdminUser}:${postgresAdminPassword}@${postgres.properties.fullyQualifiedDomainName}:5432/${pgDatabaseName}?sslmode=require'

// --- Observability -----------------------------------------------------------

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: workspaceName
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

// --- Identity Airflow uses to call the ARM Jobs API --------------------------

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
}

// Let the identity start/read ACA Job executions in this resource group.
// Contributor is broad; scope to a custom role for least privilege in production.
// Reference the built-in role by its well-known ID via a lookup so the GUID
// can't be mistyped (Contributor = b24988ac-6180-42a0-ab88-20f7382dd24c).
resource contributorRoleDef 'Microsoft.Authorization/roleDefinitions@2022-04-01' existing = {
  scope: subscription()
  name: 'b24988ac-6180-42a0-ab88-20f7382dd24c'
}
// Conditional: creating a role assignment needs Owner or User Access Administrator.
// If the deployer lacks that (common on internal/locked-down subscriptions), set
// assignJobsRole=false — the stack still deploys and an admin grants the role
// afterward with the command surfaced in the deployment outputs.
resource jobsRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (assignJobsRole == 'true') {
  name: guid(resourceGroup().id, identity.id, contributorRoleDef.id)
  properties: {
    roleDefinitionId: contributorRoleDef.id
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// --- Shared storage for DAGs + plugins --------------------------------------

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource fileService 'Microsoft.Storage/storageAccounts/fileServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource fileShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-05-01' = {
  parent: fileService
  name: shareName
  properties: {
    accessTier: 'TransactionOptimized'
    enabledProtocols: 'SMB'
  }
}

// --- Postgres metadata database ---------------------------------------------

resource postgres 'Microsoft.DBforPostgreSQL/flexibleServers@2023-06-01-preview' = {
  name: pgServerName
  location: empty(postgresLocation) ? location : postgresLocation
  sku: {
    name: postgresSkuName
    tier: postgresSkuTier
  }
  properties: {
    version: '16'
    availabilityZone: empty(postgresZone) ? null : postgresZone
    administratorLogin: postgresAdminUser
    administratorLoginPassword: postgresAdminPassword
    storage: { storageSizeGB: 32 }
    backup: { backupRetentionDays: 7 }
    highAvailability: { mode: 'Disabled' }
  }
}

resource pgDatabase 'Microsoft.DBforPostgreSQL/flexibleServers/databases@2023-06-01-preview' = {
  parent: postgres
  name: pgDatabaseName
}

// Allow other Azure services (the ACA apps) to reach the server.
resource pgFirewall 'Microsoft.DBforPostgreSQL/flexibleServers/firewallRules@2023-06-01-preview' = {
  parent: postgres
  name: 'AllowAllAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// --- Container Apps environment + mounted share ------------------------------

// 2024-10-02-preview is the API version Microsoft documents for the managed
// OpenTelemetry agent (openTelemetryConfiguration).
resource managedEnvironment 'Microsoft.App/managedEnvironments@2024-10-02-preview' = {
  name: managedEnvName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
    workloadProfiles: [
      {
        name: 'Consumption'
        workloadProfileType: 'Consumption'
      }
    ]
    // Managed OTel agent: apps get OTEL_EXPORTER_OTLP_ENDPOINT (gRPC) injected and
    // the agent forwards traces to Datadog. The agent has no Key Vault reference
    // support, so the key is passed as a secure parameter.
    openTelemetryConfiguration: enableDatadog ? {
      destinationsConfiguration: {
        dataDogConfiguration: {
          site: datadogSite
          key: datadogApiKey
        }
      }
      tracesConfiguration: {
        destinations: [ 'dataDog' ]
      }
    } : null
  }
}

resource envStorage 'Microsoft.App/managedEnvironments/storages@2024-03-01' = {
  parent: managedEnvironment
  name: 'airflow-shared'
  properties: {
    azureFile: {
      accountName: storage.name
      accountKey: storage.listKeys().keys[0].value
      shareName: shareName
      accessMode: 'ReadWrite'
    }
  }
}

// Environment shared by every Airflow role (web/scheduler/triggerer).
var airflowEnv = [
  { name: 'AIRFLOW__CORE__EXECUTOR', value: 'LocalExecutor' }
  { name: 'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', secretRef: 'sql-alchemy-conn' }
  { name: 'AIRFLOW__CORE__LOAD_EXAMPLES', value: 'false' }
  { name: 'AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION', value: 'false' }
  { name: 'AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE', value: 'true' }
  { name: 'AIRFLOW__CORE__DAGS_FOLDER', value: '/opt/airflow/shared/dags' }
  { name: 'AIRFLOW__CORE__PLUGINS_FOLDER', value: '/opt/airflow/shared/plugins' }
  // Accept HTTP basic auth on the stable REST API (in addition to the browser
  // session) so external systems can trigger DAGs with a username/password.
  { name: 'AIRFLOW__API__AUTH_BACKENDS', value: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session' }
  { name: '_PIP_ADDITIONAL_REQUIREMENTS', value: pipRequirements }
  // Point the operator at the sample job (overridable per-DAG at trigger time).
  { name: 'AIRFLOW_VAR_AZURE_SUBSCRIPTION_ID', value: subscription().subscriptionId }
  { name: 'AIRFLOW_VAR_ACA_RESOURCE_GROUP', value: resourceGroup().name }
  { name: 'AIRFLOW_VAR_ACA_JOB_NAME', value: sampleJobName }
  // user-assigned managed identity, so DefaultAzureCredential picks the right one.
  { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
  ...datadogEnvVars
]

var datadogEnvVars = enableDatadog ? [
  // --- OpenLineage -> Datadog Data Jobs Monitoring (native "datadog" transport) ---
  { name: 'OPENLINEAGE__TRANSPORT__TYPE', value: 'datadog' }
  { name: 'DD_API_KEY', secretRef: 'dd-api-key' }
  { name: 'DD_SITE', value: datadogSite }
  { name: 'OPENLINEAGE_NAMESPACE', value: datadogEnv }
  // --- OpenTelemetry SDK -> ACA managed agent -> Datadog (traces only) ---
  { name: 'OTEL_TRACES_EXPORTER', value: 'otlp' }
  { name: 'OTEL_METRICS_EXPORTER', value: 'none' }
  { name: 'OTEL_LOGS_EXPORTER', value: 'none' }
  // Overrides the attributes ACA injects, so Datadog gets env / unified tagging.
  { name: 'OTEL_RESOURCE_ATTRIBUTES', value: 'deployment.environment=${datadogEnv},deployment.environment.name=${datadogEnv},service.namespace=airflow,cloud.provider=azure,cloud.platform=azure_container_apps' }
  // Health checks would otherwise dominate the webserver's traces.
  { name: 'OTEL_PYTHON_FLASK_EXCLUDED_URLS', value: 'health,static' }
  // One trace per DAG run with a span per task (plugins/otel_dag_run_traces.py).
  { name: 'AIRFLOW_OTEL_DAG_RUN_TRACES', value: 'true' }
] : []

// Per-component OTel service name (only set when Datadog is enabled).
func otelServiceEnv(enabled bool, service string) array => enabled ? [ { name: 'OTEL_SERVICE_NAME', value: service } ] : []

var airflowSecrets = concat(
  [ { name: 'sql-alchemy-conn', value: sqlAlchemyConn } ],
  enableDatadog ? [ { name: 'dd-api-key', value: datadogApiKey } ] : []
)

var sharedVolumes = [
  { name: 'airflow-shared', storageType: 'AzureFile', storageName: 'airflow-shared' }
]
var sharedVolumeMounts = [
  { volumeName: 'airflow-shared', mountPath: '/opt/airflow/shared' }
]

// --- Airflow web (the only externally reachable component) -------------------

resource airflowWeb 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-web'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: { '${identity.id}': {} }
  }
  properties: {
    managedEnvironmentId: managedEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
      }
      secrets: airflowSecrets
    }
    template: {
      containers: [
        {
          name: 'airflow-web'
          image: airflowImage
          // No `command` override: it would replace the image ENTRYPOINT
          // (dumb-init + /entrypoint), which is what installs
          // _PIP_ADDITIONAL_REQUIREMENTS. Passing `bash -c ...` as args lets the
          // entrypoint run first, then exec our script.
          // Migrate the DB and create the admin user on first boot, then serve.
          args: [
            'bash'
            '-c'
            'airflow db migrate && (airflow users create --username "${airflowAdminUser}" --password "${airflowAdminPassword}" --firstname Air --lastname Flow --role Admin --email admin@example.com || true) && ${webServeCmd}'
          ]
          resources: { cpu: json('1.0'), memory: '2Gi' }
          env: concat(airflowEnv, otelServiceEnv(enableDatadog, 'airflow-webserver'))
          volumeMounts: sharedVolumeMounts
        }
      ]
      volumes: sharedVolumes
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
  dependsOn: [ envStorage, pgDatabase, fileShare ]
}

// --- Scheduler (runs tasks with LocalExecutor) ------------------------------

resource airflowScheduler 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-scheduler'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: { '${identity.id}': {} }
  }
  properties: {
    managedEnvironmentId: managedEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: airflowSecrets
    }
    template: {
      containers: [
        {
          name: 'airflow-scheduler'
          image: airflowImage
          args: [ 'bash', '-c', 'exec ${otelWrap}airflow scheduler' ]
          resources: { cpu: json('1.0'), memory: '2Gi' }
          env: concat(airflowEnv, otelServiceEnv(enableDatadog, 'airflow-scheduler'))
          volumeMounts: sharedVolumeMounts
        }
      ]
      volumes: sharedVolumes
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
  dependsOn: [ envStorage, airflowWeb ]
}

// --- Triggerer (powers deferrable operators) --------------------------------

resource airflowTriggerer 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-triggerer'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: { '${identity.id}': {} }
  }
  properties: {
    managedEnvironmentId: managedEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: airflowSecrets
    }
    template: {
      containers: [
        {
          name: 'airflow-triggerer'
          image: airflowImage
          args: [ 'bash', '-c', 'exec ${otelWrap}airflow triggerer' ]
          resources: { cpu: json('0.5'), memory: '1Gi' }
          env: concat(airflowEnv, otelServiceEnv(enableDatadog, 'airflow-triggerer'))
          volumeMounts: sharedVolumeMounts
        }
      ]
      volumes: sharedVolumes
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
  dependsOn: [ envStorage, airflowWeb ]
}

// --- The "muscle": a sample ACA Job Airflow drives --------------------------

resource sampleJob 'Microsoft.App/jobs@2024-03-01' = {
  name: sampleJobName
  location: location
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 1
      replicaTimeout: 1800
    }
    template: {
      containers: [
        {
          name: 'worker'
          image: sampleJobImage
          resources: { cpu: json('0.5'), memory: '1Gi' }
        }
      ]
    }
  }
}

output airflowWebUrl string = 'https://${airflowWeb.properties.configuration.ingress.fqdn}'
output sampleJobName string = sampleJob.name
output managedEnvironmentName string = managedEnvironment.name
output storageAccountName string = storage.name
output fileShareName string = shareName
output airflowIdentityClientId string = identity.properties.clientId
output airflowIdentityPrincipalId string = identity.properties.principalId
output jobsRoleAssigned bool = assignJobsRole == 'true'
output datadogEnabled bool = enableDatadog
// If assignJobsRole=false, an admin (Owner/User Access Administrator) grants the
// role afterward by running this exact command:
output grantRoleCommand string = 'az role assignment create --assignee-object-id ${identity.properties.principalId} --assignee-principal-type ServicePrincipal --role Contributor --scope ${resourceGroup().id}'
