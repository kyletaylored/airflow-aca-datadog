@description('Azure location for all resources.')
param location string = resourceGroup().location

@description('Name prefix used for resources.')
@minLength(3)
@maxLength(12)
param namePrefix string = 'aflo'

@description('Airflow container image (stock image; DAGs/plugins are delivered via a mounted file share).')
param airflowImage string = 'apache/airflow:3.4.0'

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

@secure()
@description('Shared secret for every Airflow component: [api] secret_key (sessions/CSRF) and [api_auth] jwt_secret (Execution API tokens the scheduler issues to tasks). Must be identical across components (set via: azd env set AIRFLOW_SECRET_KEY "$(openssl rand -hex 32)").')
param airflowSecretKey string

@description('Whether to create the Contributor role assignment for the managed identity. Requires the deployer to have Owner or User Access Administrator on the scope. Set to "false" if you lack role-assignment rights (common on locked-down/internal subscriptions) and have an admin grant the role separately using the command in the deployment outputs.')
@allowed([ 'true', 'false' ])
param assignJobsRole string = 'true'

@description('Extra tags for every resource, as comma-separated key=value pairs (e.g. "team=data,cost-center=123"). Merged on top of the resource group\'s own tags.')
param resourceTags string = ''

@secure()
@description('Datadog API key. When set, enables (1) the ACA managed OpenTelemetry agent exporting traces to Datadog and (2) OpenLineage events to Datadog Data Jobs Monitoring. Leave empty to deploy without Datadog.')
param datadogApiKey string = ''

@description('Datadog site, e.g. datadoghq.com, us3.datadoghq.com, us5.datadoghq.com, datadoghq.eu, ap1.datadoghq.com.')
param datadogSite string = 'datadoghq.com'

@description('Datadog "env" tag on traces, and the OpenLineage namespace (shown as "env" in Data Jobs Monitoring).')
param datadogEnv string = 'airflow-aca'

@description('Schedule for the bundled demo DAGs (cron or preset like @hourly). Empty = manual trigger only.')
param demoDagSchedule string = '*/5 * * * *'

@description('Comma-separated OTel signals sent to Datadog via the ACA managed agent: any of traces, metrics, logs.')
param datadogOtelSignals string = 'traces,metrics,logs'

@description('Deploy the optional demo data pipeline: a contoso_dw database on the Postgres server, the contoso_orders DAGs, and a Datadog Agent that runs Data Observability checks on it. Needs DD_API_KEY and datadogPostgresPassword.')
@allowed([ 'true', 'false' ])
param enableDataPipeline string = 'false'

@secure()
@description('Password for the read-only "datadog" Postgres role the Agent uses (set via: azd env set DD_POSTGRES_PASSWORD "$(openssl rand -hex 24)"). Required when enableDataPipeline is true.')
param datadogPostgresPassword string = ''

@description('Datadog Agent image for the data pipeline. Data Observability for PostgreSQL needs 7.81.0 or later.')
param datadogAgentImage string = 'datadog/agent:7.85.4'

@description('Add the order stream to the data pipeline: Event Hubs (Kafka) between a storefront and an order processor that lands batches in ADLS for contoso_orders to load. Needs enableDataPipeline. Adds about $40-50 a month (Event Hubs Standard and two small apps).')
@allowed([ 'true', 'false' ])
param enableOrderStream string = 'false'

@description('Orders per minute the storefront produces.')
param storefrontOrdersPerMinute string = '20'

var suffix = uniqueString(subscription().subscriptionId, resourceGroup().id)
// Resource tags = the resource group's tags (so ARM updates never strip tags a
// tag-inheritance policy added) + any RESOURCE_TAGS pairs.
var extraTags = toObject(
  filter(split(resourceTags, ','), pair => contains(pair, '=')),
  pair => trim(split(pair, '=')[0]),
  pair => trim(substring(pair, indexOf(pair, '=') + 1))
)
var tags = union(resourceGroup().tags ?? {}, extraTags)
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
var otelSignals = map(split(toLower(datadogOtelSignals), ','), sig => trim(sig))
var otelMetrics = enableDatadog && contains(otelSignals, 'metrics')
var otelLogs = enableDatadog && contains(otelSignals, 'logs')

var sqlAlchemyConn = 'postgresql+psycopg2://${postgresAdminUser}:${postgresAdminPassword}@${postgres.properties.fullyQualifiedDomainName}:5432/${pgDatabaseName}?sslmode=require'

// Optional demo data pipeline (docs/data-pipeline-demo.md): off unless asked for,
// and only with Datadog on and a password for the Agent's Postgres role.
var enablePipeline = enableDatadog && enableDataPipeline == 'true' && !empty(datadogPostgresPassword)
var dwDatabaseName = 'contoso_dw'
var ddAgentName = '${namePrefix}-dd-agent'
// Airflow connection "contoso_dw". The host must stay the server FQDN: OpenLineage
// names the tables postgres://<host>:5432, and the Agent's check uses the same host,
// so Data Jobs Monitoring and the Data Observability catalog see one set of tables.
// Order stream (phase 2 of the pipeline): storefront -> Event Hubs -> processor -> ADLS.
var enableStream = enablePipeline && enableOrderStream == 'true'
var eventHubsName = '${namePrefix}-eh-${suffix}'
var ordersTopic = 'orders'
var kafkaBootstrap = '${eventHubsName}.servicebus.windows.net:9093'
// A small ADLS Gen2 account (hierarchical namespace), so abfss:// names in lineage
// describe it accurately. The Airflow file share's account stays as it is.
var landingAccountName = toLower('stdl${take(suffix, 20)}')
var landingContainerName = 'landing'
// Packages the two services install at startup on python:3.12-slim.
var streamPackages = 'ddtrace==4.16.1 confluent-kafka==2.16.1 azure-storage-blob==12.31.1'
var dwConn = 'postgresql://${postgresAdminUser}:${postgresAdminPassword}@${postgres.properties.fullyQualifiedDomainName}:5432/${dwDatabaseName}?sslmode=require'

// --- Observability -----------------------------------------------------------

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: workspaceName
  location: location
  tags: tags
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

// --- Identity Airflow uses to call the ARM Jobs API --------------------------

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
  tags: tags
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
  tags: tags
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
  tags: tags
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

// The demo warehouse, on the same server but separate from Airflow's metadata DB.
resource pgDwDatabase 'Microsoft.DBforPostgreSQL/flexibleServers/databases@2023-06-01-preview' = if (enablePipeline) {
  parent: postgres
  name: dwDatabaseName
  dependsOn: [ pgDatabase ]
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
  tags: tags
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
      metricsConfiguration: otelMetrics ? { destinations: [ 'dataDog' ] } : null
      logsConfiguration: otelLogs ? { destinations: [ 'dataDog' ] } : null
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
  // FAB auth manager (bundled in the image): username/password users created
  // with `airflow users create`; REST API clients get a JWT from /auth/token.
  { name: 'AIRFLOW__CORE__AUTH_MANAGER', value: 'airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager' }
  // Must match on every component: sessions/CSRF and Execution API JWTs.
  { name: 'AIRFLOW__API__SECRET_KEY', secretRef: 'airflow-secret-key' }
  { name: 'AIRFLOW__API_AUTH__JWT_SECRET', secretRef: 'airflow-secret-key' }
  // Point the operator at the sample job (overridable per-DAG at trigger time).
  { name: 'AIRFLOW_VAR_AZURE_SUBSCRIPTION_ID', value: subscription().subscriptionId }
  { name: 'AIRFLOW_VAR_ACA_RESOURCE_GROUP', value: resourceGroup().name }
  { name: 'AIRFLOW_VAR_ACA_JOB_NAME', value: sampleJobName }
  // user-assigned managed identity, so DefaultAzureCredential picks the right one.
  { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
  // Read by the bundled DAGs; empty = manual-only.
  { name: 'AIRFLOW_DEMO_SCHEDULE', value: demoDagSchedule }
  ...datadogEnvVars
  ...pipelineEnvVars
]

// contoso_dw_bootstrap and contoso_orders exist only when these are set.
var pipelineEnvVars = concat(enablePipeline ? [
  { name: 'AIRFLOW_CONN_CONTOSO_DW', secretRef: 'contoso-dw-conn' }
  { name: 'AIRFLOW_VAR_DATADOG_POSTGRES_PASSWORD', secretRef: 'dd-postgres-password' }
] : [], enableStream ? [
  // contoso_orders loads the processor's batches from here instead of generating orders.
  { name: 'CONTOSO_LANDING_ACCOUNT', value: landingAccountName }
  { name: 'CONTOSO_LANDING_CONTAINER', value: landingContainerName }
  { name: 'CONTOSO_LANDING_CONNECTION_STRING', secretRef: 'landing-conn' }
] : [])

var datadogEnvVars = enableDatadog ? [
  // --- OpenLineage -> Datadog Data Jobs Monitoring (native "datadog" transport;
  //     the image's openlineage provider is new enough, no install needed) ---
  { name: 'OPENLINEAGE__TRANSPORT__TYPE', value: 'datadog' }
  { name: 'DD_API_KEY', secretRef: 'dd-api-key' }
  { name: 'DD_SITE', value: datadogSite }
  { name: 'OPENLINEAGE_NAMESPACE', value: datadogEnv }
  // --- Airflow-native OpenTelemetry -> ACA managed agent -> Datadog ---
  // Airflow 3 reads the standard OTEL_EXPORTER_OTLP_ENDPOINT/PROTOCOL that ACA
  // injects (gRPC), so turning tracing on is all it takes.
  { name: 'AIRFLOW__TRACES__OTEL_ON', value: 'True' }
  // Don't set OTEL_EXPORTER_OTLP_* here: ACA injects the agent's endpoint and
  // protocol (gRPC) only when the app defines none of them; setting
  // OTEL_EXPORTER_OTLP_PROTOCOL alone left every app without an endpoint.
  { name: 'OTEL_TRACES_EXPORTER', value: 'otlp' }
  { name: 'OTEL_METRICS_EXPORTER', value: otelMetrics ? 'otlp' : 'none' }
  { name: 'OTEL_LOGS_EXPORTER', value: otelLogs ? 'otlp' : 'none' }
  ...(otelMetrics ? [ { name: 'AIRFLOW__METRICS__OTEL_ON', value: 'True' } ] : [])
  // Airflow has no native OTel logs; plugins/otel_logging_config.py adds an OTLP
  // log handler to Airflow's logging config (records carry trace/span ids).
  ...(otelLogs ? [ { name: 'AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS', value: 'otel_logging_config.LOGGING_CONFIG' } ] : [])
] : []

// Per-component env: OTel service name (when Datadog is on) and, for every
// component except the api-server itself, where the Execution API lives (tasks
// started by the scheduler, triggers and DAG parsing all report through it).
// OTEL_RESOURCE_ATTRIBUTES per Datadog's ACA managed-agent guide: cloud.provider,
// cloud.platform and the app's cloud.resource_id are required, cloud.region is
// recommended; version comes
// from service.version. The managed agent's pipeline maps Datadog `env` only
// from the older deployment.environment key, so set it alongside .name.
func otelServiceEnv(enabled bool, service string, resourceId string, region string, ddEnv string, version string) array => enabled ? [
  { name: 'OTEL_SERVICE_NAME', value: service }
  { name: 'OTEL_RESOURCE_ATTRIBUTES', value: 'cloud.provider=azure,cloud.platform=azure.container_apps,cloud.resource_id=${resourceId},cloud.region=${region},deployment.environment=${ddEnv},deployment.environment.name=${ddEnv},service.version=${version},service.namespace=airflow' }
] : []
// Components that make ARM calls (the scheduler runs the operator's tasks, the
// triggerer polls) get `requests` instrumentation, so each call is a span under
// the task's worker.*/trigger.* span. Installed at startup by the image
// entrypoint; pinned to the image's OTel SDK (1.45.2 <-> 0.66b2).
var httpTracingEnv = enableDatadog ? [
  { name: '_PIP_ADDITIONAL_REQUIREMENTS', value: 'opentelemetry-instrumentation-requests==0.66b2' }
] : []
// The sample ACA Job's own env: what a job needs to send OpenLineage events
// straight to Datadog (see airflow/dags/jobs/contoso_store_job.py).
var jobLineageEnv = enableDatadog ? [
  { name: 'DD_API_KEY', secretRef: 'dd-api-key' }
  { name: 'DD_SITE', value: datadogSite }
  { name: 'OPENLINEAGE_NAMESPACE', value: datadogEnv }
] : []
var airflowVersion = last(split(airflowImage, ':'))
var appIdPrefix = '${resourceGroup().id}/providers/Microsoft.App/containerApps'
var executionApiEnv = [
  { name: 'AIRFLOW__CORE__EXECUTION_API_SERVER_URL', value: 'https://${airflowWeb.properties.configuration.ingress.fqdn}/execution/' }
]

var airflowSecrets = concat(
  [ { name: 'sql-alchemy-conn', value: sqlAlchemyConn } ],
  [ { name: 'airflow-secret-key', value: airflowSecretKey } ],
  enableDatadog ? [ { name: 'dd-api-key', value: datadogApiKey } ] : [],
  enablePipeline ? [
    { name: 'contoso-dw-conn', value: dwConn }
    { name: 'dd-postgres-password', value: datadogPostgresPassword }
  ] : [],
  enableStream ? [ { name: 'landing-conn', value: landingConn } ] : []
)

var sharedVolumes = [
  { name: 'airflow-shared', storageType: 'AzureFile', storageName: 'airflow-shared' }
]
var sharedVolumeMounts = [
  { volumeName: 'airflow-shared', mountPath: '/opt/airflow/shared' }
]

// --- Airflow API server: UI + REST API + Execution API (the only external app) --

resource airflowWeb 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-web'
  location: location
  tags: tags
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
          // (dumb-init + /entrypoint, which waits for the DB and sets up the
          // environment). Passing `bash -c ...` as args runs it first.
          // Migrate the DB (core + FAB tables), create the admin user on first boot, serve.
          args: [
            'bash'
            '-c'
            'airflow db migrate && (airflow fab-db migrate || true) && (airflow users create --username "${airflowAdminUser}" --password "${airflowAdminPassword}" --firstname Air --lastname Flow --role Admin --email admin@example.com || true) && exec airflow api-server --host 0.0.0.0 --port 8080'
          ]
          resources: { cpu: json('1.0'), memory: '2Gi' }
          env: concat(airflowEnv, otelServiceEnv(enableDatadog, 'airflow-aca-api-server', '${appIdPrefix}/${namePrefix}-web', location, datadogEnv, airflowVersion))
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
  tags: tags
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
          args: [ 'bash', '-c', 'exec airflow scheduler' ]
          resources: { cpu: json('1.0'), memory: '2Gi' }
          env: concat(airflowEnv, executionApiEnv, otelServiceEnv(enableDatadog, 'airflow-aca-scheduler', '${appIdPrefix}/${namePrefix}-scheduler', location, datadogEnv, airflowVersion), httpTracingEnv)
          volumeMounts: sharedVolumeMounts
        }
      ]
      volumes: sharedVolumes
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
  dependsOn: [ envStorage ]
}

// --- Triggerer (powers deferrable operators) --------------------------------

resource airflowTriggerer 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-triggerer'
  location: location
  tags: tags
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
          args: [ 'bash', '-c', 'exec airflow triggerer' ]
          resources: { cpu: json('0.5'), memory: '1Gi' }
          env: concat(airflowEnv, executionApiEnv, otelServiceEnv(enableDatadog, 'airflow-aca-triggerer', '${appIdPrefix}/${namePrefix}-triggerer', location, datadogEnv, airflowVersion), httpTracingEnv)
          volumeMounts: sharedVolumeMounts
        }
      ]
      volumes: sharedVolumes
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
  dependsOn: [ envStorage ]
}

// --- DAG processor (Airflow 3 parses DAG files outside the scheduler) --------

resource airflowDagProcessor 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-dag-processor'
  location: location
  tags: tags
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
          name: 'airflow-dag-processor'
          image: airflowImage
          args: [ 'bash', '-c', 'exec airflow dag-processor' ]
          resources: { cpu: json('0.5'), memory: '1Gi' }
          env: concat(airflowEnv, executionApiEnv, otelServiceEnv(enableDatadog, 'airflow-aca-dag-processor', '${appIdPrefix}/${namePrefix}-dag-processor', location, datadogEnv, airflowVersion))
          volumeMounts: sharedVolumeMounts
        }
      ]
      volumes: sharedVolumes
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
  dependsOn: [ envStorage ]
}

// --- The "muscle": a sample ACA Job Airflow drives --------------------------

resource sampleJob 'Microsoft.App/jobs@2024-03-01' = {
  name: sampleJobName
  location: location
  tags: tags
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 1
      replicaTimeout: 1800
      secrets: enableDatadog ? [ { name: 'dd-api-key', value: datadogApiKey } ] : []
    }
    template: {
      containers: [
        {
          name: 'worker'
          image: sampleJobImage
          resources: { cpu: json('0.5'), memory: '1Gi' }
          // Lets a job report its own OpenLineage run to Datadog. The operator keeps
          // these when it overrides env per execution, and adds the parent run.
          env: jobLineageEnv
        }
      ]
    }
  }
}

// --- Datadog Agent for the demo data pipeline (optional) -----------------------
//
// Runs the Postgres check with Data Observability on contoso_dw: table metadata
// for the catalog, plus the data quality monitors, which reach it over Remote
// Configuration. Exactly one replica, so each monitor runs once. Airflow itself
// doesn't use this Agent; it stays on the managed OTel agent. The internal
// ingress on 8126 is the trace intake for services instrumented with dd-trace.

resource ddAgent 'Microsoft.App/containerApps@2024-03-01' = if (enablePipeline) {
  name: ddAgentName
  location: location
  tags: tags
  properties: {
    managedEnvironmentId: managedEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: false
        targetPort: 8126
        transport: 'http'
      }
      secrets: [
        { name: 'dd-api-key', value: datadogApiKey }
        { name: 'dd-postgres-password', value: datadogPostgresPassword }
      ]
    }
    template: {
      containers: [
        {
          name: 'datadog-agent'
          image: datadogAgentImage
          // Write the Postgres check config from env (the password never lands on
          // the file share), then hand over to the image's own entrypoint.
          command: [ '/bin/sh', '-c' ]
          args: [
            '''
set -eu
mkdir -p /etc/datadog-agent/conf.d/postgres.d
cat > /etc/datadog-agent/conf.d/postgres.d/conf.yaml <<EOF
init_config:
instances:
  - host: ${DD_PG_HOST}
    port: 5432
    username: datadog
    password: "${DD_PG_PASSWORD}"
    dbname: ${DD_PG_DBNAME}
    ssl: require
    data_observability:
      enabled: true
    tags:
      - "env:${DD_ENV}"
EOF
exec /bin/entrypoint.sh
'''
          ]
          resources: { cpu: json('0.5'), memory: '1Gi' }
          env: [
            { name: 'DD_API_KEY', secretRef: 'dd-api-key' }
            { name: 'DD_SITE', value: datadogSite }
            { name: 'DD_ENV', value: datadogEnv }
            // No host to detect in a container app; name it after the app.
            { name: 'DD_HOSTNAME', value: ddAgentName }
            { name: 'DD_REMOTE_CONFIGURATION_ENABLED', value: 'true' }
            { name: 'DD_APM_ENABLED', value: 'true' }
            { name: 'DD_APM_NON_LOCAL_TRAFFIC', value: 'true' }
            { name: 'DD_LOGS_ENABLED', value: 'false' }
            { name: 'DD_PROCESS_CONFIG_PROCESS_COLLECTION_ENABLED', value: 'false' }
            { name: 'DD_PG_HOST', value: postgres.properties.fullyQualifiedDomainName }
            { name: 'DD_PG_DBNAME', value: dwDatabaseName }
            { name: 'DD_PG_PASSWORD', secretRef: 'dd-postgres-password' }
          ]
        }
      ]
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
  dependsOn: [ pgDwDatabase ]
}

// --- Order stream for the demo data pipeline (optional) -------------------------
//
// contoso-storefront produces orders to Event Hubs' Kafka endpoint; order-processor
// consumes them and lands newline-delimited JSON batches in ADLS, which
// contoso_orders loads into raw.orders. Both services run dd-trace with Data
// Streams Monitoring (it needs a Datadog SDK; OTel can't produce DSM pathways) and
// send traces to the Agent above. Their code comes from the file share.

resource eventHubs 'Microsoft.EventHub/namespaces@2024-01-01' = if (enableStream) {
  name: eventHubsName
  location: location
  tags: tags
  // Basic has no Kafka endpoint.
  sku: { name: 'Standard', tier: 'Standard', capacity: 1 }
  properties: {
    minimumTlsVersion: '1.2'
  }
}

resource ordersHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = if (enableStream) {
  parent: eventHubs
  name: ordersTopic
  properties: {
    partitionCount: 2
    messageRetentionInDays: 1
  }
}

resource ordersSend 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2024-01-01' = if (enableStream) {
  parent: ordersHub
  name: 'storefront-send'
  properties: { rights: [ 'Send' ] }
}

resource ordersListen 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2024-01-01' = if (enableStream) {
  parent: ordersHub
  name: 'processor-listen'
  properties: { rights: [ 'Listen' ] }
}

resource landingStorage 'Microsoft.Storage/storageAccounts@2023-05-01' = if (enableStream) {
  name: landingAccountName
  location: location
  tags: tags
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
  }
}

resource landingBlobs 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = if (enableStream) {
  parent: landingStorage
  name: 'default'
}

resource landingContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = if (enableStream) {
  parent: landingBlobs
  name: landingContainerName
}

var landingConn = 'DefaultEndpointsProtocol=https;AccountName=${landingAccountName};AccountKey=${landingStorage!.listKeys().keys[0].value};EndpointSuffix=${environment().suffixes.storage}'

// What both services share: dd-trace with DSM, pointed at the Agent's intake.
func streamServiceEnv(service string, ddEnv string, agentUrl string) array => [
  { name: 'PYTHONUNBUFFERED', value: '1' }
  { name: 'PYTHONPATH', value: '/opt/contoso/services' }
  { name: 'DD_SERVICE', value: service }
  { name: 'DD_ENV', value: ddEnv }
  { name: 'DD_TRACE_AGENT_URL', value: agentUrl }
  { name: 'DD_DATA_STREAMS_ENABLED', value: 'true' }
  { name: 'DD_TRACE_REMOVE_INTEGRATION_SERVICE_NAMES_ENABLED', value: 'true' }
  // No DogStatsD here: the Agent is only reachable over HTTP ingress.
  { name: 'DD_RUNTIME_METRICS_ENABLED', value: 'false' }
]

var ddAgentUrl = 'http://${ddAgentName}'
var streamVolumeMounts = [
  { volumeName: 'airflow-shared', mountPath: '/opt/contoso' }
]

resource storefront 'Microsoft.App/containerApps@2024-03-01' = if (enableStream) {
  name: '${namePrefix}-storefront'
  location: location
  tags: tags
  properties: {
    managedEnvironmentId: managedEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: [
        { name: 'kafka-conn', value: ordersSend!.listKeys().primaryConnectionString }
      ]
    }
    template: {
      containers: [
        {
          name: 'storefront'
          image: 'python:3.12-slim'
          command: [ '/bin/sh', '-c' ]
          args: [ 'pip install --no-cache-dir --disable-pip-version-check -q ${streamPackages} && exec ddtrace-run python /opt/contoso/services/storefront.py' ]
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
          env: concat(streamServiceEnv('contoso-storefront', datadogEnv, ddAgentUrl), [
            { name: 'KAFKA_BOOTSTRAP_SERVERS', value: kafkaBootstrap }
            { name: 'KAFKA_CONNECTION_STRING', secretRef: 'kafka-conn' }
            { name: 'KAFKA_TOPIC', value: ordersTopic }
            { name: 'ORDERS_PER_MINUTE', value: storefrontOrdersPerMinute }
          ])
          volumeMounts: streamVolumeMounts
        }
      ]
      volumes: sharedVolumes
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
  dependsOn: [ envStorage, ddAgent ]
}

resource orderProcessor 'Microsoft.App/containerApps@2024-03-01' = if (enableStream) {
  name: '${namePrefix}-order-processor'
  location: location
  tags: tags
  properties: {
    managedEnvironmentId: managedEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: [
        { name: 'kafka-conn', value: ordersListen!.listKeys().primaryConnectionString }
        { name: 'landing-conn', value: landingConn }
        { name: 'dd-api-key', value: datadogApiKey }
      ]
    }
    template: {
      containers: [
        {
          name: 'order-processor'
          image: 'python:3.12-slim'
          command: [ '/bin/sh', '-c' ]
          args: [ 'pip install --no-cache-dir --disable-pip-version-check -q ${streamPackages} && exec ddtrace-run python /opt/contoso/services/order_processor.py' ]
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
          env: concat(streamServiceEnv('order-processor', datadogEnv, ddAgentUrl), [
            { name: 'KAFKA_BOOTSTRAP_SERVERS', value: kafkaBootstrap }
            { name: 'KAFKA_CONNECTION_STRING', secretRef: 'kafka-conn' }
            { name: 'KAFKA_TOPIC', value: ordersTopic }
            { name: 'KAFKA_GROUP_ID', value: 'order-processor' }
            { name: 'LANDING_CONNECTION_STRING', secretRef: 'landing-conn' }
            { name: 'LANDING_ACCOUNT', value: landingAccountName }
            { name: 'LANDING_CONTAINER', value: landingContainerName }
            { name: 'FLUSH_SECONDS', value: '300' }
            // The processor reports each landed batch to Data Observability itself.
            { name: 'DD_API_KEY', secretRef: 'dd-api-key' }
            { name: 'DD_SITE', value: datadogSite }
            { name: 'OPENLINEAGE_NAMESPACE', value: datadogEnv }
          ])
          volumeMounts: streamVolumeMounts
        }
      ]
      volumes: sharedVolumes
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
  dependsOn: [ envStorage, ddAgent, landingContainer ]
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
output dataPipelineEnabled bool = enablePipeline
output orderStreamEnabled bool = enableStream
// If assignJobsRole=false, an admin (Owner/User Access Administrator) grants the
// role afterward by running this exact command:
output grantRoleCommand string = 'az role assignment create --assignee-object-id ${identity.properties.principalId} --assignee-principal-type ServicePrincipal --role Contributor --scope ${resourceGroup().id}'
