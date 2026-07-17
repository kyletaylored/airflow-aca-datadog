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

var suffix = uniqueString(subscription().subscriptionId, resourceGroup().id)
var workspaceName = '${namePrefix}-law-${suffix}'
var managedEnvName = '${namePrefix}-env-${suffix}'
var identityName = '${namePrefix}-id-${suffix}'
var storageAccountName = toLower('st${take(suffix, 22)}')
var shareName = 'airflow-shared'
var pgServerName = '${namePrefix}-pg-${suffix}'
var pgDatabaseName = 'airflow'
var sampleJobName = '${namePrefix}-sample-job-${suffix}'

var pipRequirements = 'azure-identity>=1.16.0 requests>=2.31.0'
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
  location: location
  sku: {
    name: 'Standard_B1ms'
    tier: 'Burstable'
  }
  properties: {
    version: '16'
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

resource managedEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
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
  { name: '_PIP_ADDITIONAL_REQUIREMENTS', value: pipRequirements }
  // Point the operator at the sample job (overridable per-DAG at trigger time).
  { name: 'AIRFLOW_VAR_AZURE_SUBSCRIPTION_ID', value: subscription().subscriptionId }
  { name: 'AIRFLOW_VAR_ACA_RESOURCE_GROUP', value: resourceGroup().name }
  { name: 'AIRFLOW_VAR_ACA_JOB_NAME', value: sampleJobName }
  // user-assigned managed identity, so DefaultAzureCredential picks the right one.
  { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
]

var airflowSecrets = [
  { name: 'sql-alchemy-conn', value: sqlAlchemyConn }
]

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
          command: [ '/bin/bash', '-c' ]
          // Migrate the DB and create the admin user on first boot, then serve.
          args: [
            'airflow db migrate && (airflow users create --username "${airflowAdminUser}" --password "${airflowAdminPassword}" --firstname Air --lastname Flow --role Admin --email admin@example.com || true) && exec airflow webserver'
          ]
          resources: { cpu: json('1.0'), memory: '2Gi' }
          env: airflowEnv
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
          command: [ '/bin/bash', '-c' ]
          args: [ 'exec airflow scheduler' ]
          resources: { cpu: json('1.0'), memory: '2Gi' }
          env: airflowEnv
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
          command: [ '/bin/bash', '-c' ]
          args: [ 'exec airflow triggerer' ]
          resources: { cpu: json('0.5'), memory: '1Gi' }
          env: airflowEnv
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
// If assignJobsRole=false, an admin (Owner/User Access Administrator) grants the
// role afterward by running this exact command:
output grantRoleCommand string = 'az role assignment create --assignee-object-id ${identity.properties.principalId} --assignee-principal-type ServicePrincipal --role Contributor --scope ${resourceGroup().id}'
