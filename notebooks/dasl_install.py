# Databricks notebook source
# MAGIC %pip install databricks-sdk==0.49.0
# MAGIC %pip install dasl-client==1.0.32
# MAGIC %pip install dasl-api==0.1.29
# MAGIC %pip install --upgrade typing_extensions
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog, iam, sql, settings
from databricks.sdk.errors.platform import NotFound

from dasl_client.client import Client
from dasl_client.types import (
    AdminConfig,
    DatasourcesConfig,
    DetectionRuleMetadata,
    SystemTablesConfig,
    WorkspaceConfig,
    WorkspaceConfigObservables
)
import dasl_api

service_principal_name = "dasl-service-principal"

# STEP 1: Insert the Client ID as a string
# Creating an app client ID requires account-level admin permissions. To
# create an ID, navigate to the account management console -> settings ->
# app connections and click "Add Connection". Use the following settings.
# Application Name = "Databricks Security Lakehouse"
# Redirect URLs = ["https://api.sl.us-west-2.cloud.databricks.com/apis/dbui/v1/token-exchange"]
# Access Scopes = All APIs
# Client secret: uncheck "generate a client secret"
# Access token TTL = 60 (suggested)
# Refresh token TTL = 10080 (suggested)
# Save the app connection and populate the resulting client ID below.
app_client_id = "4ec82c77-a93a-48c5-8973-14d8614a2f9d"
if app_client_id is None:
    raise Exception("must specify app_client_id")

# STEP 2: Specify the catalog where the data and metadata will be stored,
# automatically managed by the security lakehouse.
# You may use any name you wish here, and it is
# also acceptable to specify a catalog which already exists.
catalog_name = "sec_lakehouse"

# STEP 3: Specify the managed location where catalogs will be created for 
# the lakehouse. Note that you will need to be a metastore admin for your
# Databricks account; it is not generally sufficient to be a workspace
# admin. If the catalog (catalog_name) above already exists, you may
# leave this set to None.
managed_location = None

# STEP 4: Specify names for Bronze, Silver, Gold schemas
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

# Various details about the current user and workspace. Note in particular
# that the current user's email address will be registered as the admin
# email address for the DASL workspace created below
dbc = WorkspaceClient()
all_users = "account users"
current_user = dbc.current_user.me().user_name
databricks_instance = "fe-sandbox-lakewatch-sandbox-qpliq9.cloud.databricks.com"
workspace = databricks_instance

# Run the rest of the notebook as is to complete the installation.
# The below cells contain configuration code can optionally be customized.
# Reach out to support@antimatter.io for questions or assistance.


# COMMAND ----------

# Create a service principal. This service principal is used to manage
# the catalogs created in this file. It will be granted (along with all
# other account users) ALL_PRIVILEGES on the catalog(s) specified above,
# but no additional privileges will be granted.
sps = list(dbc.service_principals.list(
    filter=f"displayName eq '{service_principal_name}'"
))
if len(sps) > 1:
    raise ValueError(f"multiple service principals found with name {service_principal_name}. Please pick a different name")
elif len(sps) == 1:
    sp = sps[0]
else:
    sp = dbc.service_principals.create(display_name=service_principal_name)

service_principal_application_id = sp.application_id
service_principal_id = sp.id

# Create an oauth secret for the service principal. This token will be sent
# to the ASL control plane as a persistent means of authenticating with the
# Databricks API when creating jobs.
# Start by deleting the oldest existing secret (if necessary) for this
# service principal since no more than 5 secrets are allowed.
service_principal_secrets = dbc.api_client.do(
    "GET",
    path=f"/api/2.0/accounts/servicePrincipals/{service_principal_id}/credentials/secrets",
)

# Only delete a secret if there are already 5
if "secrets" in service_principal_secrets and len(service_principal_secrets["secrets"]) >= 5:
    # Find the element with the oldest create_time
    oldest = min(
        service_principal_secrets["secrets"],
        key=lambda secret: secret["create_time"]
    )

    dbc.api_client.do(
        "DELETE",
        path=f"/api/2.0/accounts/servicePrincipals/{service_principal_id}/credentials/secrets/{oldest['id']}",
    )

service_principal_secret = dbc.api_client.do(
    "POST",
    path=f"/api/2.0/accounts/servicePrincipals/{service_principal_id}/credentials/secrets",
)["secret"]


# COMMAND ----------

# Before continuing we need to ensure that the newly created service principal
# can list users, groups, and service principals. This is required to manage
# user access control. This should not fail as these resources can be listed
# without any additional permissions.

from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient

cfg = Config(
    client_id=service_principal_id,
    client_secret=service_principal_secret,
)

chk = WorkspaceClient(config=cfg)

try:
    list(chk.users.list())
except Exception as e:
    raise Exception(f"Please ensure that the service principal has permissions to list users in this workspace. Error: {e}")
try:
    list(chk.groups.list())
except Exception as e:
    raise Exception(f"Please ensure that the service principal has permissions to list groups in this workspace. Error: {e}")
try:
    list(chk.service_principals.list())
except Exception as e:
    raise Exception(f"Please ensure that the service principal has permissions to list service principals in this workspace. Error: {e}")

# COMMAND ----------

# Now we create the catalog (if required), the admin schema, and do the permission grants

# Create the catalog, if it doesn't already exist
try:
    _ = dbc.catalogs.get(catalog_name)
except NotFound:
    # If the catalog did not already exist, we can create it for you, but we need to know what
    # managed location to use
    if managed_location is not None:
        dbc.catalogs.create(catalog_name, storage_root=managed_location)
    else:
        dbc.catalogs.create(catalog_name)

# Grant ALL_PRIVILEGES on the newly created catalog to the service principal and additionally to
# all workspace users.
# You can refine the all users grant later in the normal Unity Catalog UI, but this is a good default
for principal in [service_principal_application_id, all_users]:
    grants = dbc.grants.get(
        securable_type=catalog.SecurableType.CATALOG,
        full_name=catalog_name,
        principal=principal,
    ).privilege_assignments

    create_grant = True
    for pa in grants:
        if catalog.Privilege.ALL_PRIVILEGES in pa.privileges:
            create_grant = False

    # actually grant permissions if necessary
    if create_grant:
        dbc.grants.update(
            securable_type=catalog.SecurableType.CATALOG,
            full_name=catalog_name,
            changes=[
                catalog.PermissionsChange(
                    add=[catalog.Privilege.ALL_PRIVILEGES],
                    principal=principal,
                ),
            ],
        )

# Create the admin schema, if it doesn't already exist
try:
    _ = dbc.schemas.get(f"{catalog_name}.admin")
except NotFound:
    dbc.schemas.create("admin",catalog_name)

# Grant permission on the admin schema to the service principal if required
grants = dbc.grants.get(
        securable_type=catalog.SecurableType.SCHEMA,
        full_name=f"{catalog_name}.admin",
        principal=service_principal_application_id,
    ).privilege_assignments

create_grant = True
for pa in grants:
    if catalog.Privilege.ALL_PRIVILEGES in pa.privileges:
        create_grant = False

# actually grant permissions if necessary
if create_grant:
    dbc.grants.update(
        securable_type=catalog.SecurableType.SCHEMA,
        full_name=f"{catalog_name}.admin",
        changes=[
            catalog.PermissionsChange(
                add=[catalog.Privilege.ALL_PRIVILEGES],
                principal=service_principal_application_id,
            ),
        ],
    )

# Create a limited view in the admin schema that DASL can use to track
# the usage of DASL jobs.
# This is needed for cost attribution and optimization features
spark.sql(f"""CREATE OR REPLACE VIEW `{catalog_name}`.admin.usage
              AS SELECT record_id, sku_name,
                 usage_start_time, usage_end_time, usage_date,
                 usage_unit, usage_quantity, usage_type, ingestion_date,
                 to_json(usage_metadata) as usage_metadata
              FROM system.billing.usage
              WHERE custom_tags.Source = 'DASL'
              """)

# Create the lakewatch-job-viewers group
groups = list(dbc.groups.list(filter="displayName eq lakewatch-job-viewers"))
if len(groups) < 1:
    # If the group did not already exist, we will create it for you
    dbc.groups.create(display_name="lakewatch-job-viewers")


# COMMAND ----------

# Create a Small SQL warehouse that is used by the DASL UI and for some
# report generation features. The vast majority of data processing is done
# as serverless jobs, but nevertheless, this warehouse is required
dasl_warehouses = [w for w in dbc.warehouses.list() if w.name == "DASL Warehouse"]
if len(dasl_warehouses) == 0:
    dasl_warehouse = dbc.warehouses.create(
        enable_serverless_compute=True,
        auto_stop_mins=10,
        max_num_clusters=1,
        cluster_size="Small",
        name="DASL Warehouse",
        tags=sql.EndpointTags(custom_tags=[sql.EndpointTagPair(key="Source", value="DASL")])
        )
else:
    dasl_warehouse = dasl_warehouses[0]

# Let the service principal and all users use this warehouse
dbc.permissions.set(
    request_object_type='warehouses',
    request_object_id=dasl_warehouse.id,
    access_control_list=[
        iam.AccessControlRequest(
            user_name=service_principal_application_id,
            permission_level=iam.PermissionLevel.CAN_USE
        ),
        iam.AccessControlRequest(
            group_name="users",
            permission_level=iam.PermissionLevel.CAN_USE
        )]
    )




# COMMAND ----------

# Check if the workspace has IP Access Lists in use and add the DASL control plane
try:
    conf_value = dbc.workspace_conf.get_status(keys=["enableIpAccessLists"]).get("enableIpAccessLists", "false")
    ip_access_list_enabled = (False if conf_value is None or conf_value.lower() != "true" else True)
    allow_lists = [l for l in dbc.ip_access_lists.list() if l.enabled==True
                   and l.list_type == settings.ListType.ALLOW
                   and l.label != "DASL"]
    # Only add the DASL allow list if the workspace has IP access lists enabled and there is at least one
    # existing enabled allow list
    if ip_access_list_enabled and len(allow_lists) > 0:
        print ("Workspace appears to use IP allow lists. Adding DASL to a new allow list")
        # create the allow list if it doesn't already exist
        if len([l for l in dbc.ip_access_lists.list() if l.label == "DASL"]) == 0:
            dbc.ip_access_lists.create(
                label='DASL',
                ip_addresses=[
                    "44.232.111.4/32",
                    "54.185.9.202/32",
                    "35.167.155.192/32"
                ],
                list_type=settings.ListType.ALLOW)
    else:
        print ("Workspace does not appear to use IP allow lists")
except NotFound as e:
    print("This workspace does not have IP access lists, nothing to be done")



# COMMAND ----------

# The Databricks setup is now complete. Below, we create and set up the security lakehouse
# workspace by making API calls to the control plane.

am_workspace = Client.new_or_existing(
    current_user,
    app_client_id,
    service_principal_application_id,
    service_principal_secret,
    workspace_url=f"https://{databricks_instance}",
    dasl_host="https://api.sl.us-west-2.cloud.databricks.com",
)

# This is a good initial config for DASL, but everything here can be
# customized in the UI or through the API later
am_workspace.put_config(
    WorkspaceConfig(
        dasl_storage_path=f"/Volumes/{catalog_name}/internal/common",
        datasources=DatasourcesConfig(
            catalog_name=catalog_name,
            bronze_schema=bronze_schema,
            silver_schema=silver_schema,
            gold_schema=gold_schema,
        ),
        default_sql_warehouse="DASL Warehouse",
        job_viewers_group="lakewatch-job-viewers",
        detection_rule_metadata=DetectionRuleMetadata(
            detection_categories=[
                "APT",
                "Malware",
                "Policy",
                "SpecialEvent",
                "SuspectEvent",
                "Target",
                "Trend",
            ],
        ),
        observables=WorkspaceConfigObservables(
            kinds=[
                WorkspaceConfigObservables.ObservablesKinds(
                    name="Email Address",
                    sql_type="STRING",
                ),
                WorkspaceConfigObservables.ObservablesKinds(
                    name="IP Address",
                    sql_type="STRING",
                ),
                WorkspaceConfigObservables.ObservablesKinds(
                    name="Domain Name",
                    sql_type="STRING",
                ),
            ],
            relationships=[
                "ActingUser",
                "DstIP",
                "SrcIP",
                "TargetUser",
            ],
        ),
        system_tables_config=SystemTablesConfig(
            catalog_name=catalog_name,
            var_schema="system",
        ),
    )
)


# COMMAND ----------

# The DASL installation is now complete. A job will be created in your environment, called
# 'dasl-background-tasks', that will create all the OCSF gold tables. This job takes
# approximately 5 minutes to complete. After that has finished, you can navigate to
# https://ui.sl.us-west-2.cloud.databricks.com, paste in a workspace URL, and use the application.
