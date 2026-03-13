# Databricks notebook source
# MAGIC %md
# MAGIC # Lakewatch Demo Data Generator

# COMMAND ----------

dbutils.widgets.text("catalog", "sec_lakehouse", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Authentication Events

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog}.gold.authentication (dasl_id, activity_name, category_name, class_name, severity, severity_id, status, status_id, disposition, disposition_id, is_mfa, time, type_name, type_uid, message,
  user, actor, src_endpoint, dst_endpoint, metadata)
SELECT
  concat('auth-demo-', lpad(cast(id as string), 6, '0')) as dasl_id,
  'Logon' as activity_name,
  'Identity & Access Management' as category_name,
  'Authentication' as class_name,
  CASE
    WHEN rv < 0.65 THEN 'Informational'
    WHEN rv < 0.80 THEN 'Medium'
    WHEN rv < 0.92 THEN 'High'
    ELSE 'Critical'
  END as severity,
  CASE WHEN rv < 0.65 THEN 1 WHEN rv < 0.80 THEN 3 WHEN rv < 0.92 THEN 4 ELSE 5 END as severity_id,
  CASE WHEN rv < 0.65 THEN 'Success' ELSE 'Failure' END as status,
  CASE WHEN rv < 0.65 THEN 1 ELSE 2 END as status_id,
  CASE WHEN rv < 0.65 THEN 'Allowed' ELSE 'Blocked' END as disposition,
  CASE WHEN rv < 0.65 THEN 1 ELSE 2 END as disposition_id,
  rv < 0.65 as is_mfa,
  current_timestamp() - make_interval(0, 0, 0, abs(hash(id, 42)) % 30, abs(hash(id, 43)) % 24, abs(hash(id, 44)) % 60, abs(hash(id, 45)) % 60) as time,
  'Authentication: Logon' as type_name,
  300201 as type_uid,
  concat(CASE WHEN rv < 0.65 THEN 'Successful' ELSE 'Failed' END, ' authentication for ', user_name, ' from ', src_ip) as message,
  named_struct('has_mfa', rv < 0.65, 'name', user_name, 'type', 'User', 'type_id', 1, 'uid', concat(user_name, '@acme.com')) as user,
  named_struct(
    'app_name', cast(null as string), 'app_uid', cast(null as string),
    'authorizations', cast(null as array<struct<decision:string>>),
    'idp', cast(null as struct<domain:string,name:string,protocol_name:string,tenant_uid:string,uid:string>),
    'process', cast(null as struct<cmd_line:string,cpid:string,name:string,pid:int,session:struct<created_time:timestamp,credential_uid:string,expiration_reason:string,expiration_time:timestamp,is_mfa:boolean,is_remote:boolean,is_vpn:boolean,issuer:string,terminal:string,uid:string,uid_alt:string,uuid:string>,uid:string,user:struct<has_mfa:boolean,name:string,type:string,type_id:int,uid:string>>),
    'user', named_struct('has_mfa', rv < 0.65, 'name', user_name, 'type', 'User', 'type_id', 1, 'uid', concat(user_name, '@acme.com'))
  ) as actor,
  named_struct(
    'domain', cast(null as string), 'hostname', cast(null as string), 'instance_uid', cast(null as string),
    'interface_name', cast(null as string), 'interface_uid', cast(null as string),
    'ip', src_ip, 'name', cast(null as string), 'port', cast(null as int),
    'svc_name', cast(null as string), 'type', cast(null as string), 'type_id', cast(null as int),
    'uid', cast(null as string),
    'location', named_struct('city', city, 'continent', cast(null as string), 'country', country, 'lat', lat, 'long', lon, 'postal_code', cast(null as string)),
    'mac', cast(null as string), 'vpc_uid', cast(null as string), 'zone', cast(null as string)
  ) as src_endpoint,
  named_struct(
    'domain', 'acme.cloud.databricks.com', 'hostname', cast(null as string), 'instance_uid', cast(null as string),
    'interface_name', cast(null as string), 'interface_uid', cast(null as string),
    'ip', '10.0.0.1', 'name', cast(null as string), 'port', 443,
    'svc_name', svc, 'type', cast(null as string), 'type_id', cast(null as int),
    'uid', cast(null as string),
    'location', cast(null as struct<city:string,continent:string,country:string,lat:float,long:float,postal_code:string>),
    'mac', cast(null as string), 'vpc_uid', cast(null as string), 'zone', cast(null as string)
  ) as dst_endpoint,
  named_struct(
    'correlation_uid', cast(null as string), 'event_code', cast(null as string), 'log_level', cast(null as string),
    'log_name', cast(null as string), 'log_provider', cast(null as string), 'log_version', cast(null as string),
    'logged_time', cast(null as timestamp), 'modified_time', cast(null as timestamp),
    'original_time', cast(null as string), 'processed_time', current_timestamp(),
    'product', named_struct('name', 'Databricks', 'vendor_name', 'Databricks', 'version', '2024.1'),
    'tags', cast(null as variant), 'tenant_uid', cast(null as string),
    'uid', concat('meta-auth-', lpad(cast(id as string), 6, '0')), 'version', '1.3.0'
  ) as metadata
FROM (
  SELECT
    id,
    abs(hash(id, 1)) % 100 / 100.0 as rv,
    ELT(1 + abs(hash(id, 10)) % 8, 'alice.johnson','bob.smith','carol.williams','dave.chen','eve.martinez','admin','svc-backup','root') as user_name,
    CASE WHEN abs(hash(id, 1)) % 100 / 100.0 >= 0.65
      THEN ELT(1 + abs(hash(id, 20)) % 5, '185.220.101.34','91.219.237.229','194.26.29.113','45.155.205.233','23.129.64.130')
      ELSE ELT(1 + abs(hash(id, 20)) % 5, '10.0.1.50','10.0.1.51','10.0.2.100','172.16.0.10','192.168.1.25')
    END as src_ip,
    CASE WHEN abs(hash(id, 1)) % 100 / 100.0 >= 0.65
      THEN ELT(1 + abs(hash(id, 30)) % 4, 'Moscow','Beijing','Pyongyang','Tehran')
      ELSE ELT(1 + abs(hash(id, 30)) % 3, 'New York','San Francisco','Chicago')
    END as city,
    CASE WHEN abs(hash(id, 1)) % 100 / 100.0 >= 0.65
      THEN ELT(1 + abs(hash(id, 30)) % 4, 'RU','CN','KP','IR')
      ELSE 'US'
    END as country,
    CASE WHEN abs(hash(id, 1)) % 100 / 100.0 >= 0.65
      THEN cast(ELT(1 + abs(hash(id, 30)) % 4, '55.76','39.90','39.04','35.69') as float)
      ELSE cast(ELT(1 + abs(hash(id, 30)) % 3, '40.71','37.77','41.88') as float)
    END as lat,
    CASE WHEN abs(hash(id, 1)) % 100 / 100.0 >= 0.65
      THEN cast(ELT(1 + abs(hash(id, 30)) % 4, '37.62','116.40','125.76','51.39') as float)
      ELSE cast(ELT(1 + abs(hash(id, 30)) % 3, '-74.01','-122.42','-87.63') as float)
    END as lon,
    ELT(1 + abs(hash(id, 40)) % 4, 'workspace-api','sql-warehouse','cluster-manager','unity-catalog') as svc
  FROM range(5000)
)
""")
print("Wrote 5000 authentication events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. API Activity Events

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog}.gold.api_activity (dasl_id, activity_name, category_name, class_name, severity, severity_id, status, status_id, disposition, disposition_id, time, type_name, type_uid, message,
  api, actor, src_endpoint, cloud, metadata)
SELECT
  concat('api-demo-', lpad(cast(id as string), 6, '0')),
  op_type, 'Application Activity', 'API Activity', severity, severity_id,
  CASE WHEN rv < 0.75 THEN 'Success' ELSE 'Failure' END,
  CASE WHEN rv < 0.75 THEN 1 ELSE 2 END,
  CASE WHEN rv < 0.75 THEN 'Allowed' ELSE 'Blocked' END,
  CASE WHEN rv < 0.75 THEN 1 ELSE 2 END,
  current_timestamp() - make_interval(0,0,0, abs(hash(id,42))%30, abs(hash(id,43))%24, abs(hash(id,44))%60, abs(hash(id,45))%60),
  concat('API Activity: ', op_type), 600301,
  concat(user_name, ' called ', api_op, ' from ', src_ip),
  named_struct('operation', api_op, 'request', named_struct('data', cast(null as variant), 'uid', cast(null as string)), 'response', named_struct('code', CASE WHEN rv < 0.75 THEN 200 ELSE 403 END, 'data', cast(null as variant), 'error', CASE WHEN rv >= 0.75 THEN 'AccessDenied' ELSE cast(null as string) END, 'message', CASE WHEN rv >= 0.75 THEN 'Access Denied' ELSE 'OK' END)),
  named_struct(
    'app_name', cast(null as string), 'app_uid', cast(null as string),
    'authorizations', cast(null as array<struct<decision:string>>),
    'idp', cast(null as struct<domain:string,name:string,protocol_name:string,tenant_uid:string,uid:string>),
    'process', cast(null as struct<cmd_line:string,cpid:string,name:string,pid:int,session:struct<created_time:timestamp,credential_uid:string,expiration_reason:string,expiration_time:timestamp,is_mfa:boolean,is_remote:boolean,is_vpn:boolean,issuer:string,terminal:string,uid:string,uid_alt:string,uuid:string>,uid:string,user:struct<has_mfa:boolean,name:string,type:string,type_id:int,uid:string>>),
    'user', named_struct('has_mfa', true, 'name', user_name, 'type', 'User', 'type_id', 1, 'uid', concat(user_name, '@acme.com'))
  ),
  named_struct('domain', cast(null as string), 'hostname', cast(null as string), 'instance_uid', cast(null as string), 'interface_name', cast(null as string), 'interface_uid', cast(null as string), 'ip', src_ip, 'name', cast(null as string), 'port', cast(null as int), 'svc_name', cast(null as string), 'type', cast(null as string), 'type_id', cast(null as int), 'uid', cast(null as string), 'location', cast(null as struct<city:string,continent:string,country:string,lat:float,long:float,postal_code:string>), 'mac', cast(null as string), 'vpc_uid', cast(null as string), 'zone', cast(null as string)),
  named_struct('account', named_struct('name', 'acme-production', 'uid', '123456789012'), 'cloud_partition', cast(null as string), 'project_uid', cast(null as string), 'provider', 'AWS', 'region', ELT(1+abs(hash(id,50))%3, 'us-east-1','us-west-2','eu-west-1'), 'zone', cast(null as string)),
  named_struct('correlation_uid', cast(null as string), 'event_code', cast(null as string), 'log_level', cast(null as string), 'log_name', cast(null as string), 'log_provider', cast(null as string), 'log_version', cast(null as string), 'logged_time', cast(null as timestamp), 'modified_time', cast(null as timestamp), 'original_time', cast(null as string), 'processed_time', current_timestamp(), 'product', named_struct('name', 'AWS CloudTrail', 'vendor_name', 'AWS', 'version', '1.0'), 'tags', cast(null as variant), 'tenant_uid', cast(null as string), 'uid', concat('meta-api-', lpad(cast(id as string), 6, '0')), 'version', '1.3.0')
FROM (
  SELECT id, abs(hash(id,1))%100/100.0 as rv,
    ELT(1+abs(hash(id,10))%8, 'alice.johnson','bob.smith','carol.williams','dave.chen','eve.martinez','admin','svc-backup','root') as user_name,
    CASE WHEN abs(hash(id,1))%100/100.0 >= 0.70
      THEN ELT(1+abs(hash(id,20))%5, '185.220.101.34','91.219.237.229','194.26.29.113','45.155.205.233','23.129.64.130')
      ELSE ELT(1+abs(hash(id,20))%5, '10.0.1.50','10.0.1.51','10.0.2.100','172.16.0.10','192.168.1.25')
    END as src_ip,
    ELT(1+abs(hash(id,60))%10, 'iam:CreateUser','iam:AttachRolePolicy','iam:CreateAccessKey','s3:GetObject','s3:PutBucketPolicy','ec2:RunInstances','kms:Decrypt','sts:AssumeRole','secretsmanager:GetSecretValue','lambda:UpdateFunctionCode') as api_op,
    ELT(1+abs(hash(id,60))%10, 'IAM','IAM','IAM','S3','S3','EC2','KMS','STS','SecretsManager','Lambda') as svc_name,
    ELT(1+abs(hash(id,60))%10, 'Create','Update','Create','Read','Update','Create','Read','Read','Read','Update') as op_type,
    CASE WHEN abs(hash(id,1))%100/100.0 < 0.60 THEN 'Informational' WHEN abs(hash(id,1))%100/100.0 < 0.80 THEN 'Medium' WHEN abs(hash(id,1))%100/100.0 < 0.92 THEN 'High' ELSE 'Critical' END as severity,
    CASE WHEN abs(hash(id,1))%100/100.0 < 0.60 THEN 1 WHEN abs(hash(id,1))%100/100.0 < 0.80 THEN 3 WHEN abs(hash(id,1))%100/100.0 < 0.92 THEN 4 ELSE 5 END as severity_id
  FROM range(3000)
)
""")
print("Wrote 3000 API activity events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Vulnerability Findings

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog}.gold.vulnerability_finding (dasl_id, activity_name, category_name, class_name, severity, severity_id, status, status_id, disposition, disposition_id, time, type_name, type_uid, message, device, vulnerabilities, metadata)
SELECT
  concat('vuln-demo-', lpad(cast(id as string), 6, '0')),
  'Create', 'Findings', 'Vulnerability Finding', severity, severity_id,
  ELT(1+abs(hash(id,80))%4, 'New','In Progress','Resolved','Suppressed'),
  1+abs(hash(id,80))%4,
  'Detected', 1,
  current_timestamp() - make_interval(0,0,0, abs(hash(id,42))%30, abs(hash(id,43))%24, abs(hash(id,44))%60, abs(hash(id,45))%60),
  'Vulnerability Finding: Create', 200201,
  concat(cve_id, ': ', cve_desc, ' on ', hostname),
  named_struct('hostname', hostname, 'ip', host_ip, 'os', named_struct('name', os_name), 'type', 'Server', 'type_id', 1),
  array(named_struct('cve', named_struct('uid', cve_id, 'cvss', array(named_struct('base_score', cvss_score, 'version', '3.1'))), 'desc', cve_desc, 'severity', severity, 'title', cve_desc)),
  named_struct('correlation_uid', cast(null as string), 'event_code', cast(null as string), 'log_level', cast(null as string), 'log_name', cast(null as string), 'log_provider', cast(null as string), 'log_version', cast(null as string), 'logged_time', cast(null as timestamp), 'modified_time', cast(null as timestamp), 'original_time', cast(null as string), 'processed_time', current_timestamp(), 'product', named_struct('name', 'Qualys VMDR', 'vendor_name', 'Qualys', 'version', '10.0'), 'tags', cast(null as variant), 'tenant_uid', cast(null as string), 'uid', concat('meta-vuln-', lpad(cast(id as string), 6, '0')), 'version', '1.3.0')
FROM (
  SELECT id,
    ELT(1+abs(hash(id,11))%10, 'CVE-2024-3094','CVE-2024-21762','CVE-2023-44487','CVE-2024-1709','CVE-2023-46805','CVE-2024-0012','CVE-2023-4966','CVE-2024-27198','CVE-2023-22515','CVE-2024-23897') as cve_id,
    ELT(1+abs(hash(id,11))%10, 'XZ Utils Backdoor RCE','FortiOS Out-of-Bound Write RCE','HTTP/2 Rapid Reset DDoS','ScreenConnect Auth Bypass','Ivanti Connect Secure Auth Bypass','PAN-OS Management RCE','Citrix Bleed Info Disclosure','TeamCity Auth Bypass','Confluence Privilege Escalation','Jenkins Arbitrary File Read') as cve_desc,
    ELT(1+abs(hash(id,11))%10, 'Critical','Critical','High','Critical','High','Critical','High','Critical','Critical','High') as severity,
    cast(ELT(1+abs(hash(id,11))%10, '5','5','4','5','4','5','4','5','5','4') as int) as severity_id,
    cast(ELT(1+abs(hash(id,11))%10, '10.0','9.8','7.5','10.0','8.2','9.8','7.5','9.8','9.8','7.5') as float) as cvss_score,
    ELT(1+abs(hash(id,22))%8, 'web-prod-01','web-prod-02','api-server-01','db-primary','ci-runner-01','vpn-gateway','win-dc-01','win-jump-01') as hostname,
    ELT(1+abs(hash(id,22))%8, '10.0.1.10','10.0.1.11','10.0.2.20','10.0.3.30','10.0.4.40','10.0.0.5','10.0.5.50','10.0.5.51') as host_ip,
    ELT(1+abs(hash(id,22))%8, 'Linux','Linux','Linux','Linux','Linux','Linux','Windows','Windows') as os_name
  FROM range(1500)
)
""")
print("Wrote 1500 vulnerability findings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DNS Activity

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog}.gold.dns_activity (dasl_id, activity_name, category_name, class_name, severity, severity_id, disposition, disposition_id, time, type_name, type_uid, message, app_name, src_endpoint, dst_endpoint, metadata)
SELECT
  concat('dns-demo-', lpad(cast(id as string), 6, '0')),
  'Query', 'Network Activity', 'DNS Activity',
  CASE WHEN rv < 0.70 THEN 'Informational' WHEN rv < 0.85 THEN 'High' ELSE 'Critical' END,
  CASE WHEN rv < 0.70 THEN 1 WHEN rv < 0.85 THEN 4 ELSE 5 END,
  CASE WHEN rv < 0.70 THEN 'Allowed' ELSE 'Blocked' END,
  CASE WHEN rv < 0.70 THEN 1 ELSE 2 END,
  current_timestamp() - make_interval(0,0,0, abs(hash(id,42))%30, abs(hash(id,43))%24, abs(hash(id,44))%60, abs(hash(id,45))%60),
  'DNS Activity: Query', 400301,
  concat('DNS query for ', domain),
  'DNS',
  named_struct('domain', cast(null as string), 'hostname', cast(null as string), 'instance_uid', cast(null as string), 'interface_name', cast(null as string), 'interface_uid', cast(null as string), 'ip', ELT(1+abs(hash(id,20))%5, '10.0.1.50','10.0.1.51','10.0.2.100','172.16.0.10','192.168.1.25'), 'name', cast(null as string), 'port', cast(null as int), 'svc_name', cast(null as string), 'type', cast(null as string), 'type_id', cast(null as int), 'uid', cast(null as string), 'location', cast(null as struct<city:string,continent:string,country:string,lat:float,long:float,postal_code:string>), 'mac', cast(null as string), 'vpc_uid', cast(null as string), 'zone', cast(null as string)),
  named_struct('domain', cast(null as string), 'hostname', cast(null as string), 'instance_uid', cast(null as string), 'interface_name', cast(null as string), 'interface_uid', cast(null as string), 'ip', '10.0.0.2', 'name', cast(null as string), 'port', 53, 'svc_name', 'dns', 'type', cast(null as string), 'type_id', cast(null as int), 'uid', cast(null as string), 'location', cast(null as struct<city:string,continent:string,country:string,lat:float,long:float,postal_code:string>), 'mac', cast(null as string), 'vpc_uid', cast(null as string), 'zone', cast(null as string)),
  named_struct('correlation_uid', cast(null as string), 'event_code', cast(null as string), 'log_level', cast(null as string), 'log_name', cast(null as string), 'log_provider', cast(null as string), 'log_version', cast(null as string), 'logged_time', cast(null as timestamp), 'modified_time', cast(null as timestamp), 'original_time', cast(null as string), 'processed_time', current_timestamp(), 'product', named_struct('name', 'Route53 DNS', 'vendor_name', 'AWS', 'version', '1.0'), 'tags', cast(null as variant), 'tenant_uid', cast(null as string), 'uid', concat('meta-dns-', lpad(cast(id as string), 6, '0')), 'version', '1.3.0')
FROM (
  SELECT id, abs(hash(id,1))%100/100.0 as rv,
    CASE
      WHEN abs(hash(id,1))%100/100.0 < 0.70 THEN ELT(1+abs(hash(id,33))%7, 'api.databricks.com','login.microsoft.com','s3.amazonaws.com','pypi.org','github.com','slack.com','google.com')
      WHEN abs(hash(id,1))%100/100.0 < 0.85 THEN concat(substring(md5(cast(id as string)), 1, 16), ELT(1+abs(hash(id,44))%4, '.xyz','.cc','.tk','.ml'))
      ELSE concat(substring(md5(cast(id*7 as string)), 1, 40), '.tunnel.', ELT(1+abs(hash(id,44))%4, 'update-service.xyz','cdn-analytics.cc','api-telemetry.ru','cloud-sync.cn'))
    END as domain
  FROM range(4000)
)
""")
print("Wrote 4000 DNS activity events")

# COMMAND ----------

print("Demo data generation complete!")
print("Total: 13,500 OCSF events across authentication, API, DNS, and vulnerability tables")
