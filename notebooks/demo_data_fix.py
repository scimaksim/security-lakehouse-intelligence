# Databricks notebook source
dbutils.widgets.text("catalog", "sec_lakehouse", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# DNS Activity
spark.sql(f"""
INSERT INTO {catalog}.gold.dns_activity (dasl_id, activity_name, category_name, class_name, severity, severity_id, disposition, disposition_id, time, type_name, type_uid, message, app_name, src_endpoint, dst_endpoint, metadata, query)
SELECT
  concat('dns-demo-', lpad(cast(id as string), 6, '0')),
  'Query', 'Network Activity', 'DNS Activity',
  CASE WHEN rv < 0.70 THEN 'Informational' WHEN rv < 0.85 THEN 'High' ELSE 'Critical' END,
  CASE WHEN rv < 0.70 THEN 1 WHEN rv < 0.85 THEN 4 ELSE 5 END,
  CASE WHEN rv < 0.70 THEN 'Allowed' ELSE 'Blocked' END,
  CASE WHEN rv < 0.70 THEN 1 ELSE 2 END,
  current_timestamp() - make_interval(0,0,0, abs(hash(id,42))%30, abs(hash(id,43))%24, abs(hash(id,44))%60, abs(hash(id,45))%60),
  'DNS Activity: Query',
  cast(400301 as bigint),
  concat('DNS query for ', domain),
  'DNS',
  named_struct('domain', cast(null as string), 'hostname', cast(null as string), 'instance_uid', cast(null as string), 'interface_name', cast(null as string), 'interface_uid', cast(null as string), 'ip', ELT(1+abs(hash(id,20))%5, '10.0.1.50','10.0.1.51','10.0.2.100','172.16.0.10','192.168.1.25'), 'name', cast(null as string), 'port', cast(null as int), 'svc_name', cast(null as string), 'type', cast(null as string), 'type_id', cast(null as int), 'uid', cast(null as string), 'location', cast(null as struct<city:string,continent:string,country:string,lat:float,long:float,postal_code:string>), 'mac', cast(null as string), 'vpc_uid', cast(null as string), 'zone', cast(null as string)),
  named_struct('domain', cast(null as string), 'hostname', cast(null as string), 'instance_uid', cast(null as string), 'interface_name', cast(null as string), 'interface_uid', cast(null as string), 'ip', '10.0.0.2', 'name', cast(null as string), 'port', 53, 'svc_name', 'dns', 'type', cast(null as string), 'type_id', cast(null as int), 'uid', cast(null as string), 'location', cast(null as struct<city:string,continent:string,country:string,lat:float,long:float,postal_code:string>), 'mac', cast(null as string), 'vpc_uid', cast(null as string), 'zone', cast(null as string)),
  named_struct('correlation_uid', cast(null as string), 'event_code', cast(null as string), 'log_level', cast(null as string), 'log_name', cast(null as string), 'log_provider', cast(null as string), 'log_version', cast(null as string), 'logged_time', cast(null as timestamp), 'modified_time', cast(null as timestamp), 'original_time', cast(null as string), 'processed_time', current_timestamp(), 'product', named_struct('name', 'Route53 DNS', 'vendor_name', 'AWS', 'version', '1.0'), 'tags', cast(null as variant), 'tenant_uid', cast(null as string), 'uid', concat('meta-dns-', lpad(cast(id as string), 6, '0')), 'version', '1.3.0'),
  named_struct('class', 'IN', 'packet_uid', cast(null as int), 'type', 'A', 'hostname', domain, 'opcode', 'QUERY', 'opcode_id', 0)
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

# Vulnerability Findings
spark.sql(f"""
INSERT INTO {catalog}.gold.vulnerability_finding (dasl_id, activity_name, category_name, class_name, severity, severity_id, status, status_id, disposition, disposition_id, time, type_name, message, device, vulnerabilities, metadata)
SELECT
  concat('vuln-demo-', lpad(cast(id as string), 6, '0')),
  'Create', 'Findings', 'Vulnerability Finding', severity, severity_id,
  ELT(1+abs(hash(id,80))%4, 'New','In Progress','Resolved','Suppressed'),
  1+abs(hash(id,80))%4,
  'Detected', 1,
  current_timestamp() - make_interval(0,0,0, abs(hash(id,42))%30, abs(hash(id,43))%24, abs(hash(id,44))%60, abs(hash(id,45))%60),
  'Vulnerability Finding: Create',
  concat(cve_id, ': ', cve_desc, ' on ', hostname),
  named_struct('created_time', cast(null as timestamp), 'desc', cast(null as string), 'domain', cast(null as string),
    'groups', cast(null as array<struct<name:string,privileges:string,type:string,uid:string>>),
    'hostname', hostname, 'ip', host_ip, 'is_compliant', cast(null as boolean), 'is_managed', true,
    'is_personal', false, 'is_trusted', true, 'name', hostname, 'region', 'us-west-2',
    'risk_level', severity, 'risk_level_id', severity_id, 'risk_score', cast(cvss_score * 10 as int),
    'subnet', cast(null as string), 'type', 'Server', 'type_id', 1, 'uid', host_ip),
  array(named_struct(
    'cve', named_struct('created_time', cast(null as timestamp), 'cvss', array(named_struct('base_score', cvss_score, 'overall_score', cast(null as float), 'severity', severity, 'src_url', cast(null as string))), 'uid', cve_id),
    'desc', cve_desc,
    'exploit_last_seen_time', cast(null as timestamp), 'first_seen_time', cast(null as timestamp),
    'fix_available', cast(null as boolean), 'is_exploit_available', cast(null as boolean), 'is_fix_available', cast(null as boolean))),
  named_struct('correlation_uid', cast(null as string), 'event_code', cast(null as string), 'log_level', cast(null as string), 'log_name', cast(null as string), 'log_provider', cast(null as string), 'log_version', cast(null as string), 'logged_time', cast(null as timestamp), 'modified_time', cast(null as timestamp), 'original_time', cast(null as string), 'processed_time', current_timestamp(), 'product', named_struct('name', 'Qualys VMDR', 'vendor_name', 'Qualys', 'version', '10.0'), 'tags', cast(null as variant), 'tenant_uid', cast(null as string), 'uid', concat('meta-vuln-', lpad(cast(id as string), 6, '0')), 'version', '1.3.0')
FROM (
  SELECT id,
    ELT(1+abs(hash(id,11))%10, 'CVE-2024-3094','CVE-2024-21762','CVE-2023-44487','CVE-2024-1709','CVE-2023-46805','CVE-2024-0012','CVE-2023-4966','CVE-2024-27198','CVE-2023-22515','CVE-2024-23897') as cve_id,
    ELT(1+abs(hash(id,11))%10, 'XZ Utils Backdoor RCE','FortiOS Out-of-Bound Write RCE','HTTP/2 Rapid Reset DDoS','ScreenConnect Auth Bypass','Ivanti Connect Secure Auth Bypass','PAN-OS Management RCE','Citrix Bleed Info Disclosure','TeamCity Auth Bypass','Confluence Privilege Escalation','Jenkins Arbitrary File Read') as cve_desc,
    ELT(1+abs(hash(id,11))%10, 'Critical','Critical','High','Critical','High','Critical','High','Critical','Critical','High') as severity,
    cast(ELT(1+abs(hash(id,11))%10, '5','5','4','5','4','5','4','5','5','4') as int) as severity_id,
    cast(ELT(1+abs(hash(id,11))%10, '10.0','9.8','7.5','10.0','8.2','9.8','7.5','9.8','9.8','7.5') as float) as cvss_score,
    ELT(1+abs(hash(id,22))%8, 'web-prod-01','web-prod-02','api-server-01','db-primary','ci-runner-01','vpn-gateway','win-dc-01','win-jump-01') as hostname,
    ELT(1+abs(hash(id,22))%8, '10.0.1.10','10.0.1.11','10.0.2.20','10.0.3.30','10.0.4.40','10.0.0.5','10.0.5.50','10.0.5.51') as host_ip
  FROM range(1500)
)
""")
print("Wrote 1500 vulnerability findings")

# COMMAND ----------

print("Done! Remaining demo data inserted into DNS and vulnerability tables.")
