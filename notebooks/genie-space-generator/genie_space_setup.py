# Databricks notebook source
# MAGIC %md
# MAGIC # Lakewatch Security Genie Space Generator
# MAGIC Creates a Genie Space with AI-generated semantic layer on Lakewatch OCSF Gold tables.

# COMMAND ----------

# MAGIC %pip install langchain-databricks

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Skip auto_configure — we already have a pre-built security config.yaml
# Just run the main framework to create metric views + Genie Space

import sys, os

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    framework_path = '/Workspace' + os.path.dirname(notebook_path)
except:
    framework_path = os.getcwd()

if framework_path not in sys.path:
    sys.path.insert(0, framework_path)

config_path = f'{framework_path}/config.yaml'
print(f"Framework path: {framework_path}")
print(f"Config path: {config_path}")

# Verify config exists
with open(config_path) as f:
    print(f.read()[:500])

# COMMAND ----------

from framework.main_orchestrator import GenieSpaceFramework

config_path_ws = '/Workspace/Users/maksim.nikiforov@databricks.com/genie-space-generator/config.yaml'
framework = GenieSpaceFramework(config_path=config_path_ws)
result = framework.run()

print(f"\n{'='*60}")
print(f"Genie Space created successfully!")
if isinstance(result, dict):
    print(f"URL: {result.get('genie_space_url', result.get('url', 'Check workspace'))}")
    print(f"Space ID: {result.get('space_id', 'N/A')}")
else:
    print(f"Result: {result}")
print(f"{'='*60}")
