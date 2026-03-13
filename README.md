# Security Lakehouse Intelligence

A production-ready cybersecurity analytics demo built entirely on the Databricks Data Intelligence Platform. Deploys a full security operations dashboard with AI-powered investigation capabilities — from synthetic threat data generation to natural language querying — all within a single Databricks Asset Bundle.

![Architecture](https://img.shields.io/badge/Databricks-Asset%20Bundle-orange) ![OCSF](https://img.shields.io/badge/Schema-OCSF%201.3-blue) ![AI](https://img.shields.io/badge/AI-Foundation%20Models-purple)

## What This Demo Shows

**The core premise**: A Security Operations Center (SOC) analyst can go from raw security telemetry to actionable threat intelligence using nothing but the Databricks platform — no external SIEM, no separate AI tools, no custom infrastructure.

### The Dashboard

A dark-themed, real-time security operations interface featuring:

- **Overview** — aggregate threat metrics, severity distribution charts, and 30-day event timelines across all security domains
- **Authentication Events** — login attempts with geo-location, MFA status, brute-force detection from known threat-actor IPs (Russia, China, North Korea, Iran)
- **API Activity** — AWS CloudTrail events including IAM privilege escalation, unauthorized S3 access, and KMS decryption attempts
- **DNS Activity** — domain queries with DGA detection, DNS tunneling indicators, and sinkhole dispositions
- **Vulnerability Findings** — CVE tracking with CVSS scores, remediation status, and host-level exposure mapping

### AI-Powered Investigation

Every data tab includes an **"Ask AI"** button that opens a floating query window with suggested questions tailored to the current view. Analysts can ask questions in plain English and get SQL-backed answers with data tables.

The **Security AI Assistant** tab provides two modes:

- **Genie Mode** — Direct natural language to SQL via Databricks Genie. Ask "Which IPs have the most critical auth failures?" and get an instant, data-backed answer with the generated SQL.
- **Agent Mode** — Multi-step deep research powered by Foundation Models. Complex questions like "Investigate the overall security posture and identify the highest-risk attack vectors" are automatically decomposed into sub-queries, each executed through Genie, then synthesized into a comprehensive analyst report.

## Databricks Tools Powering This

| Component | Databricks Feature | Role |
|---|---|---|
| **Data Layer** | Unity Catalog | OCSF-normalized gold tables with column-level annotations |
| **Compute** | SQL Warehouse (Serverless) | Sub-second queries across millions of security events |
| **AI Assistant** | Genie Spaces + Conversation API | Natural language → SQL with business context and semantic understanding |
| **Deep Research** | Foundation Model API (Claude) | Multi-step question decomposition and report synthesis |
| **Semantic Layer** | Metric Views | Dimensions, measures, and join relationships for Genie accuracy |
| **Application** | Databricks Apps | Full-stack web app (FastAPI + React) with managed auth and deployment |
| **Security Schema** | DASL (Databricks Security Lakehouse) | OCSF 1.3 table schemas for normalized security telemetry |
| **Deployment** | Databricks Asset Bundles | One-command reproducible deployment across any workspace |

## The Power of Databricks AI for Cybersecurity

Traditional security platforms force analysts to context-switch between dashboards, SIEM tools, threat intel feeds, and manual SQL consoles. This demo showcases a fundamentally different approach:

**1. Unified Data + AI Platform**
Security telemetry lives in the same lakehouse where AI models can query it. No data pipelines to external tools, no API integrations to maintain, no data copies getting stale.

**2. Natural Language Threat Hunting**
SOC analysts don't need to know SQL. "Show me all authentication failures from Tor exit nodes without MFA" becomes an instant, accurate query through Genie's semantic understanding of OCSF-normalized data.

**3. AI Agent for Complex Investigations**
The Agent mode demonstrates how Foundation Models can orchestrate multi-step security investigations — decomposing complex questions, querying multiple data sources, correlating findings, and producing analyst-ready reports. This turns hours of manual investigation into seconds.

**4. Governed and Auditable**
Every AI-generated query runs through Unity Catalog with full lineage and access controls. The SQL is visible and auditable — analysts can verify what the AI did, not just trust a black box.

**5. Extensible to Real Data**
While this demo uses synthetic data, the architecture is production-ready. Replace the demo data generator with real log ingestion (via DASL connectors for AWS CloudTrail, Okta, CrowdStrike, etc.) and the entire dashboard, Genie space, and AI assistant work identically on live threat data.

## Project Structure

```
security-lakehouse-intelligence/
├── databricks.yml                    # Bundle configuration
├── README.md
├── notebooks/
│   ├── demo_data_generator.py        # Generates synthetic OCSF security events
│   ├── demo_data_fix.py              # Additional DNS + vulnerability data
│   └── genie-space-generator/        # Auto-creates Genie space with semantic layer
│       ├── genie_space_setup.py      # Entry point notebook
│       ├── config.yaml               # Genie space configuration
│       └── framework/                # LLM-powered metric view + space builder
├── app/                              # Databricks App (dashboard)
│   ├── app.py                        # FastAPI entry point
│   ├── requirements.txt
│   ├── server/
│   │   ├── config.py                 # Dual-mode auth (local / Databricks App)
│   │   ├── db.py                     # SQL Warehouse connection
│   │   └── routes.py                 # API routes + Genie + Research mode
│   └── frontend/
│       ├── src/                      # React + TypeScript source
│       └── dist/                     # Built frontend (committed for deployment)
```

## Deployment

### Prerequisites

- A Databricks workspace with:
  - **Unity Catalog** enabled
  - **Serverless SQL Warehouse**
  - **Databricks Apps** enabled
  - **Foundation Model API** access (for Agent mode)
  - **DASL** installed (creates the `sec_lakehouse` catalog with OCSF schemas)
- **Databricks CLI** v0.229.0+ authenticated to the workspace
- **Node.js** 18+ (only if rebuilding the frontend)

### Step 1: Configure the Bundle

```bash
git clone https://github.com/scimaksim/security-lakehouse-intelligence.git
cd security-lakehouse-intelligence
```

Edit `databricks.yml` and set your variables, or pass them at deploy time:

```bash
# Find your SQL warehouse ID
databricks warehouses list --profile <your-profile>
```

### Step 2: Deploy

```bash
# Deploy the bundle (uploads notebooks, creates job + app)
databricks bundle deploy --var warehouse_id=<YOUR_WAREHOUSE_ID> --profile <your-profile>

# Run the setup job (generates demo data + creates Genie space)
databricks bundle run setup_security_lakehouse --profile <your-profile>
```

### Step 3: Set the Genie Space ID

After the setup job completes, the Genie space ID will be printed in the notebook output. Update the bundle:

```bash
databricks bundle deploy \
  --var warehouse_id=<YOUR_WAREHOUSE_ID> \
  --var genie_space_id=<GENIE_SPACE_ID> \
  --profile <your-profile>
```

### Step 4: Launch the App

```bash
databricks bundle run security_dashboard --profile <your-profile>
```

The app URL will be printed in the output. Open it to access the dashboard.

### Rebuilding the Frontend (Optional)

If you modify the React source code:

```bash
cd app/frontend
npm install
npm run build   # Outputs to dist/
cd ../..
databricks bundle deploy --profile <your-profile>
databricks bundle run security_dashboard --profile <your-profile>
```

## Local Development

```bash
# Backend
cd app
export DATABRICKS_PROFILE=<your-profile>
export WAREHOUSE_ID=<your-warehouse-id>
export CATALOG=sec_lakehouse
export GENIE_SPACE_ID=<your-genie-space-id>
pip install -r requirements.txt
uvicorn app:app --reload --port 8000

# Frontend (in a separate terminal)
cd app/frontend
npm install
npm run dev   # Runs on port 5173 with proxy to 8000
```

## License

This project is provided as-is for demonstration purposes.
