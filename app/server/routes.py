from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import time
import json
import threading
import requests as http_requests
from .db import execute_query
from .config import CATALOG, GENIE_SPACE_ID, get_workspace_client, get_workspace_host

router = APIRouter()

GOLD = f"{CATALOG}.gold"

SERVING_ENDPOINT = "databricks-claude-sonnet-4-6"


# ── Helpers ──────────────────────────────────────────────────────────

def _get_token():
    w = get_workspace_client()
    token = w.config.authenticate()
    return token.get("Authorization", "").replace("Bearer ", "") if isinstance(token, dict) else ""


def _genie_headers():
    return {"Authorization": f"Bearer {_get_token()}", "Content-Type": "application/json"}


def _genie_base():
    host = get_workspace_host()
    return f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}"


def _genie_ask_and_poll(question: str, conversation_id: str = None):
    """Send a question to Genie and poll until complete. Returns result dict."""
    headers = _genie_headers()
    base = _genie_base()

    if conversation_id:
        url = f"{base}/conversations/{conversation_id}/messages"
        resp = http_requests.post(url, headers=headers, json={"content": question}, timeout=30)
    else:
        url = f"{base}/start-conversation"
        resp = http_requests.post(url, headers=headers, json={"content": question}, timeout=30)

    resp.raise_for_status()
    data = resp.json()
    conv_id = data.get("conversation_id", conversation_id)
    msg_id = data.get("message_id") or data.get("id")

    # Poll for result
    for _ in range(45):
        time.sleep(3)
        poll_url = f"{base}/conversations/{conv_id}/messages/{msg_id}"
        poll_resp = http_requests.get(poll_url, headers=headers, timeout=30)
        if not poll_resp.ok:
            continue
        poll_data = poll_resp.json()
        status = poll_data.get("status", "UNKNOWN")
        if status in ("COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED"):
            return _parse_genie_result(poll_data, conv_id, msg_id, headers)

    return {"status": "TIMEOUT", "conversation_id": conv_id, "message_id": msg_id, "reply": "Query timed out."}


def _parse_genie_result(data, conv_id, msg_id, headers):
    status = data.get("status", "UNKNOWN")
    result = {"status": status, "conversation_id": conv_id, "message_id": msg_id}

    attachments = data.get("attachments", [])
    reply_text = ""
    query_result = None

    for att in attachments:
        if att.get("text") and att["text"].get("content"):
            reply_text += att["text"]["content"] + "\n"
        if att.get("query"):
            query_info = att["query"]
            result["sql"] = query_info.get("query", "")
            result["description"] = query_info.get("description", "")
            att_id = att.get("attachment_id") or query_info.get("attachment_id")
            if att_id:
                base = _genie_base()
                qr_url = f"{base}/conversations/{conv_id}/messages/{msg_id}/query-result/{att_id}"
                qr_resp = http_requests.get(qr_url, headers=headers, timeout=30)
                if qr_resp.ok:
                    qr_data = qr_resp.json()
                    columns = [c.get("name", "") for c in qr_data.get("statement_response", {}).get("manifest", {}).get("schema", {}).get("columns", [])]
                    rows_raw = qr_data.get("statement_response", {}).get("result", {}).get("data_array", [])
                    query_result = {"columns": columns, "rows": rows_raw[:100]}

    if not reply_text:
        reply_text = data.get("content", "")

    result["reply"] = reply_text.strip()
    if query_result:
        result["query_result"] = query_result
    return result


def _llm_call(messages: list) -> str:
    """Call Foundation Model API."""
    host = get_workspace_host()
    token = _get_token()
    resp = http_requests.post(
        f"{host}/serving-endpoints/{SERVING_ENDPOINT}/invocations",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"messages": messages, "max_tokens": 4096, "temperature": 0.3},
        timeout=180,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


# ── Genie Standard API ───────────────────────────────────────────────

class GenieQuestion(BaseModel):
    question: str
    conversation_id: Optional[str] = None


@router.post("/api/genie/ask")
def genie_ask(body: GenieQuestion):
    headers = _genie_headers()
    base = _genie_base()

    if body.conversation_id:
        url = f"{base}/conversations/{body.conversation_id}/messages"
        resp = http_requests.post(url, headers=headers, json={"content": body.question}, timeout=30)
    else:
        url = f"{base}/start-conversation"
        resp = http_requests.post(url, headers=headers, json={"content": body.question}, timeout=30)

    resp.raise_for_status()
    data = resp.json()
    conversation_id = data.get("conversation_id", body.conversation_id)
    message_id = data.get("message_id") or data.get("id")
    return {"conversation_id": conversation_id, "message_id": message_id}


@router.get("/api/genie/poll/{conversation_id}/{message_id}")
def genie_poll(conversation_id: str, message_id: str):
    headers = _genie_headers()
    base = _genie_base()
    url = f"{base}/conversations/{conversation_id}/messages/{message_id}"

    resp = http_requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return _parse_genie_result(data, conversation_id, message_id, headers)


# ── Genie Research Mode (multi-step agent) ────────────────────────────

class ResearchQuestion(BaseModel):
    question: str


class _Result:
    """Wrapper to distinguish fn() result from keepalive strings."""
    __slots__ = ("value",)
    def __init__(self, v):
        self.value = v


def _run_in_thread_with_keepalive(fn):
    """Run fn() in a thread while yielding keepalive SSE every 2s. Yields _Result(value) at end."""
    result_box = [None]
    error_box = [None]

    def _worker():
        try:
            result_box[0] = fn()
        except Exception as e:
            error_box[0] = e

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    while t.is_alive():
        t.join(timeout=2)
        if t.is_alive():
            yield _sse({"type": "keepalive"})

    if error_box[0]:
        raise error_box[0]
    yield _Result(result_box[0])


def _llm_call_safe(messages: list, retries: int = 2) -> str:
    """LLM call with retry on timeout."""
    last_err = None
    for attempt in range(retries + 1):
        try:
            return _llm_call(messages)
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(2)
    raise last_err


def _genie_ask_and_poll_safe(question: str) -> dict:
    """Ask Genie with retry on initial POST failure."""
    headers = _genie_headers()
    base = _genie_base()

    # Retry the initial ask up to 2 times
    for attempt in range(3):
        try:
            url = f"{base}/start-conversation"
            resp = http_requests.post(url, headers=headers, json={"content": question}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            conv_id = data.get("conversation_id")
            msg_id = data.get("message_id") or data.get("id")
            break
        except Exception:
            if attempt == 2:
                return {"reply": f"Failed to submit question to Genie after 3 attempts.", "sql": ""}
            time.sleep(2)

    # Poll with fresh headers each time (token could rotate)
    for _ in range(40):
        time.sleep(3)
        try:
            h = _genie_headers()
            poll_url = f"{base}/conversations/{conv_id}/messages/{msg_id}"
            poll_resp = http_requests.get(poll_url, headers=h, timeout=30)
            if not poll_resp.ok:
                continue
            poll_data = poll_resp.json()
            status = poll_data.get("status", "UNKNOWN")
            if status in ("COMPLETED", "COMPLETED_WITH_ERRORS"):
                return _parse_genie_result(poll_data, conv_id, msg_id, h)
            if status == "FAILED":
                return {"reply": "Genie query failed.", "sql": ""}
        except Exception:
            continue

    return {"reply": "Query timed out after 2 minutes.", "sql": ""}


@router.post("/api/genie/research")
def genie_research(body: ResearchQuestion):
    """Multi-step research: decompose question, run sub-queries via Genie, synthesize."""

    def generate():
        yield _sse({"type": "phase", "phase": "Planning research strategy..."})

        # Step 1: Decompose via LLM (in thread with keepalives)
        decompose_prompt = f"""You are a security data analyst. Break this complex question into 2-4 specific sub-questions that together answer the original question.

Question: "{body.question}"

Available data (OCSF-normalized, 30-day window):
- Authentication events: users, source IPs, countries, severity, success/failure, MFA status
- API activity: API operations, actors, source IPs, cloud regions, severity, status
- DNS activity: queried domains, source IPs, severity, disposition (allowed/blocked/sinkholed)
- Vulnerability findings: hostnames, device IPs, CVE IDs, CVSS scores, severity, status

Rules:
- Each sub-question must be a short, plain-English question (one sentence)
- Do NOT include SQL, column names, table names, or technical schema details
- Do NOT include suggestions or explanations

Respond ONLY with a JSON array of question strings. Example:
["How many failed logins occurred in the last 7 days?", "Which source IPs had the most failures?"]"""

        sub_questions = None
        try:
            for event in _run_in_thread_with_keepalive(
                lambda: _llm_call_safe([{"role": "user", "content": decompose_prompt}])
            ):
                if isinstance(event, _Result):
                    raw = event.value
                    start = raw.index("[")
                    end = raw.rindex("]") + 1
                    sub_questions = json.loads(raw[start:end])
                else:
                    yield event  # keepalive
        except Exception:
            pass

        if not sub_questions:
            sub_questions = [body.question]

        yield _sse({"type": "plan", "sub_questions": sub_questions})

        # Step 2: Run each sub-question through Genie (in thread with keepalives)
        findings = []
        for i, sq in enumerate(sub_questions):
            yield _sse({"type": "phase", "phase": f"Investigating ({i+1}/{len(sub_questions)}): {sq}"})

            result = None
            try:
                for event in _run_in_thread_with_keepalive(
                    lambda q=sq: _genie_ask_and_poll_safe(q)
                ):
                    if isinstance(event, _Result):
                        result = event.value
                    else:
                        yield event
            except Exception as e:
                result = {"reply": f"Error: {e}", "sql": ""}

            if not result:
                result = {"reply": "No answer received.", "sql": ""}

            finding = {
                "question": sq,
                "reply": result.get("reply", "No answer"),
                "sql": result.get("sql", ""),
                "query_result": result.get("query_result"),
            }
            findings.append(finding)
            yield _sse({"type": "finding", "index": i, "finding": finding})

        # Step 3: Synthesize (in thread with keepalives)
        yield _sse({"type": "phase", "phase": "Synthesizing findings into report..."})

        findings_text = ""
        for i, f in enumerate(findings):
            findings_text += f"\n### Sub-question {i+1}: {f['question']}\n"
            findings_text += f"Answer: {f['reply']}\n"
            if f.get("query_result"):
                qr = f["query_result"]
                findings_text += f"Data: {len(qr.get('rows', []))} rows returned\n"
                if qr.get("columns") and qr.get("rows"):
                    for row in qr["rows"][:5]:
                        findings_text += "  " + " | ".join(str(c) for c in row) + "\n"

        synthesis_prompt = f"""You are a senior security analyst. Write a research report based on these findings.

Original question: "{body.question}"

Findings:
{findings_text}

Structure as:
1. **Executive Summary** - 2-3 sentences
2. **Key Findings** - bullet points
3. **Details** - analysis with specific numbers
4. **Recommendations** - actionable next steps

Use markdown. Be concise."""

        report = None
        try:
            for event in _run_in_thread_with_keepalive(
                lambda: _llm_call_safe([{"role": "user", "content": synthesis_prompt}])
            ):
                if isinstance(event, _Result):
                    report = event.value
                else:
                    yield event
        except Exception as e:
            report = f"Error generating report: {e}"

        if not report:
            report = "Report generation failed. See individual findings above for results."

        yield _sse({"type": "report", "content": report, "findings": findings})
        yield _sse({"type": "done"})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


@router.get("/api/overview")
def get_overview():
    counts = execute_query(f"""
        SELECT 'authentication' as table_name, count(*) as cnt, count_if(severity IN ('High','Critical')) as high_crit FROM {GOLD}.authentication
        UNION ALL SELECT 'api_activity', count(*), count_if(severity IN ('High','Critical')) FROM {GOLD}.api_activity
        UNION ALL SELECT 'dns_activity', count(*), count_if(severity IN ('High','Critical')) FROM {GOLD}.dns_activity
        UNION ALL SELECT 'vulnerability_finding', count(*), count_if(severity IN ('High','Critical')) FROM {GOLD}.vulnerability_finding
    """)

    severity = execute_query(f"""
        SELECT severity, count(*) as cnt FROM (
            SELECT severity FROM {GOLD}.authentication
            UNION ALL SELECT severity FROM {GOLD}.api_activity
            UNION ALL SELECT severity FROM {GOLD}.dns_activity
            UNION ALL SELECT severity FROM {GOLD}.vulnerability_finding
        ) GROUP BY severity ORDER BY cnt DESC
    """)

    timeline = execute_query(f"""
        SELECT date_trunc('day', time) as day, count(*) as cnt,
               count_if(severity IN ('High','Critical')) as high_crit
        FROM (
            SELECT time, severity FROM {GOLD}.authentication
            UNION ALL SELECT time, severity FROM {GOLD}.api_activity
            UNION ALL SELECT time, severity FROM {GOLD}.dns_activity
            UNION ALL SELECT time, severity FROM {GOLD}.vulnerability_finding
        )
        WHERE time >= current_timestamp() - INTERVAL 30 DAYS
        GROUP BY 1 ORDER BY 1
    """)

    return {"counts": counts, "severity": severity, "timeline": timeline}


@router.get("/api/authentication")
def get_authentication(
    severity: str = Query(None),
    status: str = Query(None),
    limit: int = Query(100),
):
    where = ["1=1"]
    if severity:
        where.append(f"severity = '{severity}'")
    if status:
        where.append(f"status = '{status}'")
    where_clause = " AND ".join(where)

    rows = execute_query(f"""
        SELECT dasl_id, time, severity, status, disposition, is_mfa, message,
               user.name as user_name, src_endpoint.ip as src_ip,
               src_endpoint.location.city as city, src_endpoint.location.country as country,
               dst_endpoint.svc_name as target_service
        FROM {GOLD}.authentication
        WHERE {where_clause}
        ORDER BY time DESC LIMIT {limit}
    """)
    return {"events": rows}


@router.get("/api/api_activity")
def get_api_activity(
    severity: str = Query(None),
    limit: int = Query(100),
):
    where = ["1=1"]
    if severity:
        where.append(f"severity = '{severity}'")
    where_clause = " AND ".join(where)

    rows = execute_query(f"""
        SELECT dasl_id, time, severity, status, disposition, message,
               api.operation as api_operation,
               actor.user.name as actor_name,
               src_endpoint.ip as src_ip,
               cloud.region as cloud_region
        FROM {GOLD}.api_activity
        WHERE {where_clause}
        ORDER BY time DESC LIMIT {limit}
    """)
    return {"events": rows}


@router.get("/api/dns_activity")
def get_dns_activity(
    severity: str = Query(None),
    limit: int = Query(100),
):
    where = ["1=1"]
    if severity:
        where.append(f"severity = '{severity}'")
    where_clause = " AND ".join(where)

    rows = execute_query(f"""
        SELECT dasl_id, time, severity, disposition, message,
               query.hostname as query_domain,
               src_endpoint.ip as src_ip
        FROM {GOLD}.dns_activity
        WHERE {where_clause}
        ORDER BY time DESC LIMIT {limit}
    """)
    return {"events": rows}


@router.get("/api/vulnerabilities")
def get_vulnerabilities(
    severity: str = Query(None),
    status: str = Query(None),
    limit: int = Query(100),
):
    where = ["1=1"]
    if severity:
        where.append(f"severity = '{severity}'")
    if status:
        where.append(f"status = '{status}'")
    where_clause = " AND ".join(where)

    rows = execute_query(f"""
        SELECT dasl_id, time, severity, status, message,
               device.hostname as hostname, device.ip as device_ip,
               vulnerabilities[0].cve.uid as cve_id,
               vulnerabilities[0].cve.cvss[0].base_score as cvss_score,
               vulnerabilities[0].desc as vuln_desc
        FROM {GOLD}.vulnerability_finding
        WHERE {where_clause}
        ORDER BY time DESC LIMIT {limit}
    """)
    return {"events": rows}


@router.get("/api/top_threats")
def get_top_threats():
    top_ips = execute_query(f"""
        SELECT src_endpoint.ip as ip, count(*) as cnt,
               count_if(severity IN ('High','Critical')) as high_crit
        FROM {GOLD}.authentication
        WHERE status = 'Failure'
        GROUP BY 1 ORDER BY cnt DESC LIMIT 10
    """)

    top_cves = execute_query(f"""
        SELECT vulnerabilities[0].cve.uid as cve_id,
               vulnerabilities[0].desc as description,
               max(vulnerabilities[0].cve.cvss[0].base_score) as cvss,
               count(*) as affected_hosts,
               count_if(status = 'New') as unresolved
        FROM {GOLD}.vulnerability_finding
        GROUP BY 1, 2 ORDER BY cvss DESC, affected_hosts DESC LIMIT 10
    """)

    suspicious_dns = execute_query(f"""
        SELECT query.hostname as domain, count(*) as queries,
               severity
        FROM {GOLD}.dns_activity
        WHERE severity IN ('High','Critical')
        GROUP BY 1, 3 ORDER BY queries DESC LIMIT 10
    """)

    return {"top_ips": top_ips, "top_cves": top_cves, "suspicious_dns": suspicious_dns}
