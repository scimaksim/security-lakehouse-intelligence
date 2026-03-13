import { useState, useEffect, useRef } from 'react'

interface Filter {
  key: string
  options: string[]
}

interface QueryResult { columns: string[]; rows: string[][] }

interface Props {
  endpoint: string
  title: string
  columns: string[]
  filters: Filter[]
}

export default function EventTable({ endpoint, title, columns, filters }: Props) {
  const [events, setEvents] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [filterValues, setFilterValues] = useState<Record<string, string>>({})
  const [askOpen, setAskOpen] = useState(false)
  const [askInput, setAskInput] = useState('')
  const [askBusy, setAskBusy] = useState(false)
  const [askResult, setAskResult] = useState<{ reply: string; sql?: string; queryResult?: QueryResult } | null>(null)
  const askRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    setLoading(true)
    const params = new URLSearchParams()
    Object.entries(filterValues).forEach(([k, v]) => { if (v) params.set(k, v) })
    fetch(`${endpoint}?${params}`).then(r => r.json()).then(d => {
      setEvents(d.events || [])
      setLoading(false)
    })
  }, [endpoint, filterValues])

  useEffect(() => {
    if (askOpen) askRef.current?.focus()
  }, [askOpen])

  const askGenieWith = async (question: string) => {
    if (!question.trim() || askBusy) return
    setAskBusy(true)
    setAskResult(null)
    try {
      const askResp = await fetch('/api/genie/ask', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question }),
      })
      if (!askResp.ok) throw new Error(`Failed: ${askResp.status}`)
      const askData = await askResp.json()

      let result: any = null
      for (let i = 0; i < 40; i++) {
        await new Promise(r => setTimeout(r, 3000))
        const pollResp = await fetch(`/api/genie/poll/${askData.conversation_id}/${askData.message_id}`)
        if (!pollResp.ok) continue
        result = await pollResp.json()
        if (['COMPLETED', 'COMPLETED_WITH_ERRORS', 'FAILED'].includes(result.status)) break
      }

      setAskResult({
        reply: result?.reply || result?.description || 'Query completed.',
        sql: result?.sql,
        queryResult: result?.query_result,
      })
    } catch (err: any) {
      setAskResult({ reply: `Error: ${err.message}` })
    } finally {
      setAskBusy(false)
    }
  }

  const formatValue = (col: string, val: any) => {
    if (val === null || val === undefined) return '-'
    if (col === 'time') return new Date(val).toLocaleString()
    if (col === 'cvss_score') return <span className={Number(val) >= 9 ? 'critical-text' : ''}>{val}</span>
    if (col === 'severity') return <span className={`badge ${String(val).toLowerCase()}`}>{val}</span>
    if (col === 'status') return <span className={`badge ${val === 'Success' || val === 'Resolved' ? 'success' : val === 'Failure' || val === 'New' ? 'danger' : 'warning'}`}>{val}</span>
    if (col === 'disposition') return <span className={`badge ${val === 'Allowed' ? 'success' : 'danger'}`}>{val}</span>
    return String(val)
  }

  const colLabel = (col: string) => col.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())

  const tableName = title.toLowerCase().replace(/\s+events?|\s+findings?/gi, '').trim()
  const suggestions: Record<string, string[]> = {
    'authentication': [
      'How many failed logins in the last 7 days?',
      'Which IPs have the most critical auth failures?',
      'Show failed logins without MFA enabled',
      'Top 10 countries by failed login attempts',
      'Which users had the most failed logins this week?',
    ],
    'api activity': [
      'Which API operations have the most failures?',
      'Top 5 users by API call volume',
      'Show unauthorized API calls from the last 24 hours',
      'Which cloud regions generate the most high-severity API events?',
    ],
    'dns activity': [
      'Top 10 blocked domains by query count',
      'Which IPs are querying the most suspicious domains?',
      'Count of high-severity DNS queries by domain',
      'Show all sinkholed DNS queries',
    ],
    'vulnerability': [
      'Top 5 CVEs by CVSS score',
      'How many critical vulnerabilities are still unresolved?',
      'Which hosts have the most unpatched critical CVEs?',
      'Show CVEs with CVSS score above 9',
      'Count of vulnerabilities by status',
    ],
  }
  const tabSuggestions = suggestions[tableName] || []
  const placeholder = tabSuggestions[0] ? `e.g., ${tabSuggestions[0]}` : `Ask about ${title.toLowerCase()}...`

  return (
    <div className="event-table-container" style={{ position: 'relative' }}>
      <div className="table-header">
        <h2>{title}</h2>
        <div className="filters">
          {filters.map(f => (
            <select key={f.key} value={filterValues[f.key] || ''} onChange={e => setFilterValues(p => ({...p, [f.key]: e.target.value}))}>
              <option value="">All {colLabel(f.key)}</option>
              {f.options.map(o => <option key={o} value={o}>{o}</option>)}
            </select>
          ))}
          <button className="ask-ai-btn" onClick={() => { setAskOpen(!askOpen); setAskResult(null) }}>
            <AskIcon size={14} />
            Ask AI
          </button>
        </div>
      </div>

      {askOpen && (
        <div className="ask-ai-float">
          <div className="ask-ai-float-header">
            <span className="ask-ai-float-title"><AskIcon size={14} /> Ask AI about {title}</span>
            <button className="ask-ai-close" onClick={() => setAskOpen(false)}>&times;</button>
          </div>
          <div className="ask-ai-float-body">
            <div className="ask-ai-input-row">
              <input
                ref={askRef}
                type="text"
                placeholder={placeholder}
                value={askInput}
                onChange={e => setAskInput(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && askGenieWith(askInput)}
                disabled={askBusy}
              />
              <button onClick={() => askGenieWith(askInput)} disabled={askBusy || !askInput.trim()} className="ask-ai-send">
                {askBusy ? 'Asking...' : 'Ask'}
              </button>
            </div>
            {!askResult && !askBusy && tabSuggestions.length > 0 && (
              <div className="ask-ai-suggestions">
                {tabSuggestions.map((q, i) => (
                  <button key={i} className="ask-ai-chip" onClick={() => { setAskInput(q); askGenieWith(q); }}>{q}</button>
                ))}
              </div>
            )}
            {askBusy && !askResult && (
              <div className="ask-ai-loading">
                <span className="dot-pulse" />
                <span>Querying Genie...</span>
              </div>
            )}
            {askResult && (
              <div className="ask-ai-result">
                <div className="ask-ai-reply" dangerouslySetInnerHTML={{ __html: renderMarkdown(askResult.reply) }} />
                {askResult.sql && (
                  <details className="msg-sql" open>
                    <summary>Generated SQL</summary>
                    <pre>{formatSql(askResult.sql)}</pre>
                  </details>
                )}
                {askResult.queryResult && askResult.queryResult.columns.length > 0 && (
                  <div className="msg-table-wrap compact">
                    <table className="data-table">
                      <thead><tr>{askResult.queryResult.columns.map((c, ci) => <th key={ci}>{c}</th>)}</tr></thead>
                      <tbody>
                        {askResult.queryResult.rows.slice(0, 20).map((row, ri) => (
                          <tr key={ri}>{row.map((cell, ci) => <td key={ci}>{cell}</td>)}</tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                <button className="ask-ai-new" onClick={() => { setAskResult(null); setAskInput(''); }}>Ask another question</button>
              </div>
            )}
          </div>
        </div>
      )}

      {loading ? <div className="loading">Loading...</div> : (
        <div className="table-scroll">
          <table className="data-table full">
            <thead>
              <tr>{columns.map(c => <th key={c}>{colLabel(c)}</th>)}</tr>
            </thead>
            <tbody>
              {events.map((evt, i) => (
                <tr key={i}>{columns.map(c => <td key={c} className={c === 'message' || c === 'vuln_desc' || c === 'query_domain' ? 'truncate' : c === 'src_ip' || c === 'device_ip' || c === 'cve_id' ? 'mono' : ''}>{formatValue(c, evt[c])}</td>)}</tr>
              ))}
              {events.length === 0 && <tr><td colSpan={columns.length} className="empty">No events found</td></tr>}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function AskIcon({ size = 16 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="icon">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
    </svg>
  )
}

function formatSql(sql: string): string {
  if (!sql) return ''
  return sql
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY|HAVING|LIMIT|UNION ALL|UNION|LEFT JOIN|RIGHT JOIN|INNER JOIN|JOIN|ON|AND|OR|WITH|AS \(|INSERT|UPDATE|DELETE|SET|VALUES|INTO|CREATE|ALTER|DROP|CASE|WHEN|THEN|ELSE|END)\b/gi,
      (match) => '\n' + match.toUpperCase())
    .replace(/^\n/, '')
    .replace(/,\s*/g, ',\n  ')
}

function renderMarkdown(text: string): string {
  if (!text) return ''
  return text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/^### (.+)$/gm, '<h4>$1</h4>')
    .replace(/^## (.+)$/gm, '<h3>$1</h3>')
    .replace(/^# (.+)$/gm, '<h2>$1</h2>')
    .replace(/^- (.+)$/gm, '<li>$1</li>')
    .replace(/(<li>.*<\/li>)/gs, '<ul>$1</ul>')
    .replace(/<\/ul>\s*<ul>/g, '')
    .replace(/^\d+\. (.+)$/gm, '<li>$1</li>')
    .replace(/\n\n/g, '<br/><br/>')
    .replace(/\n/g, '<br/>')
}
