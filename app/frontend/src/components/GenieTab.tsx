import { useState, useRef, useEffect } from 'react'

interface QueryResult { columns: string[]; rows: string[][] }
interface Finding { question: string; reply: string; sql?: string; query_result?: QueryResult }

interface Message {
  role: 'user' | 'assistant'
  content: string
  sql?: string
  queryResult?: QueryResult
  loading?: boolean
  mode?: 'genie' | 'research'
  plan?: string[]
  phases?: string[]
  findings?: Finding[]
}

type Mode = 'genie' | 'research'

interface SampleQ { text: string; mode: Mode }

const SAMPLE_QUESTIONS: SampleQ[] = [
  { text: "How many failed logins in the last 7 days?", mode: "genie" },
  { text: "Top 5 CVEs by CVSS score", mode: "genie" },
  { text: "Which IPs have the most critical auth failures?", mode: "genie" },
  { text: "Count of high-severity DNS queries by domain", mode: "genie" },
  { text: "Investigate the overall security posture: correlate auth failures, vulnerability exposure, and suspicious DNS to identify the highest-risk attack vectors", mode: "research" },
  { text: "Research which source IPs appear across multiple event types (auth, API, DNS) and assess whether they represent coordinated threat activity", mode: "research" },
  { text: "Analyze the relationship between unpatched critical CVEs and authentication failures to determine if known vulnerabilities are being actively exploited", mode: "research" },
]

export default function GenieTab() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [conversationId, setConversationId] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [mode, setMode] = useState<Mode>('genie')
  const bottomRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const toggleMode = () => {
    setMode(m => m === 'genie' ? 'research' : 'genie')
    inputRef.current?.focus()
  }

  const askGenie = async (question: string) => {
    const assistantIdx = messages.length + 1
    setMessages(prev => [...prev,
      { role: 'user', content: question, mode: 'genie' },
      { role: 'assistant', content: '', loading: true, mode: 'genie' },
    ])

    try {
      const askResp = await fetch('/api/genie/ask', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, conversation_id: conversationId }),
      })
      if (!askResp.ok) throw new Error(`Ask failed: ${askResp.status}`)
      const askData = await askResp.json()
      setConversationId(askData.conversation_id)

      let result: any = null
      for (let i = 0; i < 60; i++) {
        await new Promise(r => setTimeout(r, 3000))
        try {
          const pollResp = await fetch(`/api/genie/poll/${askData.conversation_id}/${askData.message_id}`)
          if (!pollResp.ok) continue
          result = await pollResp.json()
          if (['COMPLETED', 'COMPLETED_WITH_ERRORS', 'FAILED'].includes(result.status)) break
        } catch { continue }
      }
      if (!result || !['COMPLETED', 'COMPLETED_WITH_ERRORS'].includes(result.status)) {
        throw new Error('Query timed out or failed. Please try again.')
      }

      setMessages(prev => {
        const updated = [...prev]
        updated[assistantIdx] = {
          role: 'assistant', mode: 'genie',
          content: result?.reply || result?.description || 'Query completed.',
          sql: result?.sql,
          queryResult: result?.query_result,
        }
        return updated
      })
    } catch (err: any) {
      setMessages(prev => {
        const updated = [...prev]
        updated[assistantIdx] = { role: 'assistant', content: `Error: ${err.message}`, mode: 'genie' }
        return updated
      })
    }
  }

  const askResearch = async (question: string) => {
    const assistantIdx = messages.length + 1
    setMessages(prev => [...prev,
      { role: 'user', content: question, mode: 'research' },
      { role: 'assistant', content: '', loading: true, mode: 'research', phases: [], plan: [], findings: [] },
    ])

    try {
      const resp = await fetch('/api/genie/research', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question }),
      })
      if (!resp.ok) throw new Error(`Research failed: ${resp.status}`)

      const reader = resp.body!.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })

        const lines = buffer.split('\n')
        buffer = lines.pop() || ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          try {
            const event = JSON.parse(line.slice(6))

            setMessages(prev => {
              const updated = [...prev]
              const msg = { ...updated[assistantIdx] }

              if (event.type === 'phase') {
                msg.phases = [...(msg.phases || []), event.phase]
              } else if (event.type === 'plan') {
                msg.plan = event.sub_questions
              } else if (event.type === 'finding') {
                msg.findings = [...(msg.findings || []), event.finding]
              } else if (event.type === 'report') {
                msg.content = event.content
                msg.loading = false
                msg.findings = event.findings
              } else if (event.type === 'done') {
                msg.loading = false
              }

              updated[assistantIdx] = msg
              return updated
            })
          } catch {}
        }
      }
    } catch (err: any) {
      setMessages(prev => {
        const updated = [...prev]
        updated[assistantIdx] = { role: 'assistant', content: `Error: ${err.message}`, mode: 'research' }
        return updated
      })
    }
  }

  const ask = async (question: string, overrideMode?: Mode) => {
    if (!question.trim() || busy) return
    setInput('')
    setBusy(true)
    const m = overrideMode || mode
    try {
      if (m === 'research') {
        await askResearch(question)
      } else {
        await askGenie(question)
      }
    } finally {
      setBusy(false)
    }
  }

  const isResearch = mode === 'research'

  return (
    <div className="genie-chat">
      <div className="genie-messages">
        {messages.length === 0 && (
          <div className="genie-welcome">
            <div className="genie-welcome-icon">🔮</div>
            <h2>Security AI Assistant</h2>
            <p>Ask questions about your security data. Toggle <strong>Agent mode</strong> for multi-step deep research investigations.</p>
            <div className="genie-suggestions">
              {SAMPLE_QUESTIONS.map((q, i) => (
                <button key={i} className={`suggestion-chip ${q.mode}`} onClick={() => ask(q.text, q.mode)}>
                  <span className={`mode-badge ${q.mode}`}>{q.mode === 'research' ? 'Agent' : 'Genie'}</span>
                  {q.text}
                </button>
              ))}
            </div>
          </div>
        )}
        {messages.map((msg, i) => (
          <div key={i} className={`genie-msg ${msg.role} ${msg.mode || ''}`}>
            <div className="msg-header">
              {msg.role === 'user' ? 'You' : (
                msg.mode === 'research'
                  ? <><AgentIcon size={14} />Agent mode</>
                  : <><GenieIcon size={14} />Genie</>
              )}
            </div>

            {msg.loading ? (
              <div className="msg-loading">
                <span className={`dot-pulse ${msg.mode}`} />
                <div className="research-progress">
                  {msg.plan && msg.plan.length > 0 && (
                    <div className="research-plan">
                      <div className="plan-label">Research plan:</div>
                      {msg.plan.map((q, pi) => {
                        const isDone = (msg.findings || []).length > pi
                        return <div key={pi} className={`plan-step ${isDone ? 'done' : ''}`}>{isDone ? '✓' : '○'} {q}</div>
                      })}
                    </div>
                  )}
                  {msg.phases && msg.phases.length > 0 && (
                    <div className="phase-text">{msg.phases[msg.phases.length - 1]}</div>
                  )}
                </div>
              </div>
            ) : (
              <>
                <div className="msg-text" dangerouslySetInnerHTML={{ __html: renderMarkdown(msg.content) }} />
                {msg.sql && (
                  <details className="msg-sql">
                    <summary>Generated SQL</summary>
                    <pre>{formatSql(msg.sql)}</pre>
                  </details>
                )}
                {msg.queryResult && msg.queryResult.columns.length > 0 && (
                  <div className="msg-table-wrap">
                    <table className="data-table">
                      <thead><tr>{msg.queryResult.columns.map((c, ci) => <th key={ci}>{c}</th>)}</tr></thead>
                      <tbody>
                        {msg.queryResult.rows.map((row, ri) => (
                          <tr key={ri}>{row.map((cell, ci) => <td key={ci}>{cell}</td>)}</tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                {msg.mode === 'research' && msg.findings && msg.findings.length > 0 && (
                  <details className="msg-findings">
                    <summary>View {msg.findings.length} sub-queries</summary>
                    {msg.findings.map((f, fi) => (
                      <div key={fi} className="finding-item">
                        <div className="finding-q">Q{fi+1}: {f.question}</div>
                        <div className="finding-a" dangerouslySetInnerHTML={{ __html: renderMarkdown(f.reply) }} />
                        {f.sql && <details className="msg-sql"><summary>SQL</summary><pre>{formatSql(f.sql)}</pre></details>}
                        {f.query_result && f.query_result.columns.length > 0 && (
                          <div className="msg-table-wrap compact">
                            <table className="data-table">
                              <thead><tr>{f.query_result.columns.map((c, ci) => <th key={ci}>{c}</th>)}</tr></thead>
                              <tbody>{f.query_result.rows.slice(0,10).map((row, ri) => (
                                <tr key={ri}>{row.map((cell, ci) => <td key={ci}>{cell}</td>)}</tr>
                              ))}</tbody>
                            </table>
                          </div>
                        )}
                      </div>
                    ))}
                  </details>
                )}
              </>
            )}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      {isResearch && (
        <div className="agent-mode-banner">
          <AgentIcon size={14} />
          <span>Agent mode is on — your question will be decomposed into multiple sub-queries for deep analysis</span>
          <button className="banner-dismiss" onClick={() => setMode('genie')}>Turn off</button>
        </div>
      )}

      <div className={`genie-input-bar ${isResearch ? 'research-active' : ''}`}>
        <button
          className={`agent-toggle ${isResearch ? 'active' : ''}`}
          onClick={toggleMode}
          disabled={busy}
          title={isResearch ? 'Switch to standard Genie' : 'Enable Agent mode for deep research'}
        >
          <AgentIcon size={18} />
        </button>
        <input
          ref={inputRef}
          type="text"
          placeholder={isResearch ? "Ask a deep research question..." : "Ask a security question..."}
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && ask(input)}
          disabled={busy}
        />
        <button className="send-btn" onClick={() => ask(input)} disabled={busy || !input.trim()}>
          {busy ? <SpinnerIcon /> : <SendIcon />}
        </button>
      </div>
    </div>
  )
}

function AgentIcon({ size = 16 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="icon agent-icon">
      <circle cx="12" cy="12" r="3" />
      <path d="M12 1v4M12 19v4M4.22 4.22l2.83 2.83M16.95 16.95l2.83 2.83M1 12h4M19 12h4M4.22 19.78l2.83-2.83M16.95 7.05l2.83-2.83" />
    </svg>
  )
}

function GenieIcon({ size = 16 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="icon genie-icon-svg">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
    </svg>
  )
}

function SendIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
      <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z" />
    </svg>
  )
}

function SpinnerIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="spinner">
      <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83" />
    </svg>
  )
}

function formatSql(sql: string): string {
  if (!sql) return ''
  // Normalize whitespace, then add newlines before major SQL keywords
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
