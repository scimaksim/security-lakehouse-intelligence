import { useState, useEffect } from 'react'
import { XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, AreaChart, Area } from 'recharts'

interface OverviewData {
  counts: { table_name: string; cnt: number; high_crit: number }[]
  severity: { severity: string; cnt: number }[]
  timeline: { day: string; cnt: number; high_crit: number }[]
}

const SEVERITY_COLORS: Record<string, string> = {
  Critical: '#ff4444',
  High: '#ff8800',
  Medium: '#ffcc00',
  Informational: '#44aaff',
}

const TABLE_LABELS: Record<string, string> = {
  authentication: 'Auth Events',
  api_activity: 'API Activity',
  dns_activity: 'DNS Activity',
  vulnerability_finding: 'Vulnerabilities',
}

export default function Overview() {
  const [data, setData] = useState<OverviewData | null>(null)
  const [threats, setThreats] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetch('/api/overview').then(r => r.json()),
      fetch('/api/top_threats').then(r => r.json()),
    ]).then(([overview, threats]) => {
      setData(overview)
      setThreats(threats)
      setLoading(false)
    })
  }, [])

  if (loading || !data) return <div className="loading">Loading security data...</div>

  const totalEvents = data.counts.reduce((s, c) => s + c.cnt, 0)
  const totalHighCrit = data.counts.reduce((s, c) => s + c.high_crit, 0)

  return (
    <div className="overview">
      <div className="stats-row">
        <div className="stat-card">
          <div className="stat-value">{totalEvents.toLocaleString()}</div>
          <div className="stat-label">Total Events</div>
        </div>
        <div className="stat-card critical">
          <div className="stat-value">{totalHighCrit.toLocaleString()}</div>
          <div className="stat-label">High/Critical</div>
        </div>
        {data.counts.map(c => (
          <div className="stat-card" key={c.table_name}>
            <div className="stat-value">{c.cnt.toLocaleString()}</div>
            <div className="stat-label">{TABLE_LABELS[c.table_name] || c.table_name}</div>
          </div>
        ))}
      </div>

      <div className="charts-row">
        <div className="chart-card wide">
          <h3>Event Timeline (30 Days)</h3>
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={data.timeline}>
              <XAxis dataKey="day" tickFormatter={d => new Date(d).toLocaleDateString('en-US', {month:'short',day:'numeric'})} stroke="#888" fontSize={11} />
              <YAxis stroke="#888" fontSize={11} />
              <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #333',borderRadius:8}} labelFormatter={d => new Date(d).toLocaleDateString()} />
              <Area type="monotone" dataKey="cnt" stroke="#44aaff" fill="#44aaff" fillOpacity={0.15} name="All Events" />
              <Area type="monotone" dataKey="high_crit" stroke="#ff4444" fill="#ff4444" fillOpacity={0.3} name="High/Critical" />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        <div className="chart-card">
          <h3>Severity Distribution</h3>
          <ResponsiveContainer width="100%" height={250}>
            <PieChart>
              <Pie data={data.severity} dataKey="cnt" nameKey="severity" cx="50%" cy="50%" innerRadius={50} outerRadius={90} paddingAngle={2} label={({severity,cnt}: any)=>`${severity}: ${cnt.toLocaleString()}`}>
                {data.severity.map((entry) => (
                  <Cell key={entry.severity} fill={SEVERITY_COLORS[entry.severity] || '#666'} />
                ))}
              </Pie>
              <Tooltip contentStyle={{background:'#1a1a2e',border:'1px solid #333',borderRadius:8}} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {threats && (
        <div className="threats-section">
          <div className="threat-card">
            <h3>Top Threat Source IPs</h3>
            <table className="data-table">
              <thead><tr><th>IP Address</th><th>Failed Attempts</th><th>High/Crit</th></tr></thead>
              <tbody>
                {threats.top_ips?.map((ip: any, i: number) => (
                  <tr key={i}><td className="mono">{ip.ip}</td><td>{ip.cnt}</td><td className="critical-text">{ip.high_crit}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="threat-card">
            <h3>Top CVEs</h3>
            <table className="data-table">
              <thead><tr><th>CVE</th><th>CVSS</th><th>Hosts</th><th>Unresolved</th></tr></thead>
              <tbody>
                {threats.top_cves?.map((cve: any, i: number) => (
                  <tr key={i}><td className="mono">{cve.cve_id}</td><td className={cve.cvss >= 9 ? 'critical-text' : ''}>{cve.cvss}</td><td>{cve.affected_hosts}</td><td className="critical-text">{cve.unresolved}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="threat-card">
            <h3>Suspicious DNS</h3>
            <table className="data-table">
              <thead><tr><th>Domain</th><th>Queries</th><th>Severity</th></tr></thead>
              <tbody>
                {threats.suspicious_dns?.map((dns: any, i: number) => (
                  <tr key={i}><td className="mono truncate">{dns.domain}</td><td>{dns.queries}</td><td><span className={`badge ${dns.severity?.toLowerCase()}`}>{dns.severity}</span></td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
