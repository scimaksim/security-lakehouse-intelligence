import { useState } from 'react'
import './App.css'
import Overview from './components/Overview'
import EventTable from './components/EventTable'
import GenieTab from './components/GenieTab'

type Tab = 'overview' | 'authentication' | 'api_activity' | 'dns_activity' | 'vulnerabilities' | 'genie'

function App() {
  const [activeTab, setActiveTab] = useState<Tab>('overview')

  const tabs: { key: Tab; label: string }[] = [
    { key: 'overview', label: 'Overview' },
    { key: 'authentication', label: 'Authentication' },
    { key: 'api_activity', label: 'API Activity' },
    { key: 'dns_activity', label: 'DNS Activity' },
    { key: 'vulnerabilities', label: 'Vulnerabilities' },
    { key: 'genie', label: 'Security AI Assistant' },
  ]

  return (
    <div className="app">
      <header className="header">
        <div className="header-left">
          <h1>Security Lakehouse Intelligence</h1>
          <span className="subtitle">OCSF-Normalized Threat Intelligence</span>
        </div>
        <div className="header-right">
          <span className="live-indicator">LIVE</span>
        </div>
      </header>
      <nav className="tabs">
        {tabs.map(tab => (
          <button
            key={tab.key}
            className={`tab ${activeTab === tab.key ? 'active' : ''}`}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </nav>
      <main className="content">
        {activeTab === 'overview' && <Overview />}
        {activeTab === 'authentication' && (
          <EventTable endpoint="/api/authentication" title="Authentication Events"
            columns={['time','severity','status','user_name','src_ip','city','country','target_service','message']}
            filters={[{key:'severity',options:['Informational','Medium','High','Critical']},{key:'status',options:['Success','Failure']}]} />
        )}
        {activeTab === 'api_activity' && (
          <EventTable endpoint="/api/api_activity" title="API Activity Events"
            columns={['time','severity','status','api_operation','actor_name','src_ip','cloud_region','message']}
            filters={[{key:'severity',options:['Informational','Medium','High','Critical']}]} />
        )}
        {activeTab === 'dns_activity' && (
          <EventTable endpoint="/api/dns_activity" title="DNS Activity Events"
            columns={['time','severity','disposition','query_domain','src_ip','message']}
            filters={[{key:'severity',options:['Informational','High','Critical']}]} />
        )}
        {activeTab === 'vulnerabilities' && (
          <EventTable endpoint="/api/vulnerabilities" title="Vulnerability Findings"
            columns={['time','severity','status','cve_id','cvss_score','hostname','device_ip','vuln_desc']}
            filters={[{key:'severity',options:['High','Critical']},{key:'status',options:['New','In Progress','Resolved','Suppressed']}]} />
        )}
        {activeTab === 'genie' && <GenieTab />}
      </main>
    </div>
  )
}

export default App
