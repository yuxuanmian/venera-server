import React, { useEffect, useState, useCallback } from 'react'

const tabs = [
  { key: 'jobs', label: '队列 Jobs' },
  { key: 'mirror', label: '镜像 Mirror' },
  { key: 'logs', label: '日志 Logs' },
]

async function api(path, options = {}) {
  const res = await fetch(`/admin/api${path}`, options)
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`)
  return res.json()
}

export default function App() {
  const [tab, setTab] = useState('jobs')
  const [stats, setStats] = useState(null)
  const [jobs, setJobs] = useState([])
  const [mirror, setMirror] = useState([])
  const [logs, setLogs] = useState('')
  const [error, setError] = useState('')
  const [refreshing, setRefreshing] = useState(false)

  const load = useCallback(async () => {
    try {
      const s = await api('/stats')
      setStats(s)
      const j = await api('/jobs')
      setJobs(j.jobs || [])
      const m = await api('/mirror')
      setMirror(m.mirror || [])
      const l = await api('/logs')
      setLogs(l.logs || '')
      setError('')
    } catch (e) {
      setError(String(e.message || e))
    }
  }, [])

  useEffect(() => {
    load()
    const t = setInterval(load, 5000)
    return () => clearInterval(t)
  }, [load])

  const onRefresh = async () => {
    setRefreshing(true)
    try {
      await api('/refresh', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })
      await load()
    } catch (e) {
      setError(String(e.message || e))
    } finally {
      setRefreshing(false)
    }
  }

  return (
    <div className="app">
      <header className="topbar">
        <h1>Venera Admin</h1>
        <div className="actions">
          <button className="btn primary" onClick={onRefresh} disabled={refreshing}>
            {refreshing ? '刷新中…' : '手动全量刷新'}
          </button>
        </div>
      </header>

      {error && <div className="error">{error}</div>}

      <section className="stats">
        <Stat label="用户" value={stats?.users ?? '–'} />
        <Stat label="镜像" value={stats?.mirror ?? '–'} />
        <Stat label="队列(Pending)" value={stats?.pending ?? '–'} />
        <Stat label="结果" value={stats?.results ?? '–'} />
        <Stat label="失败" value={stats?.failed ?? '–'} />
        <Stat label="需重登" value={stats?.needs_resubmit ?? '–'} />
      </section>

      <nav className="tabs">
        {tabs.map((t) => (
          <button key={t.key} className={tab === t.key ? 'tab active' : 'tab'} onClick={() => setTab(t.key)}>
            {t.label}
          </button>
        ))}
      </nav>

      <main>
        {tab === 'jobs' && <JobsTable jobs={jobs} />}
        {tab === 'mirror' && <MirrorTable mirror={mirror} />}
        {tab === 'logs' && <pre className="logs">{logs || '(no logs)'}</pre>}
      </main>
    </div>
  )
}

function Stat({ label, value }) {
  return (
    <div className="stat">
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  )
}

function JobsTable({ jobs }) {
  if (!jobs.length) return <p className="empty">暂无 job</p>
  return (
    <table>
      <thead>
        <tr>
          <th>ID</th><th>用户</th><th>源</th><th>漫画</th><th>状态</th><th>重试</th><th>下次重试</th>
        </tr>
      </thead>
      <tbody>
        {jobs.map((j) => (
          <tr key={j.job_id}>
            <td>{j.job_id}</td>
            <td>{j.user_id}</td>
            <td>{j.source}</td>
            <td title="？">{j.comic_id}</td>
            <td><span className={`badge b-${j.state}`}>{j.state}</span></td>
            <td>{j.attempts}/{j.max_attempts}</td>
            <td>{j.next_retry_at || '–'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function MirrorTable({ mirror }) {
  if (!mirror.length) return <p className="empty">暂无镜像</p>
  return (
    <table>
      <thead>
        <tr>
          <th>用户</th><th>源</th><th>漫画</th><th>Due</th><th>上次检查</th><th>优先级</th>
        </tr>
      </thead>
      <tbody>
        {mirror.map((m, i) => (
          <tr key={i}>
            <td>{m.user_id}</td>
            <td>{m.source}</td>
            <td title={m.comic_id}>{m.comic_id}</td>
            <td>{m.due_at}</td>
            <td>{m.last_check_time || '–'}</td>
            <td>{m.priority}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
