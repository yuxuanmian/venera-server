import React, { useEffect, useState, useCallback } from 'react'

const tabs = [
  { key: 'jobs', label: '队列 Jobs' },
  { key: 'mirror', label: '镜像 Mirror' },
  { key: 'tracking', label: '追更诊断 Tracking' },
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
  const [tracking, setTracking] = useState(null)
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
      try {
        setTracking(await api('/tracking/diagnostics'))
      } catch (_) {
        setTracking(null)
      }
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
        {tab === 'tracking' && <TrackingDiagnostics tracking={tracking} />}
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

function TrackingDiagnostics({ tracking }) {
  if (!tracking) return <p className="empty">暂无追更诊断或追更目录未配置</p>
  const data = tracking.data || {}
  const runtime = tracking.runtime || {}
  const exclusions = runtime.exclusions || {}
  const artifacts = data.artifacts || []
  return (
    <div className="tracking-diagnostics">
      <section className="stats">
        <Stat label="Active Revision" value={data.activeRevision || '–'} />
        <Stat label="Generation" value={data.generation ?? '–'} />
        <Stat label="Cloud Clients" value={data.enabledClientCount ?? '–'} />
        <Stat label="Effective Interests" value={data.effectiveInterestCount ?? '–'} />
        <Stat label="Fresh Observations" value={data.freshObservationCount ?? '–'} />
        <Stat label="Stale Observations" value={data.staleObservationCount ?? '–'} />
        <Stat label="Generation Rejections" value={runtime.generationRejections ?? '–'} />
      </section>
      <section className="stats">
        <Stat label="Demands" value={runtime.demandCount ?? '–'} />
        <Stat label="Jobs" value={runtime.jobCount ?? '–'} />
        <Stat label="Pending Jobs" value={runtime.pendingJobCount ?? '–'} />
        <Stat label="Checkpoints" value={runtime.checkpointCount ?? '–'} />
        <Stat label="Scanner Errors" value={runtime.scannerErrorCount ?? '–'} />
        <Stat label="Excluded (stale)" value={exclusions.stale ?? 0} />
        <Stat label="Excluded (old generation)" value={exclusions.oldGeneration ?? 0} />
      </section>
      <h2>Active Artifacts</h2>
      {!artifacts.length ? <p className="empty">暂无 Cloud-capable 制品</p> : (
        <table>
          <thead>
            <tr>
              <th>Source</th><th>File</th><th>Observations</th><th>Fresh</th>
              <th>Stale</th><th>Old Revision</th><th>Old Generation</th>
            </tr>
          </thead>
          <tbody>
            {artifacts.map((item) => (
              <tr key={`${item.artifact?.sourceKey}/${item.artifact?.fileName}`}>
                <td>{item.artifact?.sourceKey || '–'}</td>
                <td>{item.artifact?.fileName || '–'}</td>
                <td>{item.observationCount ?? 0}</td>
                <td>{item.freshObservationCount ?? 0}</td>
                <td>{item.staleObservationCount ?? 0}</td>
                <td>{item.oldRevisionCount ?? 0}</td>
                <td>{item.oldGenerationCount ?? 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}
