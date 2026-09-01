import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import {
  beginResourceLoad,
  emptyResource,
  filterRecentJobs,
  rejectResourceLoad,
  resolveResourceLoad,
  uniqueValues,
} from './dashboard_state.js'

const REFRESH_INTERVAL_MS = 5000

async function api(path) {
  const response = await fetch(`/admin/api${path}`, { headers: { Accept: 'application/json' } })
  const body = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(body?.error?.message || `读取失败 (${response.status})`)
  return body.data
}

export default function App() {
  const [overview, setOverview] = useState(emptyResource)
  const [filters, setFilters] = useState({ artifactId: '', state: '' })
  const [selectedJobId, setSelectedJobId] = useState('')
  const [details, setDetails] = useState({})
  const overviewSequence = useRef(0)
  const detailSequence = useRef(0)

  const loadOverview = useCallback(async () => {
    const requestId = ++overviewSequence.current
    setOverview((current) => beginResourceLoad(current, requestId))
    try {
      const data = await api('/overview')
      setOverview((current) => resolveResourceLoad(current, requestId, data))
    } catch (cause) {
      setOverview((current) => rejectResourceLoad(current, requestId, cause?.message || cause))
    }
  }, [])

  const loadDetail = useCallback(async (jobId) => {
    if (!jobId) return
    const requestId = ++detailSequence.current
    setDetails((current) => ({
      ...current,
      [jobId]: beginResourceLoad(current[jobId] || emptyResource(), requestId),
    }))
    try {
      const data = await api(`/jobs/${encodeURIComponent(jobId)}`)
      setDetails((current) => ({
        ...current,
        [jobId]: resolveResourceLoad(current[jobId] || emptyResource(), requestId, data),
      }))
    } catch (cause) {
      setDetails((current) => ({
        ...current,
        [jobId]: rejectResourceLoad(current[jobId] || emptyResource(), requestId, cause?.message || cause),
      }))
    }
  }, [])

  useEffect(() => {
    loadOverview()
    const timer = window.setInterval(loadOverview, REFRESH_INTERVAL_MS)
    return () => window.clearInterval(timer)
  }, [loadOverview])

  useEffect(() => {
    if (selectedJobId && !details[selectedJobId]) loadDetail(selectedJobId)
  }, [details, loadDetail, selectedJobId])

  const data = overview.data
  const jobs = data?.recentJobs || []
  const filteredJobs = useMemo(() => filterRecentJobs(jobs, filters), [filters, jobs])
  const artifacts = useMemo(() => uniqueValues(jobs, 'artifactId'), [jobs])
  const states = useMemo(() => uniqueValues(jobs, 'state'), [jobs])
  const selectedDetail = selectedJobId ? details[selectedJobId] || emptyResource() : null

  if (!data && overview.loading) {
    return <PageMessage title="正在读取队列数据" detail="首次加载可能需要片刻。" />
  }

  if (!data && overview.error) {
    return <PageMessage title="核心队列数据不可用" detail={overview.error} action={<button className="button primary" onClick={loadOverview}>再次尝试</button>} />
  }

  return (
    <div className="app-shell">
      <header className="page-header">
        <div>
          <p className="eyebrow">VENERA SERVER V2</p>
          <h1>扫描队列事实视图</h1>
          <p className="subtitle">只读展示 Demand、Job、Attempt、Snapshot Run 与 Source Lane 的已记录事实。</p>
        </div>
        <div className="header-actions">
          <div className="observed-time">
            <span>数据时间</span>
            <strong>{formatTime(data?.observedAt)}</strong>
          </div>
          <button className="button primary" onClick={loadOverview} disabled={overview.loading}>
            {overview.loading ? '刷新中…' : '立即刷新'}
          </button>
        </div>
      </header>

      {overview.error && (
        <div className="notice stale" role="status">
          <strong>当前显示上一次成功数据。</strong>
          <span>{overview.error}</span>
        </div>
      )}

      <section className="summary-grid" aria-label="全局事实摘要">
        <CountCard title="Scan Demand" subtitle="全量当前记录" counts={data?.demandCounts} />
        <CountCard title="当前 Job 队列" subtitle="不含终态历史" counts={data?.currentJobCounts} />
        <CountCard title="近期 Job 结果" subtitle={`最近 ${data?.recentJobLimit || 100} 条范围`} counts={data?.recentOutcomeCounts} />
        <article className="summary-card">
          <div>
            <p className="card-title">暂停的 Source Lane</p>
            <p className="card-subtitle">runtimeState = paused</p>
          </div>
          <strong className="single-count">{data?.pausedLaneCount ?? 0}</strong>
        </article>
      </section>

      <Section title="Source Lane" description="按 artifactId 划分的逻辑调度通道，不代表独立进程、专属 Worker 或物理队列。">
        {data?.lanes?.length ? <LaneTable lanes={data.lanes} /> : <Empty text="尚无 Source Lane、Demand 或 Job 记录。" />}
      </Section>

      <Section
        title="最近 Scan Job"
        description={data?.recentJobsTruncated ? `当前仅展示最近 ${data.recentJobLimit} 条，汇总数量仍覆盖完整当前范围。` : `当前范围包含 ${jobs.length} 条记录。`}
      >
        <div className="filters">
          <label>
            <span>来源</span>
            <select value={filters.artifactId} onChange={(event) => setFilters((current) => ({ ...current, artifactId: event.target.value }))}>
              <option value="">全部来源</option>
              {artifacts.map((artifactId) => <option key={artifactId} value={artifactId}>{artifactId}</option>)}
            </select>
          </label>
          <label>
            <span>Job 状态</span>
            <select value={filters.state} onChange={(event) => setFilters((current) => ({ ...current, state: event.target.value }))}>
              <option value="">全部状态</option>
              {states.map((state) => <option key={state} value={state}>{state}</option>)}
            </select>
          </label>
          {(filters.artifactId || filters.state) && <button className="button" onClick={() => setFilters({ artifactId: '', state: '' })}>清除条件</button>}
        </div>
        {filteredJobs.length ? (
          <JobTable
            jobs={filteredJobs}
            observedAt={data.observedAt}
            selectedJobId={selectedJobId}
            onSelect={setSelectedJobId}
          />
        ) : <Empty text={jobs.length ? '当前筛选条件下没有 Job。' : '尚无 Scan Job 记录。'} />}
      </Section>

      {selectedJobId && (
        <DetailPanel
          jobId={selectedJobId}
          resource={selectedDetail}
          observedAt={data?.observedAt}
          onClose={() => setSelectedJobId('')}
          onReload={() => loadDetail(selectedJobId)}
        />
      )}
    </div>
  )
}

function PageMessage({ title, detail, action }) {
  return (
    <main className="message-shell">
      <section className="message-card">
        <p className="eyebrow">VENERA SERVER V2</p>
        <h1>{title}</h1>
        <p>{detail}</p>
        {action}
      </section>
    </main>
  )
}

function CountCard({ title, subtitle, counts = {} }) {
  return (
    <article className="summary-card count-card">
      <div>
        <p className="card-title">{title}</p>
        <p className="card-subtitle">{subtitle}</p>
      </div>
      <dl>
        {Object.entries(counts || {}).map(([label, value]) => (
          <div key={label}><dt>{label}</dt><dd>{value}</dd></div>
        ))}
      </dl>
    </article>
  )
}

function Section({ title, description, children }) {
  return (
    <section className="section-card">
      <header className="section-header"><div><h2>{title}</h2><p>{description}</p></div></header>
      {children}
    </section>
  )
}

function LaneTable({ lanes }) {
  return (
    <div className="table-wrap">
      <table>
        <thead><tr><th>Artifact</th><th>Runtime state</th><th>并发</th><th>下次可执行</th><th>Demand 分组</th><th>当前 Job</th><th>错误码</th></tr></thead>
        <tbody>
          {lanes.map((lane) => (
            <tr key={lane.artifactId}>
              <td><code>{lane.artifactId}</code></td>
              <td><RawValue value={lane.runtimeState} /></td>
              <td>{lane.effectiveConcurrency == null ? '尚未记录' : `${lane.effectiveConcurrency} / ${lane.targetConcurrency}`}</td>
              <td>{formatTime(lane.nextEligibleAt)}</td>
              <td><GroupFacts items={lane.demands} /></td>
              <td><GroupFacts items={lane.jobs} /></td>
              <td><code>{lane.lastErrorCode || '尚未产生'}</code></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function GroupFacts({ items = [] }) {
  if (!items.length) return <span className="muted">无记录</span>
  return <div className="fact-list">{items.map((item) => <span key={`${item.kind || ''}-${item.state}`}>{item.kind ? `${item.kind} · ` : ''}{item.state}: <strong>{item.count}</strong></span>)}</div>
}

function JobTable({ jobs, observedAt, selectedJobId, onSelect }) {
  return (
    <div className="table-wrap">
      <table className="job-table">
        <thead><tr><th>Job</th><th>来源</th><th>类型</th><th>状态</th><th>创建时间</th><th>执行时间</th><th>最近结果</th><th>尝试</th></tr></thead>
        <tbody>
          {jobs.map((job) => (
            <tr key={job.jobId} className={selectedJobId === job.jobId ? 'selected' : ''}>
              <td><button className="job-link" onClick={() => onSelect(job.jobId)}>{job.jobId}</button></td>
              <td><code>{job.artifactId}</code></td>
              <td><RawValue value={job.jobKind} /></td>
              <td><RawValue value={job.state} /></td>
              <td>{formatTime(job.createdAt)}</td>
              <td>{formatDuration(job.startedAt, job.finishedAt, observedAt)}</td>
              <td>{job.lastOutcome || job.lastErrorCode || '尚未产生'}</td>
              <td>{job.attemptCount}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function DetailPanel({ jobId, resource, observedAt, onClose, onReload }) {
  const detail = resource?.data
  return (
    <div className="detail-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <aside className="detail-panel" aria-label={`Job ${jobId} 详情`}>
        <header className="detail-header">
          <div><p className="eyebrow">SCAN JOB DETAIL</p><h2>{jobId}</h2></div>
          <div className="detail-actions">
            <button className="button" onClick={onReload} disabled={resource?.loading}>{resource?.loading ? '刷新中…' : '刷新详情'}</button>
            <button className="icon-button" onClick={onClose} aria-label="关闭详情">×</button>
          </div>
        </header>
        {resource?.error && <div className={`notice ${detail ? 'stale' : 'error'}`} role="status"><strong>{detail ? '当前显示上一次成功详情。' : '详情读取失败。'}</strong><span>{resource.error}</span></div>}
        {!detail && resource?.loading && <Empty text="正在读取 Job 详情…" />}
        {!detail && !resource?.loading && !resource?.error && <Empty text="尚未读取详情。" />}
        {detail && <DetailContent detail={detail} observedAt={observedAt} />}
      </aside>
    </div>
  )
}

function DetailContent({ detail, observedAt }) {
  const { job, demand, attempts, snapshotRun } = detail
  return (
    <div className="detail-content">
      <DetailSection title="Scan Job">
        <FactGrid facts={[
          ['状态', job.state], ['类型', job.jobKind], ['来源', job.artifactId], ['Demand', job.demandId],
          ['创建时间', formatTime(job.createdAt)], ['更新时间', formatTime(job.updatedAt)],
          ['执行耗时', formatDuration(job.startedAt, job.finishedAt, observedAt)], ['结果摘要', job.resultDigest || '尚未产生'],
        ]} />
      </DetailSection>
      <DetailSection title="Scan Demand">
        <FactGrid facts={[
          ['状态', demand.state], ['类型', demand.demandKind], ['优先级', demand.priorityClass],
          ['到期时间', formatTime(demand.dueAt)], ['最早时间', formatTime(demand.oldestAt)],
          ['下次可执行', formatTime(demand.nextEligibleAt)], ['错误码', demand.lastErrorCode || '尚未产生'],
        ]} />
      </DetailSection>
      <DetailSection title="Snapshot Run">
        {snapshotRun ? <FactGrid facts={[
          ['Run', snapshotRun.snapshotRunId], ['状态', snapshotRun.state], ['Generation', snapshotRun.runGeneration],
          ['条目数', snapshotRun.itemCount], ['开始时间', formatTime(snapshotRun.startedAt)],
          ['完成时间', formatTime(snapshotRun.completedAt)], ['失败码', snapshotRun.failureCode || '尚未产生'],
        ]} /> : <Empty text="此 Job 尚未关联 Snapshot Run。" compact />}
      </DetailSection>
      <DetailSection title={`Scan Attempt（显示 ${attempts.items.length} / 共 ${attempts.total}）`}>
        {attempts.items.length ? (
          <div className="attempt-list">
            {attempts.items.map((attempt) => (
              <article key={attempt.attemptNo}>
                <div><strong>#{attempt.attemptNo}</strong><RawValue value={attempt.outcome} /></div>
                <p>{formatTime(attempt.startedAt)} → {formatTime(attempt.finishedAt)}</p>
                <code>{attempt.errorCode || '无错误码'}</code>
              </article>
            ))}
          </div>
        ) : <Empty text="尚无 Attempt 记录。" compact />}
        {attempts.total > attempts.items.length && <p className="range-note">另有 {attempts.total - attempts.items.length} 次更早记录未展示。</p>}
      </DetailSection>
    </div>
  )
}

function DetailSection({ title, children }) {
  return <section className="detail-section"><h3>{title}</h3>{children}</section>
}

function FactGrid({ facts }) {
  return <dl className="fact-grid">{facts.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value ?? '尚未产生'}</dd></div>)}</dl>
}

function RawValue({ value }) {
  return <span className="raw-value">{value ?? '尚未记录'}</span>
}

function Empty({ text, compact = false }) {
  return <p className={compact ? 'empty compact' : 'empty'}>{text}</p>
}

function formatTime(value) {
  if (!value) return '尚未产生'
  const time = new Date(value)
  if (Number.isNaN(time.getTime())) return String(value)
  return new Intl.DateTimeFormat(undefined, { dateStyle: 'short', timeStyle: 'medium' }).format(time)
}

function formatDuration(startedAt, finishedAt, observedAt) {
  if (!startedAt) return '尚未产生'
  const start = new Date(startedAt).getTime()
  const end = new Date(finishedAt || observedAt).getTime()
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return '尚未产生'
  const seconds = Math.floor((end - start) / 1000)
  if (seconds < 60) return `${seconds} 秒${finishedAt ? '' : '（进行中）'}`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes} 分 ${seconds % 60} 秒${finishedAt ? '' : '（进行中）'}`
  return `${Math.floor(minutes / 60)} 小时 ${minutes % 60} 分${finishedAt ? '' : '（进行中）'}`
}
