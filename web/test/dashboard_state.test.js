import test from 'node:test'
import assert from 'node:assert/strict'

import {
  beginResourceLoad,
  emptyResource,
  filterRecentJobs,
  rejectResourceLoad,
  resolveResourceLoad,
  uniqueValues,
} from '../src/dashboard_state.js'

test('filters jobs by raw artifact and state while preserving unknown values', () => {
  const jobs = [
    { jobId: '1', artifactId: 'manwa', state: 'ready', jobKind: 'known' },
    { jobId: '2', artifactId: 'future-source', state: 'future-state', jobKind: 'future-kind' },
  ]
  assert.deepEqual(filterRecentJobs(jobs, { artifactId: 'future-source', state: 'future-state' }), [jobs[1]])
  assert.deepEqual(filterRecentJobs(jobs, {}), jobs)
  assert.deepEqual(uniqueValues(jobs, 'state'), ['future-state', 'ready'])
})

test('failure retains last known good data and marks it stale', () => {
  let state = beginResourceLoad(emptyResource(), 1)
  state = resolveResourceLoad(state, 1, { observedAt: 'now' })
  state = beginResourceLoad(state, 2)
  state = rejectResourceLoad(state, 2, 'offline')
  assert.deepEqual(state.data, { observedAt: 'now' })
  assert.equal(state.error, 'offline')
  assert.equal(state.stale, true)
  assert.equal(state.loading, false)
})

test('initial failure is distinct from stale data', () => {
  let state = beginResourceLoad(emptyResource(), 1)
  state = rejectResourceLoad(state, 1, 'unavailable')
  assert.equal(state.data, null)
  assert.equal(state.stale, false)
})

test('late success and failure cannot replace the newest request', () => {
  let state = beginResourceLoad(emptyResource(), 1)
  state = beginResourceLoad(state, 2)
  assert.equal(resolveResourceLoad(state, 1, { value: 'old' }), state)
  assert.equal(rejectResourceLoad(state, 1, 'old error'), state)
  state = resolveResourceLoad(state, 2, { value: 'new' })
  assert.deepEqual(state.data, { value: 'new' })
})

test('detail resource failures do not mutate the core resource', () => {
  const core = resolveResourceLoad(beginResourceLoad(emptyResource(), 1), 1, { lanes: [] })
  const detail = rejectResourceLoad(beginResourceLoad(emptyResource(), 7), 7, 'detail failed')
  assert.deepEqual(core.data, { lanes: [] })
  assert.equal(core.error, '')
  assert.equal(detail.error, 'detail failed')
})
