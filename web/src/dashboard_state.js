export const emptyResource = () => ({
  data: null,
  error: '',
  loading: false,
  stale: false,
  requestId: 0,
})

export function beginResourceLoad(resource, requestId) {
  return {
    ...resource,
    error: '',
    loading: true,
    requestId,
  }
}

export function resolveResourceLoad(resource, requestId, data) {
  if (resource.requestId !== requestId) return resource
  return {
    ...resource,
    data,
    error: '',
    loading: false,
    stale: false,
  }
}

export function rejectResourceLoad(resource, requestId, error) {
  if (resource.requestId !== requestId) return resource
  return {
    ...resource,
    error: String(error || '读取失败'),
    loading: false,
    stale: resource.data !== null,
  }
}

export function filterRecentJobs(jobs, filters) {
  const artifactId = filters?.artifactId || ''
  const state = filters?.state || ''
  return (jobs || []).filter((job) => {
    return (!artifactId || job.artifactId === artifactId) && (!state || job.state === state)
  })
}

export function uniqueValues(items, key) {
  return [...new Set((items || []).map((item) => item[key]).filter(Boolean))].sort()
}
