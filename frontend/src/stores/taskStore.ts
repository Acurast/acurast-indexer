import { create } from 'zustand'

export interface CurrentWork {
  block?: number
  extrinsic?: string
  event?: string
  epoch?: number
  detail?: string
  phase?: number
  previous?: CurrentWork
}

export interface QueueTypeMetrics {
  pending_count: number
  min_queued_key?: string
  max_queued_key?: string
  throughput_per_sec: number
  items_processed: number
}

export interface DbEntityMetrics {
  entity: string
  rows_per_sec: number
  total_rows: number
}

export interface DbMetrics {
  entities: DbEntityMetrics[]
  total_rows_per_sec: number
  total_rows: number
}

export interface NodeMetrics {
  url: string
  calls_per_sec: number
  total_calls: number
}

export interface BlockChannelMetrics {
  name: string
  pending: number
}

export interface RangeMetric<T> {
  min: T
  max: T
}

export interface QueueMetrics {
  events: QueueTypeMetrics
  extrinsics: QueueTypeMetrics
  epochs: QueueTypeMetrics
  db: DbMetrics
  nodes: NodeMetrics[]
  block_channels: BlockChannelMetrics[]
  block_range?: RangeMetric<number>
  epoch_range?: RangeMetric<number>
  updated_at: number
}

export interface Task {
  id: number
  name: string
  worker_index?: number
  started_at: number
  ended_at?: number
  current_work: CurrentWork
}

export interface MetricsHistoryPoint {
  t: number
  events: number
  extrinsics: number
  epochs: number
  db_total: number
  db_by_entity: Record<string, number>
  nodes_by_url: Record<string, number>
}

const HISTORY_LIMIT = 120 // ~6 minutes at 3s polling

interface TaskState {
  tasks: Task[]
  queueMetrics: QueueMetrics | null
  metricsHistory: MetricsHistoryPoint[]
  isLoading: boolean
  error: string | null
  lastUpdated: number | null

  // Actions
  setTasks: (tasks: Task[]) => void
  setLoading: (loading: boolean) => void
  setError: (error: string | null) => void
  fetchTasks: () => Promise<void>
  fetchQueueMetrics: () => Promise<void>
}

export const useTaskStore = create<TaskState>((set, _get) => ({
  tasks: [],
  queueMetrics: null,
  metricsHistory: [],
  isLoading: false,
  error: null,
  lastUpdated: null,

  setTasks: (tasks) => {
    set({ tasks, lastUpdated: Date.now(), error: null })
  },

  setLoading: (loading) => {
    set({ isLoading: loading })
  },

  setError: (error) => {
    set({ error, isLoading: false })
  },

  fetchTasks: async () => {
    try {
      const res = await fetch('/api/v1/tasks')
      if (!res.ok) {
        throw new Error(`Failed to fetch tasks: ${res.status}`)
      }
      const data = await res.json()
      set({ tasks: data, lastUpdated: Date.now(), error: null, isLoading: false })
    } catch (e) {
      set({ error: e instanceof Error ? e.message : 'Unknown error', isLoading: false })
    }
  },

  fetchQueueMetrics: async () => {
    try {
      const res = await fetch('/api/v1/queue-metrics')
      if (!res.ok) {
        throw new Error(`Failed to fetch queue metrics: ${res.status}`)
      }
      const data: QueueMetrics = await res.json()
      const point: MetricsHistoryPoint = {
        t: data.updated_at,
        events: data.events?.throughput_per_sec ?? 0,
        extrinsics: data.extrinsics?.throughput_per_sec ?? 0,
        epochs: data.epochs?.throughput_per_sec ?? 0,
        db_total: data.db?.total_rows_per_sec ?? 0,
        db_by_entity: Object.fromEntries(
          (data.db?.entities ?? []).map(e => [e.entity, e.rows_per_sec])
        ),
        nodes_by_url: Object.fromEntries(
          (data.nodes ?? []).map(n => [n.url, n.calls_per_sec])
        )
      }
      set(state => ({
        queueMetrics: data,
        metricsHistory: [...state.metricsHistory, point].slice(-HISTORY_LIMIT)
      }))
    } catch (e) {
      // Silently fail for queue metrics - not critical
      console.warn('Failed to fetch queue metrics:', e)
    }
  }
}))

// Polling interval in milliseconds
const POLL_INTERVAL = 3000

let pollInterval: ReturnType<typeof setInterval> | null = null

export function startTaskPolling() {
  if (pollInterval) return

  // Fetch immediately
  useTaskStore.getState().fetchTasks()
  useTaskStore.getState().fetchQueueMetrics()

  // Then poll at interval
  pollInterval = setInterval(() => {
    useTaskStore.getState().fetchTasks()
    useTaskStore.getState().fetchQueueMetrics()
  }, POLL_INTERVAL)
}

export function stopTaskPolling() {
  if (pollInterval) {
    clearInterval(pollInterval)
    pollInterval = null
  }
}
