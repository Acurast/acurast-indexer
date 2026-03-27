import { useEffect, useState } from 'react'
import { ChevronUp, ChevronDown, Activity, Loader2, Check } from 'lucide-react'
import { useTaskStore, startTaskPolling, stopTaskPolling, type Task, type CurrentWork, type QueueTypeMetrics } from '@/stores/taskStore'

function formatElapsedTime(startTime: number): string {
  const elapsed = Date.now() - startTime
  if (elapsed < 1000) return `${elapsed}ms`
  if (elapsed < 60000) return `${(elapsed / 1000).toFixed(1)}s`
  if (elapsed < 3600000) return `${Math.floor(elapsed / 60000)}m ${Math.floor((elapsed % 60000) / 1000)}s`
  return `${Math.floor(elapsed / 3600000)}h ${Math.floor((elapsed % 3600000) / 60000)}m`
}

function formatWorkParts(work: CurrentWork): string | null {
  const parts: string[] = []
  if (work.epoch) parts.push(`Epoch ${work.epoch}`)
  if (work.block) parts.push(`Block ${work.block}`)
  if (work.extrinsic) {
    const phaseStr = work.phase !== undefined ? ` [phase ${work.phase}]` : ''
    parts.push(`Ext ${work.extrinsic}${phaseStr}`)
  }
  if (work.event) {
    const phaseStr = work.phase !== undefined ? ` [phase ${work.phase}]` : ''
    parts.push(`Event ${work.event}${phaseStr}`)
  }
  if (work.detail) parts.push(work.detail)
  return parts.length > 0 ? parts.join(' | ') : null
}

function formatCurrentWork(work: CurrentWork): { primary: string | null; detail: string | null; previous: string | null } {
  const parts: string[] = []
  if (work.epoch) parts.push(`Epoch ${work.epoch}`)
  if (work.block) parts.push(`Block ${work.block}`)
  if (work.extrinsic) {
    const phaseStr = work.phase !== undefined ? ` [phase ${work.phase}]` : ''
    parts.push(`Ext ${work.extrinsic}${phaseStr}`)
  }
  if (work.event) {
    const phaseStr = work.phase !== undefined ? ` [phase ${work.phase}]` : ''
    parts.push(`Event ${work.event}${phaseStr}`)
  }

  return {
    primary: parts.length > 0 ? parts.join(' | ') : null,
    detail: work.detail ?? null,
    previous: work.previous ? formatWorkParts(work.previous) : null
  }
}

// Calculate pressure as time-to-clear in minutes, normalized to 0-1
function calculatePressure(metrics: QueueTypeMetrics | undefined): number {
  if (!metrics) return 0
  const pending = metrics.pending_count || 0
  const throughput = metrics.throughput_per_sec || 0.1
  // Time to clear queue in minutes
  const clearTimeMinutes = pending / throughput / 60
  // Normalize: 0 = healthy (<5 min), 1 = critical (>30 min)
  return Math.min(clearTimeMinutes / 30, 1)
}

function pressureColor(pressure: number): string {
  if (pressure < 0.2) return 'bg-green-500'
  if (pressure < 0.5) return 'bg-yellow-500'
  return 'bg-red-500'
}

function QueueCard({ name, metrics, borderColor }: { name: string; metrics: QueueTypeMetrics | undefined; borderColor: string }) {
  const pressure = calculatePressure(metrics)

  return (
    <div className={`bg-gray-800 rounded-lg p-2 border-l-4 ${borderColor}`}>
      <div className="font-medium text-gray-200 text-xs">{name}</div>

      {/* Pending count with pressure bar */}
      <div className="mt-1">
        <div className="flex justify-between text-xs text-gray-400">
          <span>Pending:</span>
          <span>{metrics?.pending_count?.toLocaleString() ?? '-'}</span>
        </div>
        <div className="h-1 bg-gray-700 rounded mt-1">
          <div
            className={`h-full rounded ${pressureColor(pressure)}`}
            style={{ width: `${Math.min(pressure * 100, 100)}%` }}
          />
        </div>
      </div>

      {/* Throughput */}
      <div className="flex justify-between text-xs text-gray-500 mt-1">
        <span>Throughput:</span>
        <span>{metrics?.throughput_per_sec?.toFixed(1) ?? '-'}/s</span>
      </div>

      {/* Block range */}
      <div className="text-gray-600 text-[10px] mt-1 truncate">
        Range: {metrics?.min_queued_key ?? '?'} - {metrics?.max_queued_key ?? '?'}
      </div>
    </div>
  )
}

function TaskItem({ task, showTimer = true }: { task: Task; showTimer?: boolean }) {
  const [elapsed, setElapsed] = useState(formatElapsedTime(task.started_at))
  const isEnded = task.ended_at != null

  // Update elapsed time every second (only for running tasks)
  useEffect(() => {
    if (isEnded || !showTimer) return

    const interval = setInterval(() => {
      setElapsed(formatElapsedTime(task.started_at))
    }, 1000)

    return () => clearInterval(interval)
  }, [task.started_at, isEnded, showTimer])

  const { primary, detail, previous } = formatCurrentWork(task.current_work)
  const displayName = task.worker_index !== undefined
    ? `${task.name} #${task.worker_index}`
    : task.name

  return (
    <div className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm ${isEnded ? 'bg-gray-800/50' : 'bg-gray-800'}`}>
      {isEnded ? (
        <Check className="h-4 w-4 text-green-500 shrink-0" />
      ) : (
        <Loader2 className="h-4 w-4 animate-spin text-blue-400 shrink-0" />
      )}
      <div className="flex-1 min-w-0">
        <div className={`font-medium truncate ${isEnded ? 'text-gray-400' : ''}`}>{displayName}</div>
        {primary && (
          <div className="text-xs text-gray-400 truncate">{primary}</div>
        )}
        {detail && (
          <div className="text-xs text-gray-500 truncate">{detail}</div>
        )}
        {previous && (
          <div className="text-xs text-gray-600 truncate">Previous: {previous}</div>
        )}
      </div>
      {showTimer && (
        <div className="text-xs text-gray-500 tabular-nums shrink-0">{elapsed}</div>
      )}
    </div>
  )
}

export function TaskMonitor() {
  const { tasks, error, queueMetrics } = useTaskStore()
  const [isExpanded, setIsExpanded] = useState(false)

  // Start polling on mount
  useEffect(() => {
    startTaskPolling()
    return () => stopTaskPolling()
  }, [])

  // Sort tasks by started_at (newest first)
  const sortedTasks = [...tasks].sort((a, b) => b.started_at - a.started_at)
  const phaseWorkerTasks = sortedTasks.filter(t => t.name.startsWith('Phase worker'))
  const otherTasks = sortedTasks.filter(t => !t.name.startsWith('Phase worker'))
  const runningTasks = tasks.filter(t => t.ended_at == null)
  const finalizedTask = tasks.find(t => t.name.startsWith('Queue finalized'))

  // Don't show anything if no tasks and no error
  if (tasks.length === 0 && !error) return null

  return (
    <div className="fixed bottom-0 left-0 right-0 z-50">
      {/* Expanded panel */}
      {isExpanded && (sortedTasks.length > 0 || queueMetrics) && (
        <div className="bg-gray-900 border-t border-gray-700">
          <div className="max-w-7xl mx-auto px-4 py-2">
            {/* Queue Metrics Panel */}
            {queueMetrics && (
              <div className="border-b border-gray-700 pb-3 mb-3">
                <div className="grid grid-cols-3 gap-4">
                  <QueueCard
                    name="Events"
                    metrics={queueMetrics.events}
                    borderColor="border-blue-500"
                  />
                  <QueueCard
                    name="Extrinsics"
                    metrics={queueMetrics.extrinsics}
                    borderColor="border-green-500"
                  />
                  <QueueCard
                    name="Epochs"
                    metrics={queueMetrics.epochs}
                    borderColor="border-purple-500"
                  />
                </div>
              </div>
            )}

            {/* Task lists */}
            <div className="grid grid-cols-2 gap-4 max-h-80">
              {/* Left column: Other tasks */}
              <div className="space-y-1 overflow-y-auto max-h-80 pr-2">
                {otherTasks.map(task => (
                  <TaskItem key={task.id} task={task} showTimer={false} />
                ))}
              </div>
              {/* Right column: Phase worker tasks */}
              <div className="space-y-1 overflow-y-auto max-h-80 pr-2">
                {phaseWorkerTasks.map(task => (
                  <TaskItem key={task.id} task={task} showTimer={false} />
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Status bar */}
      <div className="bg-gray-800 border-t border-gray-700">
        <div className="max-w-7xl mx-auto px-4 py-2 flex items-center justify-between">
          <button
            onClick={() => setIsExpanded(!isExpanded)}
            className="flex items-center gap-2 text-sm hover:text-white transition-colors"
          >
            <Activity className="h-4 w-4 text-blue-400" />
            <span className="text-gray-300">
              {runningTasks.length > 0 ? (
                <>
                  <span className="font-medium text-white">{runningTasks.length}</span>
                  {' '}task{runningTasks.length !== 1 ? 's' : ''} running of <span className="font-medium text-white">{tasks.length}</span> total
                </>
              ) : error ? (
                <span className="text-yellow-500">{error}</span>
              ) : (
                <span className="text-gray-500">No active tasks</span>
              )}
            </span>
            {sortedTasks.length > 0 && (
              isExpanded ? (
                <ChevronDown className="h-4 w-4 text-gray-500" />
              ) : (
                <ChevronUp className="h-4 w-4 text-gray-500" />
              )
            )}
          </button>

          <div className="flex items-center gap-4">
            {/* Queue finalized task preview (when collapsed) */}
            {!isExpanded && finalizedTask && (() => {
              const { primary, detail } = formatCurrentWork(finalizedTask.current_work)
              return (
                <div className="flex items-center gap-2 text-sm text-gray-400 max-w-md truncate">
                  <Loader2 className="h-3 w-3 animate-spin text-blue-400 shrink-0" />
                  <span className="truncate">{finalizedTask.name}</span>
                  {primary && (
                    <span className="text-gray-500 truncate">- {primary}</span>
                  )}
                  {detail && (
                    <span className="text-gray-600 truncate">({detail})</span>
                  )}
                </div>
              )
            })()}
          </div>
        </div>
      </div>
    </div>
  )
}
