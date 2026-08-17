import { useMemo } from 'react'
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { DbEntityMetrics, MetricsHistoryPoint, NodeMetrics } from '@/stores/taskStore'

const ENTITY_COLORS: Record<string, string> = {
  block: '#3b82f6',
  extrinsic: '#10b981',
  event: '#f59e0b',
  spec_version: '#94a3b8',
  job: '#a855f7',
  extrinsic_address: '#06b6d4',
  storage_snapshot: '#ec4899',
  manager: '#84cc16',
  commitment: '#f97316',
  deployment: '#eab308',
  epoch: '#8b5cf6'
}

const NODE_COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#a855f7', '#06b6d4']

function shortenUrl(url: string): string {
  try {
    const u = new URL(url.startsWith('ws') || url.startsWith('http') ? url : `wss://${url}`)
    return u.host
  } catch {
    return url
  }
}

function formatRate(rate: number): string {
  if (rate >= 1000) return `${(rate / 1000).toFixed(1)}k/s`
  if (rate >= 10) return `${rate.toFixed(0)}/s`
  return `${rate.toFixed(1)}/s`
}

function MiniSparkline({
  data,
  dataKeys,
  colors
}: {
  data: Array<Record<string, number>>
  dataKeys: string[]
  colors: Record<string, string>
}) {
  if (data.length < 2) {
    return <div className="text-[10px] text-gray-600 italic">Collecting samples...</div>
  }
  return (
    <div style={{ height: 80 }}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
          <XAxis dataKey="t" hide />
          <YAxis hide domain={[0, 'auto']} />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1f2937',
              border: '1px solid #374151',
              borderRadius: 6,
              fontSize: 11,
              color: '#e5e7eb'
            }}
            labelFormatter={() => ''}
            formatter={(value, name) => [`${Number(value).toFixed(1)}/s`, String(name)]}
          />
          {dataKeys.map(key => (
            <Line
              key={key}
              type="monotone"
              dataKey={key}
              stroke={colors[key] ?? '#9ca3af'}
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

export function DbMetricsPanel({
  entities,
  totalPerSec,
  totalRows,
  history
}: {
  entities: DbEntityMetrics[]
  totalPerSec: number
  totalRows: number
  history: MetricsHistoryPoint[]
}) {
  const chartData = useMemo(() => {
    const allEntityKeys = Array.from(
      new Set(history.flatMap(p => Object.keys(p.db_by_entity)))
    )
    return history.map(p => {
      const row: Record<string, number> = { t: p.t }
      allEntityKeys.forEach(k => {
        row[k] = p.db_by_entity[k] ?? 0
      })
      return row
    })
  }, [history])

  const allEntityKeys = useMemo(
    () => Array.from(new Set(history.flatMap(p => Object.keys(p.db_by_entity)))),
    [history]
  )

  return (
    <div className="bg-gray-800 rounded-lg p-2 border-l-4 border-pink-500">
      <div className="flex justify-between items-baseline">
        <div className="font-medium text-gray-200 text-xs">DB Inserts</div>
        <div className="text-xs text-gray-400">
          <span className="text-pink-300 font-medium tabular-nums">
            {formatRate(totalPerSec)}
          </span>{' '}
          · {totalRows.toLocaleString()} total
        </div>
      </div>
      <MiniSparkline data={chartData} dataKeys={allEntityKeys} colors={ENTITY_COLORS} />
      <div className="grid grid-cols-2 gap-x-2 gap-y-0.5 mt-1 text-[10px]">
        {entities.length === 0 ? (
          <div className="text-gray-600 italic col-span-2">No inserts yet</div>
        ) : (
          entities
            .filter(e => e.rows_per_sec > 0 || e.total_rows > 0)
            .sort((a, b) => b.rows_per_sec - a.rows_per_sec)
            .map(e => (
              <div key={e.entity} className="flex justify-between">
                <span
                  className="truncate"
                  style={{ color: ENTITY_COLORS[e.entity] ?? '#9ca3af' }}
                >
                  {e.entity}
                </span>
                <span className="text-gray-400 tabular-nums">
                  {formatRate(e.rows_per_sec)}
                </span>
              </div>
            ))
        )}
      </div>
    </div>
  )
}

export function NodeMetricsPanel({
  nodes,
  history
}: {
  nodes: NodeMetrics[]
  history: MetricsHistoryPoint[]
}) {
  const allNodeUrls = useMemo(
    () => Array.from(new Set(history.flatMap(p => Object.keys(p.nodes_by_url)))),
    [history]
  )

  const colors = useMemo(() => {
    const m: Record<string, string> = {}
    allNodeUrls.forEach((url, i) => {
      m[url] = NODE_COLORS[i % NODE_COLORS.length]
    })
    return m
  }, [allNodeUrls])

  const chartData = useMemo(() => {
    return history.map(p => {
      const row: Record<string, number> = { t: p.t }
      allNodeUrls.forEach(url => {
        row[url] = p.nodes_by_url[url] ?? 0
      })
      return row
    })
  }, [history, allNodeUrls])

  const totalPerSec = nodes.reduce((s, n) => s + n.calls_per_sec, 0)
  const totalCalls = nodes.reduce((s, n) => s + n.total_calls, 0)

  return (
    <div className="bg-gray-800 rounded-lg p-2 border-l-4 border-cyan-500">
      <div className="flex justify-between items-baseline">
        <div className="font-medium text-gray-200 text-xs">Archive Node RPC</div>
        <div className="text-xs text-gray-400">
          <span className="text-cyan-300 font-medium tabular-nums">
            {formatRate(totalPerSec)}
          </span>{' '}
          · {totalCalls.toLocaleString()} total
        </div>
      </div>
      <MiniSparkline data={chartData} dataKeys={allNodeUrls} colors={colors} />
      <div className="space-y-0.5 mt-1 text-[10px]">
        {nodes.length === 0 ? (
          <div className="text-gray-600 italic">No RPC activity yet</div>
        ) : (
          nodes.map(n => (
            <div key={n.url} className="flex justify-between gap-2">
              <span className="truncate" style={{ color: colors[n.url] ?? '#9ca3af' }}>
                {shortenUrl(n.url)}
              </span>
              <span className="text-gray-400 tabular-nums shrink-0">
                {formatRate(n.calls_per_sec)}
              </span>
            </div>
          ))
        )}
      </div>
    </div>
  )
}
