import { useMemo, useState } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Label
} from 'recharts'

interface EpochTotalsItem {
  epoch: number
  block_number: number
  block_time: string
  total_vesting: string
  total_token_claim: string
  total_self_staked: string
  total_delegated: string
}

interface EpochTotalsGraphProps {
  data: EpochTotalsItem[]
}

// Totals are raw on-chain integer units serialized as strings (NUMERIC(38,0)),
// far beyond Number.MAX_SAFE_INTEGER. Parse to f64 for plotting only — the
// low-order precision loss is irrelevant at chart resolution.
const SERIES = [
  { key: 'self_staked', label: 'Self-staked', color: '#3b82f6' },
  { key: 'delegated', label: 'Delegated', color: '#a855f7' },
  { key: 'vesting', label: 'Vesting', color: '#22c55e' },
  { key: 'token_claim', label: 'Token claim', color: '#f59e0b' }
] as const

const compact = new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 2 })

export function EpochTotalsGraph({ data }: EpochTotalsGraphProps) {
  // All series visible by default; toggled via the checkboxes below.
  const [selected, setSelected] = useState<Set<string>>(
    () => new Set(SERIES.map((s) => s.key))
  )

  const chartData = useMemo(() => {
    return [...data]
      .sort((a, b) => a.epoch - b.epoch)
      .map((d) => ({
        epoch: d.epoch,
        self_staked: Number(d.total_self_staked),
        delegated: Number(d.total_delegated),
        vesting: Number(d.total_vesting),
        token_claim: Number(d.total_token_claim)
      }))
  }, [data])

  const xDomain = useMemo(() => {
    if (chartData.length === 0) return [0, 1]
    const minEpoch = chartData[0].epoch
    const maxEpoch = chartData[chartData.length - 1].epoch
    const padding = Math.max(1, Math.floor((maxEpoch - minEpoch) * 0.02))
    return [minEpoch - padding, maxEpoch + padding]
  }, [chartData])

  const toggle = (key: string) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  if (chartData.length === 0) {
    return (
      <div className="text-center text-gray-500 py-4">
        No data to display
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="text-sm text-gray-400 text-center">
        Network-wide totals per epoch ({chartData[0]?.epoch} - {chartData[chartData.length - 1]?.epoch})
      </div>

      {/* Series toggles */}
      <div className="flex flex-wrap gap-2 justify-center">
        {SERIES.map((s) => {
          const isSelected = selected.has(s.key)
          const color = isSelected ? s.color : '#6b7280'
          return (
            <label
              key={s.key}
              className={`
                flex items-center gap-2 px-3 py-1.5 rounded-md cursor-pointer text-xs transition-colors
                ${isSelected
                  ? 'bg-gray-700 border border-gray-500'
                  : 'bg-gray-800 border border-gray-700 hover:border-gray-600'
                }
              `}
              style={{ borderLeftColor: color, borderLeftWidth: '3px' }}
            >
              <input
                type="checkbox"
                checked={isSelected}
                onChange={() => toggle(s.key)}
                className="sr-only"
              />
              <span
                className="w-3 h-3 rounded-sm border shrink-0"
                style={{ backgroundColor: isSelected ? color : 'transparent', borderColor: color }}
              >
                {isSelected && (
                  <svg className="w-3 h-3 text-white" viewBox="0 0 12 12" fill="none">
                    <path d="M2 6L5 9L10 3" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                )}
              </span>
              <span className="text-gray-300">{s.label}</span>
            </label>
          )
        })}
      </div>

      <div className="bg-gray-900/50 rounded-lg p-3">
        <h4 className="text-sm font-medium mb-2 text-blue-400">
          Staked / Delegated / Vesting / Token-claim (raw units)
        </h4>
        {selected.size === 0 ? (
          <div className="text-center text-gray-500 py-8 text-sm">
            Select one or more series above to visualize
          </div>
        ) : (
          <div style={{ height: '320px' }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart
                data={chartData}
                margin={{ top: 10, right: 30, left: 20, bottom: 25 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis
                  dataKey="epoch"
                  type="number"
                  domain={xDomain}
                  stroke="#9ca3af"
                  tick={{ fill: '#9ca3af', fontSize: 10 }}
                  tickFormatter={(value) => String(Math.round(value))}
                >
                  <Label value="Epoch" position="bottom" offset={-5} fill="#9ca3af" fontSize={12} />
                </XAxis>
                <YAxis
                  stroke="#9ca3af"
                  tick={{ fill: '#9ca3af', fontSize: 10 }}
                  width={60}
                  tickFormatter={(value) => compact.format(value as number)}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#1f2937',
                    border: '1px solid #374151',
                    borderRadius: '8px',
                    color: '#e5e7eb'
                  }}
                  formatter={(value, name) => [compact.format(value as number), name as string]}
                  labelFormatter={(label) => `Epoch ${label}`}
                />
                {SERIES.filter((s) => selected.has(s.key)).map((s) => (
                  <Line
                    key={s.key}
                    type="monotone"
                    dataKey={s.key}
                    name={s.label}
                    stroke={s.color}
                    strokeWidth={2}
                    dot={false}
                    activeDot={{ r: 4 }}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </div>
  )
}
