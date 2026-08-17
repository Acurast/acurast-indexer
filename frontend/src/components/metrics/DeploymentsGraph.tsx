import { useMemo } from 'react'
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  Label
} from 'recharts'

// Reward is stored on-chain as u128 planck; 1 ACU = 10^12 planck.
const PLANCK_PER_ACU = 1e12

const ACTIVE_COLOR = '#10b981' // green
const INACTIVE_COLOR = '#6b7280' // gray

interface DeploymentItem {
  address: string
  seq_id: number
  created_block_time: string
  reward: string
  is_active: boolean
}

interface DeploymentsGraphProps {
  data: DeploymentItem[]
}

interface ChartDataPoint {
  time: number // ms timestamp
  reward: number // in ACU
  address: string
  seq_id: number
  is_active: boolean
}

function truncateAddress(address: string): string {
  if (address.length <= 14) return address
  return `${address.slice(0, 8)}…${address.slice(-6)}`
}

function formatTime(timestamp: number): string {
  return new Date(timestamp).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  })
}

function formatReward(value: number): string {
  const abs = Math.abs(value)
  if (abs >= 1e9) return `${(value / 1e9).toFixed(2)}B`
  if (abs >= 1e6) return `${(value / 1e6).toFixed(2)}M`
  if (abs >= 1e3) return `${(value / 1e3).toFixed(2)}K`
  return value.toFixed(4)
}

export function DeploymentsGraph({ data }: DeploymentsGraphProps) {
  const chartData = useMemo((): ChartDataPoint[] => {
    return data
      .map((item) => ({
        time: new Date(item.created_block_time).getTime(),
        reward: Number(item.reward) / PLANCK_PER_ACU,
        address: item.address,
        seq_id: item.seq_id,
        is_active: item.is_active
      }))
      .filter((point) => !isNaN(point.time) && !isNaN(point.reward))
      .sort((a, b) => a.time - b.time)
  }, [data])

  const xDomain = useMemo(() => {
    if (chartData.length === 0) return [0, 1]
    const minTime = chartData[0].time
    const maxTime = chartData[chartData.length - 1].time
    const padding = Math.max(1, Math.floor((maxTime - minTime) * 0.05))
    return [minTime - padding, maxTime + padding]
  }, [chartData])

  if (chartData.length === 0) {
    return (
      <div className="text-center text-gray-500 py-4">
        No deployment data to display
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="text-sm text-gray-400 text-center">
        {chartData.length} deployment{chartData.length === 1 ? '' : 's'} from {formatTime(chartData[0].time)} to {formatTime(chartData[chartData.length - 1].time)}
      </div>

      <div className="bg-gray-900/50 rounded-lg p-3">
        <h4 className="text-sm font-medium mb-2 text-blue-400">
          Reward by Deployment Creation Time
        </h4>
        <div style={{ height: '350px' }}>
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ top: 10, right: 30, left: 20, bottom: 25 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="time"
                type="number"
                domain={xDomain}
                stroke="#9ca3af"
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                tickFormatter={formatTime}
              >
                <Label value="Created At" position="bottom" offset={-5} fill="#9ca3af" fontSize={12} />
              </XAxis>
              <YAxis
                dataKey="reward"
                stroke="#9ca3af"
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                tickFormatter={formatReward}
                width={70}
              >
                <Label value="Reward (ACU)" angle={-90} position="insideLeft" fill="#9ca3af" fontSize={12} />
              </YAxis>
              <Tooltip
                cursor={{ strokeDasharray: '3 3', stroke: '#4b5563' }}
                contentStyle={{
                  backgroundColor: '#1f2937',
                  border: '1px solid #374151',
                  borderRadius: '8px',
                  color: '#e5e7eb'
                }}
                formatter={(value, name) => {
                  if (name === 'reward') return [`${formatReward(value as number)} ACU`, 'Reward']
                  return [value, name]
                }}
                labelFormatter={() => ''}
                content={({ active, payload }) => {
                  if (!active || !payload || payload.length === 0) return null
                  const point = payload[0].payload as ChartDataPoint
                  return (
                    <div className="bg-gray-800 border border-gray-700 rounded-lg p-2 text-xs">
                      <div className="text-gray-300 font-mono">{truncateAddress(point.address)}#{point.seq_id}</div>
                      <div className="text-gray-400">{formatTime(point.time)}</div>
                      <div className="text-blue-400">{formatReward(point.reward)} ACU</div>
                      <div className={point.is_active ? 'text-green-400' : 'text-gray-500'}>
                        {point.is_active ? 'Active' : 'Inactive'}
                      </div>
                    </div>
                  )
                }}
              />
              <Scatter data={chartData} shape="circle">
                {chartData.map((point, index) => (
                  <Cell
                    key={`${point.address}-${point.seq_id}-${index}`}
                    fill={point.is_active ? ACTIVE_COLOR : INACTIVE_COLOR}
                    fillOpacity={0.75}
                  />
                ))}
              </Scatter>
            </ScatterChart>
          </ResponsiveContainer>
        </div>
        <div className="flex flex-wrap gap-4 mt-3 justify-center">
          <div className="flex items-center gap-2 text-xs">
            <div className="w-3 h-3 rounded-full" style={{ backgroundColor: ACTIVE_COLOR }} />
            <span className="text-gray-400">Active</span>
          </div>
          <div className="flex items-center gap-2 text-xs">
            <div className="w-3 h-3 rounded-full" style={{ backgroundColor: INACTIVE_COLOR }} />
            <span className="text-gray-400">Inactive</span>
          </div>
        </div>
      </div>
    </div>
  )
}
