import { useMemo } from 'react'
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

interface ProcessorsCountItem {
  epoch: number
  count: number
}

interface ProcessorsCountGraphProps {
  data: ProcessorsCountItem[]
}

export function ProcessorsCountGraph({ data }: ProcessorsCountGraphProps) {
  // Sort data by epoch ascending for proper chart display
  const chartData = useMemo(() => {
    return [...data].sort((a, b) => a.epoch - b.epoch)
  }, [data])

  // Calculate domain for X-axis
  const xDomain = useMemo(() => {
    if (chartData.length === 0) return [0, 1]
    const minEpoch = chartData[0].epoch
    const maxEpoch = chartData[chartData.length - 1].epoch
    // Add small padding to the domain
    const padding = Math.max(1, Math.floor((maxEpoch - minEpoch) * 0.02))
    return [minEpoch - padding, maxEpoch + padding]
  }, [chartData])

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
        Active Processors per Epoch ({chartData[0]?.epoch} - {chartData[chartData.length - 1]?.epoch})
      </div>

      <div className="bg-gray-900/50 rounded-lg p-3">
        <h4 className="text-sm font-medium mb-2 text-blue-400">
          Distinct Processors with Heartbeats
        </h4>
        <div style={{ height: '300px' }}>
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
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1f2937',
                  border: '1px solid #374151',
                  borderRadius: '8px',
                  color: '#e5e7eb'
                }}
                formatter={(value) => [value, 'Processors']}
                labelFormatter={(label) => `Epoch ${label}`}
              />
              <Line
                type="monotone"
                dataKey="count"
                stroke="#3b82f6"
                strokeWidth={2}
                dot={{ r: 3, fill: '#3b82f6' }}
                activeDot={{ r: 5 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  )
}
