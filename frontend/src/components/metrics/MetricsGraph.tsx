import { useMemo } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer
} from 'recharts'

// Pool colors for the chart lines (cycle through if more pools exist)
const POOL_COLORS = [
  '#3b82f6', // blue
  '#10b981', // green
  '#f59e0b', // amber
  '#ef4444', // red
  '#8b5cf6', // purple
  '#ec4899', // pink
  '#06b6d4', // cyan
  '#84cc16', // lime
]

const getPoolColor = (poolId: number): string => {
  return POOL_COLORS[(poolId - 1) % POOL_COLORS.length]
}

interface EpochMetricsItem {
  epoch: number
  metrics: Record<string, Record<string, string>> // processor -> pool -> value
}

interface ChartDataPoint {
  epoch: number
  [key: string]: number // pool1, pool2, etc.
}

interface MetricsGraphProps {
  data: EpochMetricsItem[]
}

export function MetricsGraph({ data }: MetricsGraphProps) {
  // Find all unique pool IDs across all data
  const poolIds = useMemo((): number[] => {
    const poolSet = new Set<number>()

    data.forEach(epochItem => {
      if (epochItem.metrics && typeof epochItem.metrics === 'object') {
        Object.values(epochItem.metrics).forEach((processorMetrics) => {
          if (processorMetrics && typeof processorMetrics === 'object') {
            Object.keys(processorMetrics).forEach(poolId => {
              const num = parseInt(poolId, 10)
              if (!isNaN(num)) {
                poolSet.add(num)
              }
            })
          }
        })
      }
    })

    return Array.from(poolSet).sort((a, b) => a - b)
  }, [data])

  // Transform epochs data into chart format
  const chartData = useMemo((): ChartDataPoint[] => {
    const sorted = [...data].sort((a, b) => a.epoch - b.epoch)

    return sorted.map(epochItem => {
      const poolTotals: Record<string, bigint> = {}

      // Initialize all pools to 0
      poolIds.forEach(id => {
        poolTotals[String(id)] = BigInt(0)
      })

      // Sum up all processor metrics for each pool
      if (epochItem.metrics && typeof epochItem.metrics === 'object') {
        Object.values(epochItem.metrics).forEach((processorMetrics) => {
          if (processorMetrics && typeof processorMetrics === 'object') {
            Object.entries(processorMetrics).forEach(([poolId, value]) => {
              if (Object.hasOwn(poolTotals, poolId) && value) {
                try {
                  poolTotals[poolId] += BigInt(value)
                } catch {
                  // Skip invalid values
                }
              }
            })
          }
        })
      }

      // Convert to numbers (scaled down for display)
      const scale = BigInt(10 ** 18) // Assuming values are in 18 decimals
      const result: ChartDataPoint = { epoch: epochItem.epoch }

      poolIds.forEach(id => {
        result[`pool${id}`] = Number(poolTotals[String(id)] / scale)
      })

      return result
    })
  }, [data, poolIds])

  // Format large numbers for display
  const formatValue = (value: number) => {
    if (value >= 1e9) return `${(value / 1e9).toFixed(2)}B`
    if (value >= 1e6) return `${(value / 1e6).toFixed(2)}M`
    if (value >= 1e3) return `${(value / 1e3).toFixed(2)}K`
    return value.toFixed(2)
  }

  if (chartData.length === 0) {
    return (
      <div className="text-center text-gray-500 py-4">
        No metrics data to display
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="text-sm text-gray-400 text-center">
        Epochs {chartData[0]?.epoch} - {chartData[chartData.length - 1]?.epoch}
      </div>

      {/* Full width charts */}
      <div className="space-y-4 w-full">
        {poolIds.map((poolId) => (
          <div key={poolId} className="bg-gray-900/50 rounded-lg p-3">
            <h4 className="text-sm font-medium mb-2" style={{ color: getPoolColor(poolId) }}>
              Pool {poolId}
            </h4>
            <div style={{ height: '200px' }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart
                  data={chartData}
                  margin={{ top: 10, right: 30, left: 20, bottom: 10 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis
                    dataKey="epoch"
                    stroke="#9ca3af"
                    tick={{ fill: '#9ca3af', fontSize: 10 }}
                  />
                  <YAxis
                    stroke="#9ca3af"
                    tick={{ fill: '#9ca3af', fontSize: 10 }}
                    tickFormatter={formatValue}
                    width={60}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#1f2937',
                      border: '1px solid #374151',
                      borderRadius: '8px',
                      color: '#e5e7eb'
                    }}
                    formatter={(value) => [formatValue(value as number), `Pool ${poolId}`]}
                    labelFormatter={(label) => `Epoch ${label}`}
                  />
                  <Line
                    type="monotone"
                    dataKey={`pool${poolId}`}
                    stroke={getPoolColor(poolId)}
                    strokeWidth={2}
                    dot={false}
                    activeDot={{ r: 4 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
