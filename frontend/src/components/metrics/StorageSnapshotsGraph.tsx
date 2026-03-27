import { useState, useMemo, useCallback } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer
} from 'recharts'

// Colors for different data paths
const PATH_COLORS = [
  '#3b82f6', // blue
  '#10b981', // green
  '#f59e0b', // amber
  '#ef4444', // red
  '#8b5cf6', // purple
  '#ec4899', // pink
  '#06b6d4', // cyan
  '#84cc16', // lime
  '#f97316', // orange
  '#14b8a6', // teal
]

interface StorageSnapshotItem {
  id: number
  block_number: number
  block_time: string
  pallet: number
  storage_location: string
  storage_keys: unknown
  data: unknown
  config_rule: string
}

interface StorageSnapshotsGraphProps {
  data: StorageSnapshotItem[]
}

interface ChartDataPoint {
  block_number: number
  block_time: number // timestamp in ms
  block_time_formatted: string
  [key: string]: number | string
}

// Recursively find all paths in an object that could represent numeric values
function findNumericPaths(obj: unknown, currentPath: string = ''): string[] {
  const paths: string[] = []

  if (obj === null || obj === undefined) {
    return paths
  }

  if (typeof obj === 'number') {
    if (currentPath) paths.push(currentPath)
    return paths
  }

  if (typeof obj === 'string') {
    // Check if string could be a number (including big numbers)
    const trimmed = obj.trim()
    if (trimmed !== '' && !isNaN(Number(trimmed))) {
      if (currentPath) paths.push(currentPath)
    }
    // Also check for bigint-like strings (very large numbers)
    if (/^-?\d+$/.test(trimmed) && trimmed.length > 0) {
      if (currentPath && !paths.includes(currentPath)) {
        paths.push(currentPath)
      }
    }
    return paths
  }

  if (Array.isArray(obj)) {
    obj.forEach((item, index) => {
      const newPath = currentPath ? `${currentPath}[${index}]` : `[${index}]`
      paths.push(...findNumericPaths(item, newPath))
    })
    return paths
  }

  if (typeof obj === 'object') {
    for (const [key, value] of Object.entries(obj)) {
      const newPath = currentPath ? `${currentPath}.${key}` : key
      paths.push(...findNumericPaths(value, newPath))
    }
  }

  return paths
}

// Get value at a given path from an object
function getValueAtPath(obj: unknown, path: string): number | null {
  if (!path) return null

  const parts = path.split(/\.|\[|\]/).filter(p => p !== '')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) return null
    if (typeof current !== 'object') return null

    if (Array.isArray(current)) {
      const index = parseInt(part, 10)
      if (isNaN(index)) return null
      current = current[index]
    } else {
      current = (current as Record<string, unknown>)[part]
    }
  }

  if (typeof current === 'number') return current
  if (typeof current === 'string') {
    const num = Number(current)
    if (!isNaN(num)) return num
  }

  return null
}

// Format timestamp for display
function formatTime(timestamp: number): string {
  const date = new Date(timestamp)
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  })
}

// Format large numbers for display
function formatValue(value: number): string {
  const abs = Math.abs(value)
  if (abs >= 1e18) return `${(value / 1e18).toFixed(2)}E`
  if (abs >= 1e15) return `${(value / 1e15).toFixed(2)}P`
  if (abs >= 1e12) return `${(value / 1e12).toFixed(2)}T`
  if (abs >= 1e9) return `${(value / 1e9).toFixed(2)}B`
  if (abs >= 1e6) return `${(value / 1e6).toFixed(2)}M`
  if (abs >= 1e3) return `${(value / 1e3).toFixed(2)}K`
  if (Number.isInteger(value)) return String(value)
  return value.toFixed(2)
}

export function StorageSnapshotsGraph({ data }: StorageSnapshotsGraphProps) {
  const [selectedPaths, setSelectedPaths] = useState<Set<string>>(new Set())

  // Find all unique numeric paths across all data items
  const allPaths = useMemo(() => {
    const pathSet = new Set<string>()

    data.forEach(item => {
      if (item.data) {
        const paths = findNumericPaths(item.data)
        paths.forEach(p => pathSet.add(p))
      }
    })

    return Array.from(pathSet).sort()
  }, [data])

  // Transform data for the chart
  const chartData = useMemo((): ChartDataPoint[] => {
    const sorted = [...data].sort((a, b) => a.block_number - b.block_number)

    return sorted.map(item => {
      const timestamp = new Date(item.block_time).getTime()
      const point: ChartDataPoint = {
        block_number: item.block_number,
        block_time: timestamp,
        block_time_formatted: formatTime(timestamp)
      }

      // Extract values for all selected paths
      selectedPaths.forEach(path => {
        const value = getValueAtPath(item.data, path)
        if (value !== null) {
          point[path] = value
        }
      })

      return point
    })
  }, [data, selectedPaths])

  // Calculate domain for X-axis (block_number)
  const xDomain = useMemo(() => {
    if (chartData.length === 0) return [0, 1]
    const minBlock = chartData[0].block_number
    const maxBlock = chartData[chartData.length - 1].block_number
    const padding = Math.max(1, Math.floor((maxBlock - minBlock) * 0.02))
    return [minBlock - padding, maxBlock + padding]
  }, [chartData])

  // Generate tick values for the x-axis
  const xTicks = useMemo(() => {
    if (chartData.length <= 1) return chartData.map(d => d.block_number)

    const minBlock = chartData[0].block_number
    const maxBlock = chartData[chartData.length - 1].block_number
    const range = maxBlock - minBlock

    // Generate roughly 5-8 ticks
    const tickCount = Math.min(8, Math.max(2, chartData.length))
    const step = Math.ceil(range / (tickCount - 1))

    const ticks: number[] = []
    for (let i = 0; i < tickCount; i++) {
      const tick = minBlock + (i * step)
      if (tick <= maxBlock) {
        ticks.push(tick)
      }
    }
    if (ticks[ticks.length - 1] !== maxBlock) {
      ticks.push(maxBlock)
    }

    return ticks
  }, [chartData])

  // Get block_time for a given block_number (for tick labels)
  const getTimeForBlock = useCallback((blockNumber: number): string => {
    // Find the closest data point
    let closest = chartData[0]
    let minDiff = Math.abs(chartData[0]?.block_number - blockNumber)

    for (const point of chartData) {
      const diff = Math.abs(point.block_number - blockNumber)
      if (diff < minDiff) {
        minDiff = diff
        closest = point
      }
    }

    return closest?.block_time_formatted || ''
  }, [chartData])

  const togglePath = (path: string) => {
    setSelectedPaths(prev => {
      const next = new Set(prev)
      if (next.has(path)) {
        next.delete(path)
      } else {
        next.add(path)
      }
      return next
    })
  }

  const getPathColor = (path: string): string => {
    const pathArray = Array.from(selectedPaths)
    const index = pathArray.indexOf(path)
    return PATH_COLORS[index % PATH_COLORS.length]
  }

  if (data.length === 0) {
    return (
      <div className="text-center text-gray-500 py-4">
        No storage snapshot data to display
      </div>
    )
  }

  if (allPaths.length === 0) {
    return (
      <div className="text-center text-gray-500 py-4">
        No numeric fields found in data
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="text-sm text-gray-400 text-center">
        Storage Snapshots: Blocks {chartData[0]?.block_number} - {chartData[chartData.length - 1]?.block_number}
      </div>

      {/* Path checkboxes */}
      <div className="bg-gray-900/50 rounded-lg p-3">
        <h4 className="text-sm font-medium mb-3 text-gray-300">
          Select data paths to visualize:
        </h4>
        <div className="flex flex-wrap gap-2 max-h-48 overflow-y-auto">
          {allPaths.map((path) => {
            const isSelected = selectedPaths.has(path)
            const color = isSelected ? getPathColor(path) : '#6b7280'

            return (
              <label
                key={path}
                className={`
                  flex items-center gap-2 px-3 py-1.5 rounded-md cursor-pointer
                  text-xs font-mono transition-colors
                  ${isSelected
                    ? 'bg-gray-700 border border-gray-500'
                    : 'bg-gray-800 border border-gray-700 hover:border-gray-600'
                  }
                `}
                style={{
                  borderLeftColor: color,
                  borderLeftWidth: '3px'
                }}
              >
                <input
                  type="checkbox"
                  checked={isSelected}
                  onChange={() => togglePath(path)}
                  className="sr-only"
                />
                <span
                  className="w-3 h-3 rounded-sm border flex-shrink-0"
                  style={{
                    backgroundColor: isSelected ? color : 'transparent',
                    borderColor: color
                  }}
                >
                  {isSelected && (
                    <svg className="w-3 h-3 text-white" viewBox="0 0 12 12" fill="none">
                      <path d="M2 6L5 9L10 3" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                    </svg>
                  )}
                </span>
                <span className="text-gray-300 truncate max-w-[200px]" title={path}>
                  {path}
                </span>
              </label>
            )
          })}
        </div>
        {allPaths.length > 10 && (
          <div className="text-xs text-gray-500 mt-2">
            {allPaths.length} paths available
          </div>
        )}
      </div>

      {/* Chart */}
      {selectedPaths.size > 0 ? (
        <div className="bg-gray-900/50 rounded-lg p-3">
          <h4 className="text-sm font-medium mb-2 text-gray-300">
            Value over Block Number
          </h4>
          <div style={{ height: '350px' }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart
                data={chartData}
                margin={{ top: 10, right: 30, left: 20, bottom: 50 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis
                  dataKey="block_number"
                  type="number"
                  domain={xDomain}
                  stroke="#9ca3af"
                  tick={{ fill: '#9ca3af', fontSize: 10 }}
                  ticks={xTicks}
                  tickFormatter={(value) => {
                    const time = getTimeForBlock(value)
                    return `${value}\n${time}`
                  }}
                  height={50}
                  interval={0}
                />
                <YAxis
                  stroke="#9ca3af"
                  tick={{ fill: '#9ca3af', fontSize: 10 }}
                  tickFormatter={formatValue}
                  width={70}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#1f2937',
                    border: '1px solid #374151',
                    borderRadius: '8px',
                    color: '#e5e7eb'
                  }}
                  formatter={(value, name) => [formatValue(value as number), name as string]}
                  labelFormatter={(label) => {
                    const point = chartData.find(d => d.block_number === label)
                    return `Block ${label}${point ? ` (${point.block_time_formatted})` : ''}`
                  }}
                />
                {Array.from(selectedPaths).map((path) => (
                  <Line
                    key={path}
                    type="monotone"
                    dataKey={path}
                    stroke={getPathColor(path)}
                    strokeWidth={2}
                    dot={{ r: 3, fill: getPathColor(path) }}
                    activeDot={{ r: 5 }}
                    connectNulls
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Legend */}
          <div className="flex flex-wrap gap-4 mt-3 justify-center">
            {Array.from(selectedPaths).map((path) => (
              <div key={path} className="flex items-center gap-2 text-xs">
                <div
                  className="w-4 h-0.5 rounded"
                  style={{ backgroundColor: getPathColor(path) }}
                />
                <span className="text-gray-400 font-mono">{path}</span>
              </div>
            ))}
          </div>
        </div>
      ) : (
        <div className="bg-gray-900/50 rounded-lg p-8 text-center">
          <div className="text-gray-500 text-sm">
            Select one or more data paths above to visualize their values over time
          </div>
        </div>
      )}
    </div>
  )
}
