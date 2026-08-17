import { useEffect } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MainLayout } from '@/components/layout/MainLayout'
import { MethodForm } from '@/components/forms/MethodForm'
import { CurlPreview } from '@/components/preview/CurlPreview'
import { JsonPreview } from '@/components/preview/JsonPreview'
import { ResponsePanel } from '@/components/response/ResponsePanel'
import { BatchSheet } from '@/components/batch/BatchSheet'
import { MetricsGraph } from '@/components/metrics/MetricsGraph'
import { ProcessorsCountGraph } from '@/components/metrics/ProcessorsCountGraph'
import { StorageSnapshotsGraph } from '@/components/metrics/StorageSnapshotsGraph'
import { DeploymentsGraph } from '@/components/metrics/DeploymentsGraph'
import { EpochTotalsGraph } from '@/components/metrics/EpochTotalsGraph'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useBatchStore, useFormStore } from '@/stores/formStore'
import { useApiKey } from '@/hooks/useApiKey'
import { useUrlSync } from '@/hooks/useUrlSync'
import type { PagedResult } from '@/lib/types'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 60 * 5, // 5 minutes
      retry: false
    }
  }
})

function Dashboard() {
  const { loadFromStorage: loadBatchFromStorage } = useBatchStore()
  const { loadFromStorage: loadApiKeyFromStorage } = useApiKey()
  const { currentMethod, lastResponse } = useFormStore()

  // Sync URL with form state
  useUrlSync()

  // Load from localStorage on mount
  useEffect(() => {
    loadBatchFromStorage()
    loadApiKeyFromStorage()
  }, [loadBatchFromStorage, loadApiKeyFromStorage])

  const result = lastResponse?.result as PagedResult<unknown> | undefined
  const showMetricsGraph = currentMethod === 'epochMetrics' && result?.items && (result.items as unknown[]).length > 0
  const showProcessorsCountGraph = currentMethod === 'processorsCountByEpoch' && result?.items && (result.items as unknown[]).length > 0
  const showStorageSnapshotsGraph = currentMethod === 'storageSnapshots' && result?.items && (result.items as unknown[]).length > 0
  const showDeploymentsGraph = currentMethod === 'deployments' && result?.items && (result.items as unknown[]).length > 0
  // getEpochTotals returns a bare array (Vec), not a Page with `.items`.
  const epochTotalsItems = Array.isArray(lastResponse?.result)
    ? (lastResponse!.result as { epoch: number; block_number: number; block_time: string; total_vesting: string; total_token_claim: string; total_self_staked: string; total_delegated: string }[])
    : []
  const showEpochTotalsGraph = currentMethod === 'epochTotals' && epochTotalsItems.length > 0

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 items-stretch">
      {/* Left Panel: Form & Previews */}
      <div className="space-y-4">
        <MethodForm />
        <CurlPreview />
        <JsonPreview />
      </div>

      {/* Right Panel: h-0 prevents affecting grid row height, min-h-full fills available space */}
      <div className="flex flex-col lg:h-0 lg:min-h-full overflow-hidden min-h-100">
        <ResponsePanel />
      </div>

      {/* Metrics Graph - spans both columns */}
      {showMetricsGraph && (
        <Card className="bg-gray-800 border-gray-700 lg:col-span-2">
          <CardHeader className="py-2 px-4 border-b border-gray-700">
            <CardTitle className="text-sm font-medium text-gray-400">Graph Visualization</CardTitle>
          </CardHeader>
          <CardContent className="p-4">
            <MetricsGraph data={result.items as { epoch: number; metrics: Record<string, Record<string, string>> }[]} />
          </CardContent>
        </Card>
      )}

      {/* Processors Count Graph - spans both columns */}
      {showProcessorsCountGraph && (
        <Card className="bg-gray-800 border-gray-700 lg:col-span-2">
          <CardHeader className="py-2 px-4 border-b border-gray-700">
            <CardTitle className="text-sm font-medium text-gray-400">Processors Count by Epoch</CardTitle>
          </CardHeader>
          <CardContent className="p-4">
            <ProcessorsCountGraph data={result.items as { epoch: number; count: number }[]} />
          </CardContent>
        </Card>
      )}

      {/* Storage Snapshots Graph - spans both columns */}
      {showStorageSnapshotsGraph && (
        <Card className="bg-gray-800 border-gray-700 lg:col-span-2">
          <CardHeader className="py-2 px-4 border-b border-gray-700">
            <CardTitle className="text-sm font-medium text-gray-400">Storage Snapshots Graph</CardTitle>
          </CardHeader>
          <CardContent className="p-4">
            <StorageSnapshotsGraph data={result.items as { id: number; block_number: number; block_time: string; pallet: number; storage_location: string; storage_keys: unknown; data: unknown; config_rule: string }[]} />
          </CardContent>
        </Card>
      )}

      {/* Deployments Graph - spans both columns */}
      {showDeploymentsGraph && (
        <Card className="bg-gray-800 border-gray-700 lg:col-span-2">
          <CardHeader className="py-2 px-4 border-b border-gray-700">
            <CardTitle className="text-sm font-medium text-gray-400">Deployments Graph</CardTitle>
          </CardHeader>
          <CardContent className="p-4">
            <DeploymentsGraph data={result.items as { address: string; seq_id: number; created_block_time: string; reward: string; is_active: boolean }[]} />
          </CardContent>
        </Card>
      )}

      {/* Epoch Totals Graph - spans both columns */}
      {showEpochTotalsGraph && (
        <Card className="bg-gray-800 border-gray-700 lg:col-span-2">
          <CardHeader className="py-2 px-4 border-b border-gray-700">
            <CardTitle className="text-sm font-medium text-gray-400">Epoch Totals Graph</CardTitle>
          </CardHeader>
          <CardContent className="p-4">
            <EpochTotalsGraph data={epochTotalsItems} />
          </CardContent>
        </Card>
      )}

      {/* Batch Sheet (slide-over) */}
      <BatchSheet />
    </div>
  )
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <MainLayout>
        <Dashboard />
      </MainLayout>
    </QueryClientProvider>
  )
}

export default App
