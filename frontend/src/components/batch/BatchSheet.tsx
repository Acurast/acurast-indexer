import { useState } from 'react'
import { X, Play, Copy, Trash2, Check } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription
} from '@/components/ui/sheet'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Separator } from '@/components/ui/separator'
import { useBatchStore, useFormStore } from '@/stores/formStore'
import { useUIStore } from '@/stores/uiStore'
import { useApiKey } from '@/hooks/useApiKey'
import { useBatchRpcRequest } from '@/hooks/useRpcRequest'
import type { RpcResponse, BatchRequest } from '@/lib/types'
import type { MethodKey } from '@/config/methods'

export function BatchSheet() {
  const { isBatchSheetOpen, closeBatchSheet } = useUIStore()
  const { requests, removeRequest, clearRequests } = useBatchStore()
  const { setCurrentMethod, setFormValues, resetPagination } = useFormStore()
  const { apiKey } = useApiKey()
  const batchMutation = useBatchRpcRequest()

  const [responses, setResponses] = useState<RpcResponse[] | null>(null)
  const [responseTime, setResponseTime] = useState<number | null>(null)
  const [isExecuting, setIsExecuting] = useState(false)
  const [responseCopied, setResponseCopied] = useState(false)

  const handleExecuteBatch = async () => {
    if (!apiKey) {
      toast.error('Please enter an API key')
      return
    }

    if (requests.length === 0) {
      toast.error('No requests in batch')
      return
    }

    setIsExecuting(true)
    setResponses(null)

    try {
      const rpcRequests = requests.map(r => r.request)
      const { responses: resps, time } = await batchMutation.mutateAsync({
        apiKey,
        requests: rpcRequests
      })

      setResponses(resps)
      setResponseTime(time)
      toast.success(`Batch executed: ${resps.length} responses in ${time}ms`)
    } catch (error) {
      toast.error(`Batch failed: ${(error as Error).message}`)
    } finally {
      setIsExecuting(false)
    }
  }

  const handleCopyCurl = async () => {
    if (requests.length === 0) return

    const key = apiKey || 'YOUR_API_KEY'
    const rpcRequests = requests.map(r => r.request)
    const jsonStr = JSON.stringify(rpcRequests)

    const curl = `curl -X POST '${window.location.origin}/api/v1/rpc' \\
  -H 'Content-Type: application/json' \\
  -H 'API-Key: ${key}' \\
  -d '${jsonStr}'`

    await navigator.clipboard.writeText(curl)
    toast.success('Batch cURL copied!')
  }

  const handleCopyJson = async () => {
    if (requests.length === 0) return

    const rpcRequests = requests.map(r => r.request)
    await navigator.clipboard.writeText(JSON.stringify(rpcRequests, null, 2))
    toast.success('Batch JSON copied!')
  }

  const handleClearAll = () => {
    clearRequests()
    setResponses(null)
    setResponseTime(null)
    toast('Batch cleared')
  }

  const handleCopyResponse = async () => {
    if (!responses) return

    await navigator.clipboard.writeText(JSON.stringify(responses, null, 2))
    setResponseCopied(true)
    toast.success('Response copied!')
    setTimeout(() => setResponseCopied(false), 1500)
  }

  // Restore a batched request to the form
  const handleRestoreRequest = (req: BatchRequest) => {
    // Set the method
    setCurrentMethod(req.methodKey as MethodKey)

    // Flatten params back to form values
    const params = req.request.params
    if (typeof params === 'object' && params !== null) {
      const formValues: Record<string, unknown> = {}

      for (const [key, value] of Object.entries(params)) {
        if (key === 'cursor') continue // Skip cursor

        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          // Nested object (e.g., extrinsic: { pallet, method, account_id })
          for (const [nestedKey, nestedValue] of Object.entries(value as Record<string, unknown>)) {
            formValues[`${key}.${nestedKey}`] = nestedValue
          }
        } else if (Array.isArray(value)) {
          // JSON array - stringify it
          formValues[key] = JSON.stringify(value)
        } else {
          formValues[key] = value
        }
      }

      // Use setTimeout to ensure method is set first
      setTimeout(() => {
        setFormValues(formValues)
        resetPagination()
      }, 10)
    }

    closeBatchSheet()
    toast.success(`Loaded ${req.methodName} request`)
  }

  return (
    <Sheet open={isBatchSheetOpen} onOpenChange={(open) => !open && closeBatchSheet()}>
      <SheetContent className="w-full sm:max-w-xl bg-gray-900 border-gray-700">
        <SheetHeader>
          <SheetTitle className="text-xl text-white flex items-center justify-between">
            <span>Batch Requests</span>
            <Badge variant="secondary" className="bg-purple-600 text-white">
              {requests.length} items
            </Badge>
          </SheetTitle>
          <SheetDescription className="text-gray-400">
            Execute multiple RPC requests in a single batch call
          </SheetDescription>
        </SheetHeader>

        <div className="mt-4 space-y-4">
          {/* Request List */}
          <div className="space-y-2">
            <h3 className="text-sm font-medium text-gray-400">Queued Requests</h3>
            {requests.length === 0 ? (
              <p className="text-sm text-gray-500 italic py-4 text-center">
                No requests in batch. Add requests using "Add to Batch" button.
              </p>
            ) : (
              <ScrollArea className="h-48 rounded border border-gray-700 bg-gray-800">
                <div className="p-2 space-y-2">
                  {requests.map((req) => (
                    <div
                      key={req.id}
                      className="flex items-center justify-between p-2 rounded bg-gray-700 hover:bg-gray-600 transition group"
                    >
                      <button
                        onClick={() => handleRestoreRequest(req)}
                        className="flex-1 min-w-0 text-left cursor-pointer"
                        title="Click to load this request"
                      >
                        <div className="flex items-center space-x-2">
                          <span className="text-xs text-gray-400">#{req.id}</span>
                          <span className="text-sm font-medium text-blue-400 truncate group-hover:text-blue-300">
                            {req.methodName}
                          </span>
                        </div>
                        <p className="text-xs text-gray-500 truncate">{req.summary}</p>
                      </button>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => {
                          e.stopPropagation()
                          removeRequest(req.id)
                        }}
                        className="h-7 w-7 p-0 text-gray-400 hover:text-red-400 ml-2"
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </div>
                  ))}
                </div>
              </ScrollArea>
            )}
          </div>

          {/* Action Buttons */}
          <div className="flex flex-wrap gap-2">
            <Button
              onClick={handleExecuteBatch}
              disabled={isExecuting || requests.length === 0}
              className="bg-purple-600 hover:bg-purple-700"
            >
              {isExecuting ? (
                <span className="flex items-center space-x-2">
                  <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                  <span>Executing...</span>
                </span>
              ) : (
                <>
                  <Play className="h-4 w-4 mr-2" />
                  Execute Batch
                </>
              )}
            </Button>

            <Button
              variant="secondary"
              onClick={handleCopyCurl}
              disabled={requests.length === 0}
              className="bg-gray-700 hover:bg-gray-600"
            >
              <Copy className="h-4 w-4 mr-2" />
              Copy cURL
            </Button>

            <Button
              variant="secondary"
              onClick={handleCopyJson}
              disabled={requests.length === 0}
              className="bg-gray-700 hover:bg-gray-600"
            >
              <Copy className="h-4 w-4 mr-2" />
              Copy JSON
            </Button>

            <Button
              variant="secondary"
              onClick={handleClearAll}
              disabled={requests.length === 0}
              className="bg-red-900 hover:bg-red-800 text-red-300"
            >
              <Trash2 className="h-4 w-4 mr-2" />
              Clear All
            </Button>
          </div>

          <Separator className="bg-gray-700" />

          {/* Response Area */}
          {responses && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-medium text-gray-400">Response</h3>
                <div className="flex items-center space-x-2">
                  {responseTime && (
                    <span className="text-xs text-gray-500">{responseTime}ms</span>
                  )}
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={handleCopyResponse}
                    className="h-7 px-2 text-xs"
                  >
                    {responseCopied ? (
                      <>
                        <Check className="h-3 w-3 mr-1" />
                        Copied!
                      </>
                    ) : (
                      <>
                        <Copy className="h-3 w-3 mr-1" />
                        Copy
                      </>
                    )}
                  </Button>
                </div>
              </div>
              <ScrollArea className="h-64 rounded border border-gray-700 bg-gray-800">
                <pre className="p-4 text-sm text-gray-300 whitespace-pre-wrap">
                  {JSON.stringify(responses, null, 2)}
                </pre>
              </ScrollArea>
            </div>
          )}
        </div>
      </SheetContent>
    </Sheet>
  )
}
