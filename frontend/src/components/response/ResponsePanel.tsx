import { useState } from 'react'
import { Copy, Check } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { JsonViewer } from './JsonViewer'
import { Pagination } from './Pagination'
import { useFormStore } from '@/stores/formStore'
import type { PagedResult } from '@/lib/types'

export function ResponsePanel() {
  const [copied, setCopied] = useState(false)
  const { lastResponse, responseTime } = useFormStore()

  const handleCopyResponse = async () => {
    if (!lastResponse) return

    await navigator.clipboard.writeText(JSON.stringify(lastResponse, null, 2))
    setCopied(true)
    toast.success('Response JSON copied!')
    setTimeout(() => setCopied(false), 1500)
  }

  const isSuccess = lastResponse && !lastResponse.error
  const isError = lastResponse?.error

  const result = lastResponse?.result as PagedResult<unknown> | undefined

  return (
    <div className="flex flex-col flex-1 space-y-4 min-h-0">
      {/* Pagination */}
      {result?.items && <Pagination result={result} onPageChange={() => {}} />}

      {/* Response */}
      <Card className="bg-gray-800 border-gray-700 flex-1 flex flex-col min-h-0">
        <CardHeader className="py-2 px-4 border-b border-gray-700 shrink-0">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-2">
              <CardTitle className="text-sm font-medium text-gray-400">Response</CardTitle>
              {isSuccess && (
                <Badge className="bg-green-900 text-green-300">Success</Badge>
              )}
              {isError && (
                <Badge className="bg-red-900 text-red-300">Error</Badge>
              )}
            </div>
            <div className="flex items-center space-x-2">
              {responseTime !== null && (
                <span className="text-xs text-gray-500">{responseTime}ms</span>
              )}
              <Button
                variant="ghost"
                size="sm"
                onClick={handleCopyResponse}
                disabled={!lastResponse}
                className="h-7 px-2 text-xs"
              >
                {copied ? (
                  <>
                    <Check className="h-3 w-3 mr-1" />
                    Copied!
                  </>
                ) : (
                  <>
                    <Copy className="h-3 w-3 mr-1" />
                    Copy JSON
                  </>
                )}
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent className="p-4 bg-gray-900 rounded-b-lg overflow-auto flex-1 min-h-0">
          {!lastResponse ? (
            <p className="text-sm text-gray-500">Execute a request to see the response...</p>
          ) : isError ? (
            <pre className="text-sm text-red-400">
              {JSON.stringify(lastResponse, null, 2)}
            </pre>
          ) : (
            <JsonViewer data={lastResponse} />
          )}
        </CardContent>
      </Card>

    </div>
  )
}
