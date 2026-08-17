import { useState, useMemo } from 'react'
import { Copy, Check } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useFormStore } from '@/stores/formStore'
import { useApiKey } from '@/hooks/useApiKey'
import { methods } from '@/config/methods'
import { buildRpcRequest } from '@/lib/buildRpcRequest'
import { shellQuote } from '@/lib/shellQuote'
import type { RpcRequest } from '@/lib/types'

export function CurlPreview() {
  const [copied, setCopied] = useState(false)
  const { currentMethod, formValues } = useFormStore()
  const { apiKey } = useApiKey()

  const methodConfig = methods[currentMethod]

  const request = useMemo((): RpcRequest => {
    return buildRpcRequest(methodConfig, formValues)
  }, [methodConfig, formValues, currentMethod])

  const curlCommand = useMemo(() => {
    const key = apiKey || 'YOUR_API_KEY'
    const jsonStr = JSON.stringify(request)

    return `curl -X POST ${shellQuote(`${window.location.origin}/api/v1/rpc`)} \\
  -H ${shellQuote('Content-Type: application/json')} \\
  -H ${shellQuote(`API-Key: ${key}`)} \\
  -d ${shellQuote(jsonStr)}`
  }, [request, apiKey])

  const handleCopy = async () => {
    await navigator.clipboard.writeText(curlCommand)
    setCopied(true)
    toast.success('cURL copied!')
    setTimeout(() => setCopied(false), 1500)
  }

  return (
    <Card className="bg-gray-800 border-gray-700">
      <CardHeader className="py-2 px-4 border-b border-gray-700">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-medium text-gray-400">cURL Command</CardTitle>
          <Button
            variant="ghost"
            size="sm"
            onClick={handleCopy}
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
                Copy
              </>
            )}
          </Button>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <pre className="p-4 text-sm text-green-400 bg-gray-900 rounded-b-lg overflow-x-auto max-h-48">
          {curlCommand}
        </pre>
      </CardContent>
    </Card>
  )
}
