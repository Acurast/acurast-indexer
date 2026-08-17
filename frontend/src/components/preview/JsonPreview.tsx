import { useState, useMemo } from 'react'
import { Copy, Check } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useFormStore } from '@/stores/formStore'
import { methods } from '@/config/methods'
import { buildRpcRequest } from '@/lib/buildRpcRequest'
import type { RpcRequest } from '@/lib/types'

export function JsonPreview() {
  const [copied, setCopied] = useState(false)
  const { currentMethod, formValues } = useFormStore()

  const methodConfig = methods[currentMethod]

  const request = useMemo((): RpcRequest => {
    return buildRpcRequest(methodConfig, formValues)
  }, [methodConfig, formValues, currentMethod])

  const jsonString = useMemo(() => {
    return JSON.stringify(request, null, 2)
  }, [request])

  const handleCopy = async () => {
    await navigator.clipboard.writeText(jsonString)
    setCopied(true)
    toast.success('JSON request copied!')
    setTimeout(() => setCopied(false), 1500)
  }

  return (
    <Card className="bg-gray-800 border-gray-700">
      <CardHeader className="py-2 px-4 border-b border-gray-700">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-medium text-gray-400">JSON Request Body</CardTitle>
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
        <pre className="p-4 text-sm text-yellow-400 bg-gray-900 rounded-b-lg overflow-x-auto max-h-32 font-mono">
          {jsonString}
        </pre>
      </CardContent>
    </Card>
  )
}
