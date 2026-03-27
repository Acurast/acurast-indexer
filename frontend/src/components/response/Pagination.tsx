import { ChevronLeft, ChevronRight } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { useFormStore } from '@/stores/formStore'
import { useApiKey } from '@/hooks/useApiKey'
import { useRpcRequest } from '@/hooks/useRpcRequest'
import { useUIStore } from '@/stores/uiStore'
import { methods } from '@/config/methods'
import type { RpcRequest, PagedResult } from '@/lib/types'

interface PaginationProps {
  result: PagedResult<unknown> | null
  onPageChange: () => void
}

export function Pagination({ result, onPageChange }: PaginationProps) {
  const {
    currentMethod,
    cursorHistory,
    currentPage,
    pushCursor,
    popCursor,
    setResponse,
    setFormValue
  } = useFormStore()

  const { setExecuting } = useUIStore()
  const { apiKey } = useApiKey()
  const rpcMutation = useRpcRequest()

  if (!result?.items) return null

  const hasPrev = cursorHistory.length > 0
  const hasNext = result.cursor !== null && result.cursor !== undefined

  const methodConfig = methods[currentMethod]

  const buildRequest = (cursor: unknown): RpcRequest => {
    // Get latest formValues from store to avoid closure issues
    const latestFormValues = useFormStore.getState().formValues
    const params: Record<string, unknown> = {}
    let simpleParam: string | null = null

    methodConfig.fields.forEach(field => {
      if (field.type === 'separator') return

      const value = latestFormValues[field.name]
      if (value === undefined || value === '' || value === null) return

      let processedValue: unknown = value

      if (field.type === 'number') {
        processedValue = parseInt(value as string)
      } else if (field.type === 'json') {
        try {
          processedValue = JSON.parse(value as string)
        } catch {
          processedValue = value
        }
      } else if (field.type === 'datetime') {
        processedValue = new Date(value as string).toISOString()
      }

      if (field.isParam) {
        simpleParam = processedValue as string
      } else if (field.nested) {
        if (!params[field.nested]) {
          params[field.nested] = {}
        }
        const propName = field.name.split('.').pop()!
        ;(params[field.nested] as Record<string, unknown>)[propName] = processedValue
      } else {
        params[field.name] = processedValue
      }
    })

    // Clean up empty nested objects
    Object.keys(params).forEach(key => {
      if (typeof params[key] === 'object' && params[key] !== null && !Array.isArray(params[key])) {
        if (Object.keys(params[key] as object).length === 0) {
          delete params[key]
        }
      }
    })

    // Add cursor (pass through as-is, server handles typing)
    if (cursor !== null && cursor !== undefined) {
      params.cursor = cursor
    }

    return {
      jsonrpc: '2.0',
      method: methodConfig.name,
      params: simpleParam !== null ? simpleParam : params,
      id: 1
    }
  }

  const executePage = async (cursor: unknown) => {
    if (!apiKey) return

    setExecuting(true)

    try {
      const request = buildRequest(cursor)
      const { response, time } = await rpcMutation.mutateAsync({ apiKey, request })
      setResponse(response, time)
      onPageChange()
    } catch (error) {
      console.error('Pagination error:', error)
    } finally {
      setExecuting(false)
    }
  }

  // Convert cursor to JSON string for form field display
  const cursorToFormValue = (cursor: unknown): string => {
    if (cursor === null || cursor === undefined) return ''
    return JSON.stringify(cursor)
  }

  const handlePrev = async () => {
    const prevCursor = popCursor()
    if (prevCursor !== undefined) {
      setFormValue('cursor', cursorToFormValue(prevCursor))
      await executePage(prevCursor)
    }
  }

  const handleNext = async () => {
    if (result.cursor !== null && result.cursor !== undefined) {
      pushCursor(result.cursor)
      setFormValue('cursor', cursorToFormValue(result.cursor))
      await executePage(result.cursor)
    }
  }

  return (
    <Card className="bg-gray-800 border-gray-700">
      <CardContent className="py-3 px-4 flex items-center justify-between">
        <Button
          variant="secondary"
          size="sm"
          onClick={handlePrev}
          disabled={!hasPrev}
          className="bg-gray-700 hover:bg-gray-600"
        >
          <ChevronLeft className="h-4 w-4 mr-1" />
          Previous
        </Button>

        <span className="text-sm text-gray-400">
          Page <span className="font-semibold text-white">{currentPage}</span> |{' '}
          <span className="font-semibold text-white">{result.items.length}</span> items
        </span>

        <Button
          variant="secondary"
          size="sm"
          onClick={handleNext}
          disabled={!hasNext}
          className="bg-gray-700 hover:bg-gray-600"
        >
          Next
          <ChevronRight className="h-4 w-4 ml-1" />
        </Button>
      </CardContent>
    </Card>
  )
}
