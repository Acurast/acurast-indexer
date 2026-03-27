import { useEffect, useCallback } from 'react'
import { Play, Plus, RotateCcw } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { PalletSelector, MethodSelector, ComboboxSelector, StoragePalletSelector, StorageLocationSelector, ConfigRuleSelector } from './PalletMethodSelector'
import { DateTimePicker } from './DatePicker'
import { useFormStore, useBatchStore } from '@/stores/formStore'
import { useUIStore } from '@/stores/uiStore'
import { useApiKey } from '@/hooks/useApiKey'
import { useRpcRequest } from '@/hooks/useRpcRequest'
import { methods } from '@/config/methods'
import type { FieldConfig, RpcRequest } from '@/lib/types'

export function MethodForm() {
  const {
    currentMethod,
    formValues,
    setFormValue,
    resetFormValues,
    setResponse,
    resetPagination
  } = useFormStore()

  const { addRequest } = useBatchStore()
  const { setExecuting, isExecuting } = useUIStore()
  const { apiKey } = useApiKey()
  const rpcMutation = useRpcRequest()

  const methodConfig = methods[currentMethod]

  // Build the RPC request from form values
  const buildRequest = useCallback((id = 1): RpcRequest => {
    // Get latest formValues from store to avoid closure issues
    const latestFormValues = useFormStore.getState().formValues
    const latestCursor = useFormStore.getState().currentCursor
    const params: Record<string, unknown> = {}
    let simpleParam: string | null = null

    methodConfig.fields.forEach(field => {
      if (field.type === 'separator') return

      const value = latestFormValues[field.name]
      if (value === undefined || value === '' || value === null) return

      // Skip false checkboxes (only include when true)
      if (field.type === 'checkbox' && !value) return

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
      } else if (field.type === 'checkbox') {
        processedValue = !!value
      } else if (field.type === 'booleanSelect') {
        // Convert string "true"/"false" to actual boolean
        if (value === 'true') processedValue = true
        else if (value === 'false') processedValue = false
        else return // Skip empty values
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

    // Add cursor if paginating (pass through as-is, server handles typing)
    if (latestCursor !== null && latestCursor !== undefined) {
      params.cursor = latestCursor
    }

    return {
      jsonrpc: '2.0',
      method: methodConfig.name,
      params: simpleParam !== null ? simpleParam : params,
      id
    }
  }, [methodConfig, currentMethod])

  // Execute request
  const handleExecute = async () => {
    if (!apiKey) {
      toast.error('Please enter an API key')
      return
    }

    setExecuting(true)

    try {
      const request = buildRequest()
      const { response, time } = await rpcMutation.mutateAsync({ apiKey, request })
      setResponse(response, time)

      if (response.error) {
        toast.error(`Error: ${response.error.message}`)
      }
    } catch (error) {
      toast.error(`Request failed: ${(error as Error).message}`)
    } finally {
      setExecuting(false)
    }
  }

  // Add to batch
  const handleAddToBatch = () => {
    const request = buildRequest(Date.now())

    const summary = summarizeRequest(request)
    addRequest({
      methodKey: currentMethod,
      methodName: methodConfig.name,
      request,
      summary
    })

    toast.success(`Added ${methodConfig.name} to batch`)
  }

  // Reset form
  const handleReset = () => {
    resetFormValues()
    resetPagination()
    toast('Form reset')
  }

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault()
        handleExecute()
      }
      if (e.key === 'Escape') {
        handleReset()
      }
    }

    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [handleExecute, handleReset])

  // Render a field based on its type
  const renderField = (field: FieldConfig) => {
    if (field.type === 'separator') {
      return (
        <div key={field.name} className="pt-3 pb-1 border-t border-gray-700 mt-2">
          <span className="text-xs font-medium text-purple-400 uppercase tracking-wider">
            {field.label}
          </span>
        </div>
      )
    }

    const value = formValues[field.name] ?? field.default ?? ''
    const onChange = (val: unknown) => setFormValue(field.name, val)

    const commonInputClasses = 'bg-gray-700 border-gray-600 focus:border-blue-500'

    return (
      <div key={field.name} className="space-y-1">
        <label className="text-xs text-gray-400 flex items-center space-x-1">
          <span>{field.label}</span>
          {field.required && <span className="text-red-400">*</span>}
        </label>

        {field.type === 'text' && (
          <Input
            value={value as string}
            onChange={(e) => onChange(e.target.value)}
            placeholder={field.placeholder}
            className={commonInputClasses}
          />
        )}

        {field.type === 'number' && (
          <Input
            type="number"
            value={value as string}
            onChange={(e) => onChange(e.target.value)}
            placeholder={field.placeholder}
            className={commonInputClasses}
          />
        )}

        {field.type === 'select' && (
          <Select
            value={value as string || '__none__'}
            onValueChange={(val) => onChange(val === '__none__' ? '' : val)}
          >
            <SelectTrigger className={commonInputClasses}>
              <SelectValue placeholder="-- Select --" />
            </SelectTrigger>
            <SelectContent>
              {field.options?.map((option) => (
                <SelectItem key={option || '__none__'} value={option || '__none__'}>
                  {option || '(none)'}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}

        {field.type === 'booleanSelect' && (
          <Select
            value={value as string || '__none__'}
            onValueChange={(val) => onChange(val === '__none__' ? '' : val)}
          >
            <SelectTrigger className={commonInputClasses}>
              <SelectValue placeholder="-- Select --" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="__none__">(none)</SelectItem>
              <SelectItem value="true">Yes</SelectItem>
              <SelectItem value="false">No</SelectItem>
            </SelectContent>
          </Select>
        )}

        {field.type === 'datetime' && (
          <DateTimePicker
            value={value as string}
            onChange={onChange}
          />
        )}

        {field.type === 'json' && (
          <Textarea
            value={value as string}
            onChange={(e) => onChange(e.target.value)}
            placeholder={field.placeholder || '["value1", "value2"]'}
            className={`${commonInputClasses} font-mono`}
            rows={2}
          />
        )}

        {field.type === 'combobox' && (
          <ComboboxSelector
            value={value as string}
            onChange={onChange}
            options={field.options || []}
            placeholder={field.placeholder}
          />
        )}

        {field.type === 'palletCombobox' && (
          <PalletSelector
            value={value as string}
            onChange={onChange}
            metaType={field.metaType || 'extrinsics'}
            placeholder={field.placeholder}
          />
        )}

        {field.type === 'methodCombobox' && (
          <MethodSelector
            value={value as string}
            onChange={onChange}
            pallet={(formValues[field.palletField || 'pallet'] as string) || ''}
            metaType={field.metaType || 'extrinsics'}
            placeholder={field.placeholder}
            label={field.label}
          />
        )}

        {field.type === 'checkbox' && (
          <label className="flex items-center space-x-2 cursor-pointer">
            <input
              type="checkbox"
              checked={!!value}
              onChange={(e) => onChange(e.target.checked)}
              className="w-4 h-4 rounded border-gray-600 bg-gray-700 text-blue-600 focus:ring-blue-500"
            />
            <span className="text-sm text-gray-300">{field.placeholder || 'Enable'}</span>
          </label>
        )}

        {field.type === 'storagePalletCombobox' && (
          <StoragePalletSelector
            value={value as string}
            onChange={onChange}
            placeholder={field.placeholder}
          />
        )}

        {field.type === 'storageLocationCombobox' && (
          <StorageLocationSelector
            value={value as string}
            onChange={onChange}
            pallet={(formValues[field.palletField || 'pallet'] as string) || ''}
            placeholder={field.placeholder}
          />
        )}

        {field.type === 'configRuleCombobox' && (
          <ConfigRuleSelector
            value={value as string}
            onChange={onChange}
            placeholder={field.placeholder}
          />
        )}
      </div>
    )
  }

  return (
    <Card className="bg-gray-800 border-gray-700">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg text-blue-400">{methodConfig.name}</CardTitle>
          <Badge className="bg-orange-900 text-orange-300">POST</Badge>
        </div>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {methodConfig.fields.length === 0 ? (
            <p className="text-gray-500 text-sm italic">No parameters required</p>
          ) : (
            methodConfig.fields.map(renderField)
          )}

          <div className="flex flex-wrap gap-2 pt-2">
            <Button
              size="sm"
              onClick={handleExecute}
              disabled={isExecuting}
              className="bg-blue-600 hover:bg-blue-700"
            >
              {isExecuting ? (
                <span className="flex items-center gap-1.5">
                  <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                  <span>Loading...</span>
                </span>
              ) : (
                <span className="flex items-center gap-1.5">
                  <Play className="h-3 w-3" />
                  <span>Execute</span>
                </span>
              )}
            </Button>

            <Button
              size="sm"
              variant="secondary"
              onClick={handleAddToBatch}
              className="bg-purple-600 hover:bg-purple-700"
            >
              <Plus className="h-3 w-3 mr-1.5" />
              Add to Batch
            </Button>

            <Button
              size="sm"
              variant="secondary"
              onClick={handleReset}
              className="bg-gray-600 hover:bg-gray-700"
            >
              <RotateCcw className="h-3 w-3 mr-1.5" />
              Reset
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

function summarizeRequest(request: RpcRequest): string {
  const params = request.params
  if (typeof params === 'string') {
    return params.substring(0, 20) + (params.length > 20 ? '...' : '')
  }
  if (typeof params !== 'object') return ''

  const parts: string[] = []
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      let displayValue = String(value)
      if (displayValue.length > 15) {
        displayValue = displayValue.substring(0, 15) + '...'
      }
      parts.push(`${key}=${displayValue}`)
    }
  }
  return parts.slice(0, 3).join(', ') + (parts.length > 3 ? '...' : '')
}
