import type { FieldConfig, MethodConfig, PalletPair, RpcRequest } from './types'

/**
 * Convert flat form-state values into a fully-typed JSON-RPC params object.
 */
export function buildRpcRequest(
  methodConfig: MethodConfig,
  formValues: Record<string, unknown>,
  options: { id?: number; cursor?: unknown } = {},
): RpcRequest {
  const params: Record<string, unknown> = {}
  let simpleParam: string | null = null

  methodConfig.fields.forEach((field: FieldConfig) => {
    if (field.type === 'separator') return

    const value = formValues[field.name]
    if (value === undefined || value === '' || value === null) return

    // Unchecked checkboxes mean "filter not set"; never include them.
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
    } else if (field.type === 'palletMethodPairs') {
      const arr = (Array.isArray(value) ? value : []) as PalletPair[]
      const subKey: 'method' | 'variant' = field.metaType === 'events' ? 'variant' : 'method'
      const cleaned = arr
        .filter((p) => p && typeof p.pallet === 'string' && p.pallet !== '')
        .map((p) => {
          const out: Record<string, string> = { pallet: p.pallet as string }
          const sub = p[subKey]
          if (typeof sub === 'string' && sub !== '') out[subKey] = sub
          return out
        })
      if (cleaned.length === 0) return
      processedValue = cleaned
    } else if (field.type === 'addressList') {
      const arr = (Array.isArray(value) ? value : []) as string[]
      const cleaned = arr.map((a) => a.trim()).filter((a) => a !== '')
      if (cleaned.length === 0) return
      processedValue = cleaned
    } else if (field.type === 'datetime') {
      processedValue = new Date(value as string).toISOString()
    } else if (field.type === 'checkbox') {
      processedValue = !!value
    } else if (field.type === 'booleanSelect') {
      // Form state stores the literal strings "true"/"false"; coerce to bool.
      if (value === 'true') processedValue = true
      else if (value === 'false') processedValue = false
      else return // empty value: skip
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

  // Drop empty nested objects so the server sees missing-and-default rather
  // than {} (some endpoints require all-or-nothing nested filters).
  Object.keys(params).forEach(key => {
    const v = params[key]
    if (typeof v === 'object' && v !== null && !Array.isArray(v)) {
      if (Object.keys(v as object).length === 0) {
        delete params[key]
      }
    }
  })

  if (options.cursor !== null && options.cursor !== undefined) {
    params.cursor = options.cursor
  }

  return {
    jsonrpc: '2.0',
    method: methodConfig.name,
    params: simpleParam !== null ? simpleParam : params,
    id: options.id ?? 1,
  }
}
