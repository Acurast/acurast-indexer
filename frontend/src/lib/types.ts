export interface RpcRequest {
  jsonrpc: '2.0'
  method: string
  params: Record<string, unknown> | string
  id: number
}

export interface RpcResponse<T = unknown> {
  jsonrpc: '2.0'
  result?: T
  error?: {
    code: number
    message: string
  }
  id: number
}

export interface PagedResult<T> {
  items: T[]
  cursor: unknown
}

export interface BatchRequest {
  id: number
  methodKey: string
  methodName: string
  request: RpcRequest
  summary: string
}

export interface FieldConfig {
  name: string
  type: 'text' | 'number' | 'select' | 'booleanSelect' | 'datetime' | 'json' | 'combobox' | 'palletCombobox' | 'methodCombobox' | 'storagePalletCombobox' | 'storageLocationCombobox' | 'configRuleCombobox' | 'separator' | 'checkbox'
  label: string
  placeholder?: string
  required?: boolean
  default?: unknown
  options?: string[]
  palletField?: string
  metaType?: 'extrinsics' | 'events'
  nested?: string
  isParam?: boolean
}

export interface MethodConfig {
  name: string
  category: 'blocks' | 'extrinsics' | 'events' | 'storage' | 'jobs' | 'epochs' | 'managers' | 'staking'
  fields: FieldConfig[]
}

export interface Example {
  method: string
  values: Record<string, unknown>
  description: string
}

export type MethodCategory = 'blocks' | 'extrinsics' | 'events' | 'storage' | 'jobs' | 'epochs' | 'managers' | 'staking'

export type PalletMetadata = Record<string, string[]>
