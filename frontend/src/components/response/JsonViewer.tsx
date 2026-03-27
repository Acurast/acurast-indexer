import { useMemo, Fragment } from 'react'
import { toast } from 'sonner'
import { Copy } from 'lucide-react'
import { blake2b } from 'blakejs'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
  DropdownMenuLabel
} from '@/components/ui/dropdown-menu'
import { useFormStore } from '@/stores/formStore'
import type { MethodKey } from '@/config/methods'

// SS58 encoding with Substrate generic prefix (42)
const SS58_PREFIX = 42
const SS58_PREFIX_BYTES = new TextEncoder().encode('SS58PRE')
const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'

function hexToBytes(hex: string): Uint8Array {
  const cleanHex = hex.startsWith('0x') ? hex.slice(2) : hex
  const bytes = new Uint8Array(cleanHex.length / 2)
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(cleanHex.substr(i * 2, 2), 16)
  }
  return bytes
}

function bytesToHex(bytes: Uint8Array): string {
  return '0x' + Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('')
}

function base58Encode(bytes: Uint8Array): string {
  const digits = [0]
  for (const byte of bytes) {
    let carry = byte
    for (let j = 0; j < digits.length; j++) {
      carry += digits[j] << 8
      digits[j] = carry % 58
      carry = Math.floor(carry / 58)
    }
    while (carry > 0) {
      digits.push(carry % 58)
      carry = Math.floor(carry / 58)
    }
  }
  let result = ''
  for (let i = 0; i < bytes.length && bytes[i] === 0; i++) {
    result += BASE58_ALPHABET[0]
  }
  for (let i = digits.length - 1; i >= 0; i--) {
    result += BASE58_ALPHABET[digits[i]]
  }
  return result
}

function base58Decode(str: string): Uint8Array {
  const bytes: number[] = [0]
  for (const char of str) {
    const index = BASE58_ALPHABET.indexOf(char)
    if (index === -1) throw new Error('Invalid base58 character')
    let carry = index
    for (let j = 0; j < bytes.length; j++) {
      carry += bytes[j] * 58
      bytes[j] = carry & 0xff
      carry >>= 8
    }
    while (carry > 0) {
      bytes.push(carry & 0xff)
      carry >>= 8
    }
  }
  for (let i = 0; i < str.length && str[i] === BASE58_ALPHABET[0]; i++) {
    bytes.push(0)
  }
  return new Uint8Array(bytes.reverse())
}

// Proper SS58 checksum using blake2b-512
function ss58Checksum(payload: Uint8Array): Uint8Array {
  const input = new Uint8Array(SS58_PREFIX_BYTES.length + payload.length)
  input.set(SS58_PREFIX_BYTES)
  input.set(payload, SS58_PREFIX_BYTES.length)
  const hash = blake2b(input, undefined, 64)
  return hash.slice(0, 2)
}

function hexToSS58(hex: string): string | null {
  try {
    const addressBytes = hexToBytes(hex)
    if (addressBytes.length !== 32) return null

    const prefixBytes = new Uint8Array([SS58_PREFIX])
    const payload = new Uint8Array(prefixBytes.length + addressBytes.length)
    payload.set(prefixBytes)
    payload.set(addressBytes, prefixBytes.length)

    const checksum = ss58Checksum(payload)
    const full = new Uint8Array(payload.length + 2)
    full.set(payload)
    full.set(checksum, payload.length)

    return base58Encode(full)
  } catch {
    return null
  }
}

function ss58ToHex(ss58: string): string | null {
  try {
    const decoded = base58Decode(ss58)
    if (decoded.length < 35) return null // 1 prefix + 32 address + 2 checksum

    // Extract address bytes (skip prefix, remove checksum)
    const addressBytes = decoded.slice(1, 33)
    return bytesToHex(addressBytes)
  } catch {
    return null
  }
}

function isValidSS58(str: string): boolean {
  if (str.length < 46 || str.length > 50) return false
  return [...str].every(c => BASE58_ALPHABET.includes(c))
}

function isHex32Bytes(str: string): boolean {
  return /^(0x)?[0-9a-fA-F]{64}$/.test(str)
}

interface RpcAction {
  label: string
  method: MethodKey
  values: Record<string, unknown>
}

function getActionsForValue(value: string, path: string, _context: Record<string, unknown>): RpcAction[] {
  const actions: RpcAction[] = []

  // Check for 64-char hex (address/tx_hash)
  if (/^0x[0-9a-fA-F]{64}$/.test(value) || /^[0-9a-fA-F]{64}$/.test(value)) {
    const normalizedValue = value.startsWith('0x') ? value : `0x${value}`
    const isTxHash = path.includes('tx_hash')

    if (isTxHash) {
      actions.push({
        label: 'View Extrinsic by Hash',
        method: 'extrinsicByHash',
        values: { tx_hash: normalizedValue }
      })
    } else {
      // It's an address
      actions.push({
        label: 'Get Extrinsics by Account',
        method: 'extrinsics',
        values: { account_id: normalizedValue }
      })
      actions.push({
        label: 'Get Extrinsic Addresses',
        method: 'extrinsicAddresses',
        values: { account_id: normalizedValue }
      })
      actions.push({
        label: 'Get Jobs by Address',
        method: 'jobs',
        values: { address: normalizedValue }
      })
      actions.push({
        label: 'Get Storage Snapshots',
        method: 'storageSnapshots',
        values: { 'extrinsic.account_id': normalizedValue }
      })
    }
  }

  return actions
}

// Actions for extrinsic_index when block_number is available in context
function getExtrinsicIndexActions(blockNumber: number, extrinsicIndex: number): RpcAction[] {
  return [
    {
      label: 'View Extrinsic',
      method: 'extrinsic',
      values: { block_number: blockNumber, index: extrinsicIndex }
    },
    {
      label: 'View Block Extrinsics',
      method: 'extrinsics',
      values: { block_from: blockNumber, block_to: blockNumber }
    }
  ]
}

// Actions for event index when block_number and extrinsic_index are available in context
function getEventIndexActions(blockNumber: number, extrinsicIndex: number, eventIndex: number): RpcAction[] {
  return [
    {
      label: 'View Event',
      method: 'event',
      values: { block_number: blockNumber, extrinsic_index: extrinsicIndex, index: eventIndex }
    },
    {
      label: 'View Extrinsic',
      method: 'extrinsic',
      values: { block_number: blockNumber, index: extrinsicIndex }
    }
  ]
}

function getPalletActions(pallet: number): RpcAction[] {
  return [
    {
      label: 'Get Events by Pallet',
      method: 'events',
      values: { pallet }
    },
    {
      label: 'Get Extrinsics by Pallet',
      method: 'extrinsics',
      values: { pallet }
    }
  ]
}

function getPalletVariantActions(pallet: number, variant: number): RpcAction[] {
  return [
    {
      label: 'Get Events by Pallet + Variant',
      method: 'events',
      values: { pallet, variant }
    }
  ]
}

interface ValueWithActionsProps {
  value: string
  actions: RpcAction[]
  colorClass: string
  onNavigate: (method: MethodKey, values: Record<string, unknown>) => void
}

function ValueWithActions({ value, actions, colorClass, onNavigate }: ValueWithActionsProps) {
  const isHex = isHex32Bytes(value)
  const isSS58 = isValidSS58(value)
  const hasCopyOptions = isHex || isSS58

  const handleCopy = (text: string, label: string) => {
    navigator.clipboard.writeText(text)
    toast.success(`Copied ${label}`)
  }

  // If no actions and no special copy options, just render the value
  if (actions.length === 0 && !hasCopyOptions) {
    return <span className={colorClass}>"{value}"</span>
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <span
          className={`${colorClass} cursor-pointer rounded px-0.5 border border-gray-600 hover:border-blue-400 hover:bg-blue-500/10 transition-colors`}
          title="Click for actions"
        >
          "{value}"
        </span>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="min-w-[200px]">
        {/* Copy options */}
        <DropdownMenuLabel className="text-xs text-gray-400">Copy</DropdownMenuLabel>
        <DropdownMenuItem
          onClick={() => handleCopy(value, 'value')}
          className="text-sm cursor-pointer"
        >
          <Copy className="mr-2 h-3 w-3" />
          Copy
        </DropdownMenuItem>
        {isHex && (
          <DropdownMenuItem
            onClick={() => {
              const ss58 = hexToSS58(value)
              if (ss58) {
                handleCopy(ss58, 'as SS58 address')
              } else {
                toast.error('Failed to convert to SS58')
              }
            }}
            className="text-sm cursor-pointer"
          >
            <Copy className="mr-2 h-3 w-3" />
            Copy as SS58 Address
          </DropdownMenuItem>
        )}
        {isSS58 && (
          <DropdownMenuItem
            onClick={() => {
              const hex = ss58ToHex(value)
              if (hex) {
                handleCopy(hex, 'as hex')
              } else {
                toast.error('Failed to convert to hex')
              }
            }}
            className="text-sm cursor-pointer"
          >
            <Copy className="mr-2 h-3 w-3" />
            Copy as Hex
          </DropdownMenuItem>
        )}

        {/* RPC navigation actions */}
        {actions.length > 0 && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuLabel className="text-xs text-gray-400">Jump to RPC</DropdownMenuLabel>
            {actions.map((action, i) => (
              <DropdownMenuItem
                key={i}
                onClick={() => {
                  onNavigate(action.method, action.values)
                  toast.success(`Navigated to ${action.method}`)
                }}
                className="text-sm cursor-pointer"
              >
                {action.label}
              </DropdownMenuItem>
            ))}
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

interface NumberWithActionsProps {
  value: number
  actions: RpcAction[]
  onNavigate: (method: MethodKey, values: Record<string, unknown>) => void
}

function NumberWithActions({ value, actions, onNavigate }: NumberWithActionsProps) {
  if (actions.length === 0) {
    return <span className="text-blue-400">{value}</span>
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <span
          className="text-cyan-400 cursor-pointer rounded px-0.5 border border-gray-600 hover:border-blue-400 hover:bg-blue-500/10 transition-colors"
          title="Click for actions"
        >
          {value}
        </span>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="min-w-[200px]">
        <DropdownMenuLabel className="text-xs text-gray-400">Jump to RPC</DropdownMenuLabel>
        <DropdownMenuSeparator />
        {actions.map((action, i) => (
          <DropdownMenuItem
            key={i}
            onClick={() => {
              onNavigate(action.method, action.values)
              toast.success(`Navigated to ${action.method}`)
            }}
            className="text-sm cursor-pointer"
          >
            {action.label}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

interface JsonNodeProps {
  data: unknown
  indent: number
  path: string
  context: Record<string, unknown>
  onNavigate: (method: MethodKey, values: Record<string, unknown>) => void
}

function JsonNode({ data, indent, path, context, onNavigate }: JsonNodeProps) {
  const spaces = '  '.repeat(indent)

  if (data === null) {
    return <span className="text-gray-500">null</span>
  }

  if (typeof data === 'boolean') {
    return <span className="text-yellow-400">{String(data)}</span>
  }

  if (typeof data === 'number') {
    const key = path.split('.').pop()

    // Check for extrinsic_index with block_number in context -> link to extrinsic
    if (key === 'extrinsic_index' && typeof context.block_number === 'number') {
      const actions = getExtrinsicIndexActions(context.block_number, data)
      return <NumberWithActions value={data} actions={actions} onNavigate={onNavigate} />
    }

    // Check for index (event index) with block_number and extrinsic_index in context -> link to event
    if (key === 'index' && typeof context.block_number === 'number' && typeof context.extrinsic_index === 'number') {
      const actions = getEventIndexActions(context.block_number, context.extrinsic_index, data)
      return <NumberWithActions value={data} actions={actions} onNavigate={onNavigate} />
    }

    // Check if this is a pallet or variant field
    if (key === 'pallet') {
      const actions = getPalletActions(data)
      return <NumberWithActions value={data} actions={actions} onNavigate={onNavigate} />
    }
    if (key === 'variant' && context.pallet !== undefined) {
      const actions = getPalletVariantActions(context.pallet as number, data)
      return <NumberWithActions value={data} actions={actions} onNavigate={onNavigate} />
    }
    return <span className="text-blue-400">{data}</span>
  }

  if (typeof data === 'string') {
    const actions = getActionsForValue(data, path, context)
    const isTxHash = path.includes('tx_hash')
    const isAddress = /^0x[0-9a-fA-F]{64}$/.test(data) || /^[0-9a-fA-F]{64}$/.test(data)

    let colorClass = 'text-green-400'
    if (isTxHash) colorClass = 'text-pink-400'
    else if (isAddress) colorClass = 'text-purple-400'

    return <ValueWithActions value={data} actions={actions} colorClass={colorClass} onNavigate={onNavigate} />
  }

  if (Array.isArray(data)) {
    if (data.length === 0) return <>{'[]'}</>
    return (
      <>
        {'[\n'}
        {data.map((item, i) => (
          <Fragment key={i}>
            {spaces}{'  '}
            <JsonNode
              data={item}
              indent={indent + 1}
              path={`${path}[${i}]`}
              context={context}
              onNavigate={onNavigate}
            />
            {i < data.length - 1 ? ',\n' : '\n'}
          </Fragment>
        ))}
        {spaces}{']'}
      </>
    )
  }

  if (typeof data === 'object') {
    const keys = Object.keys(data)
    if (keys.length === 0) return <>{'{}' }</>

    const record = data as Record<string, unknown>
    // Build context for lookups (pallet/variant, block_number/extrinsic_index/index)
    const newContext = { ...context }
    if ('pallet' in record && typeof record.pallet === 'number') {
      newContext.pallet = record.pallet
    }
    if ('block_number' in record && typeof record.block_number === 'number') {
      newContext.block_number = record.block_number
    }
    if ('extrinsic_index' in record && typeof record.extrinsic_index === 'number') {
      newContext.extrinsic_index = record.extrinsic_index
    }

    return (
      <>
        {'{\n'}
        {keys.map((key, i) => {
          const newPath = path ? `${path}.${key}` : key
          return (
            <Fragment key={key}>
              {spaces}{'  '}<span className="text-gray-400">"{key}"</span>:{' '}
              <JsonNode
                data={record[key]}
                indent={indent + 1}
                path={newPath}
                context={newContext}
                onNavigate={onNavigate}
              />
              {i < keys.length - 1 ? ',\n' : '\n'}
            </Fragment>
          )
        })}
        {spaces}{'}'}
      </>
    )
  }

  return <>{String(data)}</>
}

interface JsonViewerProps {
  data: unknown
}

export function JsonViewer({ data }: JsonViewerProps) {
  const { setMethodWithValues } = useFormStore()

  const handleNavigate = useMemo(() => {
    return (method: MethodKey, values: Record<string, unknown>) => {
      setMethodWithValues(method, values)
    }
  }, [setMethodWithValues])

  return (
    <pre className="text-sm text-gray-300 whitespace-pre-wrap break-words">
      <JsonNode
        data={data}
        indent={0}
        path=""
        context={{}}
        onNavigate={handleNavigate}
      />
    </pre>
  )
}
