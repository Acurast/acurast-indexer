import type { ReactNode } from 'react'
import type { MethodKey } from '@/config/methods'
import { RpcExample } from './RpcExample'

interface DocSection {
  title: string
  description: ReactNode
  parameters?: { name: string; type: string; required?: boolean; description: string }[]
  examples?: { title: string; description?: string; params: Record<string, unknown> }[]
  notes?: string[]
}

export const rpcDocs: Partial<Record<MethodKey, DocSection>> = {
  // ============================================
  // BLOCKS
  // ============================================
  blocks: {
    title: 'Get Blocks',
    description: (
      <>
        <p>Retrieves a paginated list of blocks from the blockchain. Supports filtering by block range, time range, and pagination.</p>
      </>
    ),
    parameters: [
      { name: 'block_from', type: 'number', description: 'Minimum block number (inclusive)' },
      { name: 'block_to', type: 'number', description: 'Maximum block number (inclusive)' },
      { name: 'time_from', type: 'datetime', description: 'Filter blocks after this timestamp (ISO 8601)' },
      { name: 'time_to', type: 'datetime', description: 'Filter blocks before this timestamp (ISO 8601)' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc" (default: "desc")' },
      { name: 'limit', type: 'number', description: 'Maximum results to return (1-1000, default: 10)' },
      { name: 'cursor', type: 'number', description: 'Block number cursor for pagination' },
    ],
    examples: [
      {
        title: 'Latest 10 blocks',
        params: { limit: 10 }
      },
      {
        title: 'Blocks in range',
        description: 'Get blocks in a specific range',
        params: { block_from: 8915500, block_to: 8915600 }
      },
    ],
  },

  block: {
    title: 'Get Block by Hash',
    description: (
      <p>Retrieves a single block by its hash. Returns detailed block information including parent hash, state root, and extrinsics root.</p>
    ),
    parameters: [
      { name: 'hash', type: 'string', required: true, description: 'The block hash (0x-prefixed hex string)' },
    ],
    examples: [
      {
        title: 'Get specific block',
        params: { hash: '0xfd20604e2c0061e10dda7f2894f9558b0e7d67d45b023046502507f36786827f' }
      },
    ],
  },

  blocksCount: {
    title: 'Get Blocks Count',
    description: (
      <p>Returns the total count of blocks matching the specified filters. Useful for statistics and pagination calculations.</p>
    ),
    parameters: [
      { name: 'block_from', type: 'number', description: 'Minimum block number (inclusive)' },
      { name: 'block_to', type: 'number', description: 'Maximum block number (inclusive)' },
    ],
    examples: [
      {
        title: 'Count all blocks',
        params: {}
      },
      {
        title: 'Count blocks in range',
        params: { block_from: 8915500, block_to: 8916000 }
      },
    ],
  },

  // ============================================
  // EXTRINSICS
  // ============================================
  extrinsics: {
    title: 'Get Extrinsics',
    description: (
      <>
        <p>Retrieves a paginated list of extrinsics (transactions). Supports filtering by block range, pallet/method, and account.</p>
        <p className="mt-2 text-gray-400">Pallet and method can be specified as names (e.g., "Acurast", "register") or numeric indices.</p>
      </>
    ),
    parameters: [
      { name: 'block_from', type: 'number', description: 'Minimum block number' },
      { name: 'block_to', type: 'number', description: 'Maximum block number' },
      { name: 'pallet', type: 'string|number', description: 'Filter by pallet name or index' },
      { name: 'method', type: 'string|number', description: 'Filter by method name or index (requires pallet)' },
      { name: 'account_id', type: 'string', description: 'Filter by signer account (hex or SS58)' },
      { name: 'events', type: 'boolean', description: 'Include events for each extrinsic (default: false)' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc" (default: "desc")' },
      { name: 'limit', type: 'number', description: 'Maximum results (default: 10)' },
      { name: 'cursor', type: 'object', description: 'Cursor object with block_number and index' },
    ],
    examples: [
      {
        title: 'Latest extrinsics',
        params: { limit: 10 }
      },
      {
        title: 'Acurast register calls',
        description: 'All job registration extrinsics',
        params: { pallet: 'Acurast', method: 'register', limit: 20 }
      },
      {
        title: 'With events',
        description: 'Include associated events',
        params: { limit: 5, events: true }
      },
    ],
  },

  extrinsic: {
    title: 'Get Extrinsic',
    description: (
      <p>Retrieves a single extrinsic by its block number and index within the block.</p>
    ),
    parameters: [
      { name: 'block_number', type: 'number', required: true, description: 'The block number containing the extrinsic' },
      { name: 'index', type: 'number', required: true, description: 'The index of the extrinsic within the block' },
      { name: 'events', type: 'boolean', description: 'Include associated events (default: false)' },
    ],
    examples: [
      {
        title: 'Get by position',
        params: { block_number: 8917218, index: 2 }
      },
      {
        title: 'With events',
        params: { block_number: 8917218, index: 2, events: true }
      },
    ],
  },

  extrinsicByHash: {
    title: 'Get Extrinsic by Hash',
    description: (
      <p>Retrieves a single extrinsic by its transaction hash. The hash is the blake2b hash of the encoded extrinsic.</p>
    ),
    parameters: [
      { name: 'tx_hash', type: 'string', required: true, description: 'The transaction hash (0x-prefixed)' },
      { name: 'events', type: 'boolean', description: 'Include associated events (default: false)' },
    ],
    examples: [
      {
        title: 'Get by hash',
        params: { tx_hash: '0x6af114ee49f2dfd000a6101edf6a5d32dfd028b46c89c091cb90547949bed65d' }
      },
    ],
  },

  extrinsicsCount: {
    title: 'Get Extrinsics Count',
    description: (
      <p>Returns the count of extrinsics matching the specified filters.</p>
    ),
    parameters: [
      { name: 'block_from', type: 'number', description: 'Minimum block number' },
      { name: 'block_to', type: 'number', description: 'Maximum block number' },
      { name: 'pallet', type: 'string|number', description: 'Filter by pallet' },
      { name: 'method', type: 'string|number', description: 'Filter by method' },
      { name: 'account_id', type: 'string', description: 'Filter by signer account' },
    ],
    examples: [
      {
        title: 'Count Acurast calls',
        params: { pallet: 'Acurast' }
      },
    ],
  },

  extrinsicAddresses: {
    title: 'Get Extrinsic Addresses',
    description: (
      <>
        <p>Extracts all addresses found within extrinsics. Useful for finding all accounts involved in specific transactions.</p>
        <p className="mt-2 text-gray-400">This scans the extrinsic data for address-like values and returns them with context.</p>
      </>
    ),
    parameters: [
      { name: 'block_from', type: 'number', description: 'Minimum block number' },
      { name: 'block_to', type: 'number', description: 'Maximum block number' },
      { name: 'pallet', type: 'string|number', description: 'Filter by pallet' },
      { name: 'method', type: 'string|number', description: 'Filter by method' },
      { name: 'account_id', type: 'string', description: 'Filter by specific address' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc"' },
      { name: 'limit', type: 'number', description: 'Maximum results' },
      { name: 'cursor', type: 'object', description: 'Pagination cursor' },
    ],
    examples: [
      {
        title: 'Addresses in recent extrinsics',
        params: { limit: 20 }
      },
    ],
  },

  extrinsicMetadata: {
    title: 'Get Extrinsic Metadata',
    description: (
      <p>Returns the metadata for all pallets and their extrinsic methods. Useful for discovering available pallet/method combinations and their indices.</p>
    ),
    parameters: [],
    examples: [
      {
        title: 'Get all metadata',
        params: {}
      },
    ],
    notes: [
      'Returns a map of pallet names to their methods with indices',
      'Indices are runtime-specific and may change between versions',
    ],
  },

  // ============================================
  // EVENTS
  // ============================================
  events: {
    title: 'Get Events',
    description: (
      <>
        <p>Retrieves blockchain events. Events are emitted by pallets during extrinsic execution and provide detailed information about state changes.</p>
      </>
    ),
    parameters: [
      { name: 'block_from', type: 'number', description: 'Minimum block number' },
      { name: 'block_to', type: 'number', description: 'Maximum block number' },
      { name: 'pallet', type: 'string|number', description: 'Filter by pallet name or index' },
      { name: 'variant', type: 'string|number', description: 'Filter by event variant name or index' },
      { name: 'account_id', type: 'string', description: 'Filter by account ID (hex or SS58)' },
      { name: 'data', type: 'json', description: 'Filter by data (JSON containment)' },
      { name: 'job', type: 'string', description: 'Filter by job (SS58 or hex) or specific job (address#seq_id)' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc"' },
      { name: 'limit', type: 'number', description: 'Maximum results' },
      { name: 'cursor', type: 'object', description: 'Cursor with block_number, extrinsic_index, index' },
    ],
    examples: [
      {
        title: 'Latest events',
        params: { limit: 20 }
      },
      {
        title: 'Transfer events',
        params: { pallet: 'Balances', variant: 'Transfer', limit: 10 }
      },
      {
        title: 'Events for a job',
        description: 'Events associated with a specific job',
        params: { job: '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY#123', limit: 10 }
      },
    ],
  },

  event: {
    title: 'Get Event',
    description: (
      <p>Retrieves a single event by its unique identifier: block number, extrinsic index, and event index within the extrinsic.</p>
    ),
    parameters: [
      { name: 'block_number', type: 'number', required: true, description: 'The block containing the event' },
      { name: 'extrinsic_index', type: 'number', required: true, description: 'The extrinsic index within the block' },
      { name: 'index', type: 'number', required: true, description: 'The event index within the extrinsic' },
    ],
    examples: [
      {
        title: 'Get specific event',
        params: { block_number: 8917224, extrinsic_index: 2, index: 4 }
      },
    ],
  },

  eventMetadata: {
    title: 'Get Event Metadata',
    description: (
      <p>Returns metadata for all event types across all pallets. Useful for discovering available event variants.</p>
    ),
    parameters: [],
    examples: [
      {
        title: 'Get all event metadata',
        params: {}
      },
    ],
  },

  // ============================================
  // STORAGE
  // ============================================
  storageSnapshots: {
    title: 'Get Storage Snapshots',
    description: (
      <>
        <p>Retrieves historical snapshots of on-chain storage. Storage snapshots capture the state of specific storage locations at particular blocks.</p>
        <p className="mt-2 text-gray-400">Supports filtering by storage location, keys, data content, and the triggering extrinsic/event. Use sampling to keep only some snapshots by time period.</p>
      </>
    ),
    parameters: [
      { name: 'block_from', type: 'number', description: 'Minimum block number' },
      { name: 'block_to', type: 'number', description: 'Maximum block number' },
      { name: 'time_from', type: 'datetime', description: 'Filter by timestamp (ISO 8601)' },
      { name: 'time_to', type: 'datetime', description: 'Filter by timestamp (ISO 8601)' },
      { name: 'pallet', type: 'string|number', description: 'Storage pallet name or index' },
      { name: 'storage_location', type: 'string', description: 'Storage location name (e.g., "StoredJobRegistration")' },
      { name: 'storage_keys', type: 'json', description: 'JSON array of storage keys to match (containment)' },
      { name: 'data', type: 'json', description: 'JSON object to match in data (containment)' },
      { name: 'config_rule', type: 'string', description: 'Filter by indexer config rule name' },
      { name: 'exclude_deleted', type: 'boolean', description: 'Exclude entries that were later deleted' },
      { name: 'extrinsic.pallet', type: 'string', description: 'Filter by triggering extrinsic pallet' },
      { name: 'extrinsic.method', type: 'string', description: 'Filter by triggering extrinsic method' },
      { name: 'extrinsic.account_id', type: 'string', description: 'Filter by triggering account' },
      { name: 'event.pallet', type: 'string', description: 'Filter by triggering event pallet' },
      { name: 'event.variant', type: 'string', description: 'Filter by triggering event variant' },
      { name: 'include_epochs', type: 'boolean', description: 'Include nested epoch info (epoch, epoch_start, epoch_end, epoch_start_time) in response' },
      { name: 'sample', type: 'string', description: 'Sample by time period: per_epoch, day (~8 epochs), week (~56 epochs), month (~240 epochs)' },
      { name: 'fill', type: 'boolean', description: 'Fill missing time periods with last known value (only with sample)' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc"' },
      { name: 'limit', type: 'number', description: 'Maximum results' },
      { name: 'cursor', type: 'number', description: 'Snapshot ID cursor' },
    ],
    examples: [
      {
        title: 'Latest storage changes',
        params: { limit: 10 }
      },
      {
        title: 'Job registrations',
        description: 'Storage snapshots for job registrations',
        params: { pallet: 'Acurast', storage_location: 'StoredJobRegistration', limit: 10 }
      },
      {
        title: 'Active entries only',
        description: 'Exclude entries that were later deleted',
        params: { exclude_deleted: true, limit: 10 }
      },
      {
        title: 'Daily samples with fill',
        description: 'One snapshot per day, filling gaps with previous values',
        params: { sample: 'day', fill: true, limit: 30 }
      },
    ],
    notes: [
      'The data filter uses PostgreSQL JSONB containment (@>)',
      'Storage keys are stored as JSON arrays',
      'Deleted entries have data set to JSON null',
      'When using sample, response format changes to a dictionary keyed by epoch number',
      'Epoch durations: day ~8, week ~56, month ~240 epochs (~3 hours per epoch)',
      'When fill is enabled, synthetic/filled rows have negative IDs (the negated epoch bucket number)',
    ],
  },

  // ============================================
  // JOBS
  // ============================================
  jobs: {
    title: 'Get Jobs',
    description: (
      <>
        <p>Retrieves Acurast job registrations. Jobs are tasks registered on the Acurast network for execution by processors.</p>
      </>
    ),
    parameters: [
      { name: 'block_from', type: 'number', description: 'Minimum block number' },
      { name: 'block_to', type: 'number', description: 'Maximum block number' },
      { name: 'job', type: 'string', description: 'Filter by job (SS58 or hex) or specific job (address#seq_id)' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc"' },
      { name: 'limit', type: 'number', description: 'Maximum results' },
      { name: 'cursor', type: 'object', description: 'Pagination cursor' },
    ],
    examples: [
      {
        title: 'Latest jobs',
        params: { limit: 20 }
      },
      {
        title: 'By address (SS58)',
        params: { job: '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY', limit: 20 }
      },
      {
        title: 'Specific job by ID',
        description: 'Filter by address and sequence ID',
        params: { job: '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY#123' }
      },
    ],
  },

  // ============================================
  // EPOCHS
  // ============================================
  epochs: {
    title: 'Get Epochs',
    description: (
      <>
        <p>Retrieves epoch information. Epochs are time periods used for reward calculations and validator rotations.</p>
      </>
    ),
    parameters: [
      { name: 'epoch_from', type: 'number', description: 'Minimum epoch number' },
      { name: 'epoch_to', type: 'number', description: 'Maximum epoch number' },
      { name: 'block_from', type: 'number', description: 'Filter by epoch start block' },
      { name: 'block_to', type: 'number', description: 'Filter by epoch start block' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc"' },
      { name: 'limit', type: 'number', description: 'Maximum results' },
      { name: 'cursor', type: 'number', description: 'Epoch number cursor' },
    ],
    examples: [
      {
        title: 'Latest epochs',
        params: { limit: 10 }
      },
      {
        title: 'Epoch range',
        params: { epoch_from: 9900, epoch_to: 9907 }
      },
    ],
  },

  // ============================================
  // METRICS
  // ============================================
  epochMetrics: {
    title: 'Get Metrics by Manager',
    description: (
      <>
        <p>Retrieves performance metrics for a specific manager address across epochs. Includes processor statistics aggregated per epoch.</p>
        <p className="mt-2 text-gray-400">Results can be visualized in the metrics graph below the response.</p>
      </>
    ),
    parameters: [
      { name: 'manager', type: 'string', required: true, description: 'Manager address (hex or SS58, required)' },
      { name: 'epoch_from', type: 'number', description: 'Minimum epoch number' },
      { name: 'epoch_to', type: 'number', description: 'Maximum epoch number' },
      { name: 'limit', type: 'number', description: 'Maximum epochs to return' },
      { name: 'cursor', type: 'number', description: 'Epoch cursor' },
    ],
    examples: [
      {
        title: 'Manager metrics',
        description: 'Replace with a valid manager address',
        params: { manager: '0xbfc6cb913b74b07f571609da3df04fb20ae5c450c66d1ca0195bc2e8b6887b92', limit: 16 }
      },
    ],
  },

  processorMetrics: {
    title: 'Get Metrics by Processor',
    description: (
      <>
        <p>Retrieves performance metrics for a specific processor address across epochs.</p>
      </>
    ),
    parameters: [
      { name: 'processor', type: 'string', required: true, description: 'Processor address (hex or SS58, required)' },
      { name: 'epoch_from', type: 'number', description: 'Minimum epoch number' },
      { name: 'epoch_to', type: 'number', description: 'Maximum epoch number' },
      { name: 'limit', type: 'number', description: 'Maximum epochs to return' },
      { name: 'cursor', type: 'number', description: 'Epoch cursor' },
    ],
    examples: [
      {
        title: 'Processor metrics',
        description: 'Replace with a valid processor address',
        params: { processor: '0xbfc6cb913b74b07f571609da3df04fb20ae5c450c66d1ca0195bc2e8b6887b92', limit: 16 }
      },
    ],
  },

  processorsCountByEpoch: {
    title: 'Get Processors Count by Epoch',
    description: (
      <>
        <p>Returns the count of distinct processors that sent at least one heartbeat per epoch. Useful for monitoring network participation over time.</p>
        <p className="mt-2 text-gray-400">Results are visualized in the graph below the response.</p>
      </>
    ),
    parameters: [
      { name: 'epoch_from', type: 'number', description: 'Minimum epoch number' },
      { name: 'epoch_to', type: 'number', description: 'Maximum epoch number' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc" (by epoch)' },
      { name: 'limit', type: 'number', description: 'Maximum epochs to return (default: 16)' },
      { name: 'cursor', type: 'number', description: 'Epoch cursor for pagination' },
    ],
    examples: [
      {
        title: 'Recent epochs',
        description: 'Get processor counts for recent epochs',
        params: { limit: 16 }
      },
      {
        title: 'Specific epoch range',
        description: 'Get processor counts for epochs 9900-9910',
        params: { epoch_from: 9900, epoch_to: 9910 }
      },
    ],
  },

  // ============================================
  // STAKING / COMMITMENTS
  // ============================================
  commitments: {
    title: 'Get Commitments',
    description: (
      <>
        <p>Retrieves stake commitments from the AcurastCompute pallet. Commitments represent staked tokens by validators/processors.</p>
        <p className="mt-2 text-gray-400">Returns denormalized commitment data with ownership info, stake amounts, and delegation statistics. Supports flexible sorting by any numeric column.</p>
      </>
    ),
    parameters: [
      { name: 'commitment_id', type: 'number', description: 'Filter by specific commitment ID' },
      { name: 'committer_address', type: 'string', description: 'Filter by committer address (hex or SS58)' },
      { name: 'manager_id', type: 'number', description: 'Filter by manager ID' },
      { name: 'manager_address', type: 'string', description: 'Filter by manager address (hex or SS58)' },
      { name: 'is_active', type: 'boolean', description: 'Filter by active status (true/false)' },
      { name: 'in_cooldown', type: 'boolean', description: 'Filter by cooldown status (true = in cooldown, false = not)' },
      { name: 'min_stake_amount', type: 'string', description: 'Minimum stake amount (raw value)' },
      { name: 'max_stake_amount', type: 'string', description: 'Maximum stake amount (raw value)' },
      { name: 'min_delegations_total_amount', type: 'string', description: 'Minimum total delegations amount' },
      { name: 'max_delegations_total_amount', type: 'string', description: 'Maximum total delegations amount' },
      { name: 'min_commission', type: 'string', description: 'Minimum commission (Perbill)' },
      { name: 'max_commission', type: 'string', description: 'Maximum commission (Perbill)' },
      { name: 'min_delegation_utilization', type: 'string', description: 'Minimum delegation utilization (Perbill)' },
      { name: 'max_delegation_utilization', type: 'string', description: 'Maximum delegation utilization (Perbill)' },
      { name: 'min_combined_utilization', type: 'string', description: 'Minimum combined utilization (Perbill)' },
      { name: 'max_combined_utilization', type: 'string', description: 'Maximum combined utilization (Perbill)' },
      { name: 'min_remaining_capacity', type: 'string', description: 'Minimum remaining capacity' },
      { name: 'max_remaining_capacity', type: 'string', description: 'Maximum remaining capacity' },
      { name: 'min_cooldown_period', type: 'string', description: 'Minimum cooldown period' },
      { name: 'max_cooldown_period', type: 'string', description: 'Maximum cooldown period' },
      { name: 'order_by', type: 'string', description: 'Column to sort by: stake_amount, delegations_total_amount, commission, delegation_utilization, combined_utilization, remaining_capacity, cooldown_period, epoch, block_number, commitment_id, combined_stake (stake + delegations), combined_weight (delegations_slash_weight + self_slash_weight)' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc" (default: desc)' },
      { name: 'limit', type: 'number', description: 'Maximum results (default: 50)' },
      { name: 'cursor', type: 'number', description: 'Commitment ID cursor for pagination' },
    ],
    examples: [
      {
        title: 'Top stakers',
        description: 'Get commitments sorted by stake amount',
        params: { order_by: 'stake_amount', sort_order: 'desc', limit: 20 }
      },
      {
        title: 'Active commitments',
        description: 'Get only active commitments',
        params: { is_active: true, limit: 50 }
      },
      {
        title: 'By manager',
        description: 'Get commitments for a specific manager',
        params: { manager_id: 123, limit: 10 }
      },
      {
        title: 'High utilization',
        description: 'Commitments with >50% combined utilization',
        params: { min_combined_utilization: '500000000', is_active: true, limit: 20 }
      },
      {
        title: 'Available capacity',
        description: 'Active commitments with remaining capacity',
        params: { min_remaining_capacity: '1000000000000', is_active: true, order_by: 'remaining_capacity', sort_order: 'desc', limit: 20 }
      },
      {
        title: 'By total stake',
        description: 'Sort by combined stake (self stake + delegations)',
        params: { is_active: true, order_by: 'combined_stake', sort_order: 'desc', limit: 20 }
      },
    ],
    notes: [
      'Commitment data is extracted from chain storage and denormalized for fast queries',
      'The committer_address is the owner of the commitment NFT (Uniques collection 1)',
      'The manager_address is the owner of the manager NFT (Uniques collection 0)',
      'Numeric fields (amounts, weights) are stored as raw on-chain values without decimal shifting',
      'Utilization metrics (delegation_utilization, target_weight_per_compute_utilization, combined_utilization) are stored as Perbill: 1,000,000,000 = 100%',
    ],
  },
}

// Component to render documentation
export function DocContent({ methodKey }: { methodKey: MethodKey }) {
  const doc = rpcDocs[methodKey]

  if (!doc) {
    return (
      <div className="text-gray-500 text-sm">
        <p>No documentation available for this endpoint.</p>
      </div>
    )
  }

  return (
    <div className="space-y-4 text-sm">
      {/* Description */}
      <div className="text-gray-300 leading-relaxed">
        {doc.description}
      </div>

      {/* Parameters */}
      {doc.parameters && doc.parameters.length > 0 && (
        <div>
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Parameters</h4>
          <div className="space-y-1.5">
            {doc.parameters.map((param) => (
              <div key={param.name} className="flex gap-2 text-xs">
                <code className={`font-mono ${param.required ? 'text-amber-400' : 'text-blue-400'}`}>
                  {param.name}
                  {param.required && <span className="text-red-400">*</span>}
                </code>
                <span className="text-gray-500">({param.type})</span>
                <span className="text-gray-400 flex-1">{param.description}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Examples */}
      {doc.examples && doc.examples.length > 0 && (
        <div>
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Examples</h4>
          <div className="space-y-2">
            {doc.examples.map((example, i) => (
              <RpcExample
                key={i}
                methodKey={methodKey}
                title={example.title}
                description={example.description}
                params={example.params}
              />
            ))}
          </div>
        </div>
      )}

      {/* Notes */}
      {doc.notes && doc.notes.length > 0 && (
        <div>
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Notes</h4>
          <ul className="list-disc list-inside text-gray-400 text-xs space-y-1">
            {doc.notes.map((note, i) => (
              <li key={i}>{note}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}
