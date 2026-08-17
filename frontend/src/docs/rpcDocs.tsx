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
      { name: 'data', type: 'json', description: 'Filter by data (JSON containment). Requires pallet and method to be set.' },
      { name: 'event.pallet', type: 'string|number', description: 'Only return extrinsics that emitted an event from this pallet' },
      { name: 'event.variant', type: 'string|number', description: 'Only return extrinsics that emitted this event variant (requires event.pallet if using name)' },
      { name: 'events', type: 'boolean', description: 'Include events for each extrinsic (default: false)' },
      { name: 'explode_batch', type: 'boolean', description: 'Expand utility.batch/batchAll into individual items with mapped events (default: false)' },
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
    notes: [
      'Response field data is null when the extrinsic carries no payload',
      'Response field events is null unless events=true',
      'When explode_batch=true, batch items include a batch_index field (0-based position within parent batch)',
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
      { name: 'pallet', type: 'string|number', description: 'Filter by pallet (single pair, backwards compatible)' },
      { name: 'method', type: 'string|number', description: 'Filter by method (single pair, backwards compatible)' },
      { name: 'account_id', type: 'string', description: 'Filter by signer account' },
      { name: 'pairs', type: 'array', description: 'Optional array of {pallet, method?} pairs (OR’d together, and combined with the single pallet/method if both are supplied). Each pair must specify a pallet; method is optional.' },
    ],
    examples: [
      {
        title: 'Count Acurast calls',
        params: { pallet: 'Acurast' }
      },
      {
        title: 'Count multiple pallet/method pairs',
        description: 'OR-list across a few pairs',
        params: { pairs: [{ pallet: 'Acurast', method: 'register' }, { pallet: 'Balances', method: 'transfer_keep_alive' }] }
      },
    ],
    notes: [
      'When no filter is provided, returns an approximate count from pg_class (instant).',
      'When pairs is provided, the planner uses BitmapOr over the (pallet, method, …) index. Keep the list short (a few pairs).',
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
        <p className="mt-2 text-gray-400">Events can be filtered by their emission source: extrinsics (user-initiated) or system (block initialization/finalization).</p>
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
      { name: 'source', type: 'string', description: 'Filter by event source: "extrinsic" (user-initiated) or "system" (block init/finalization)' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc"' },
      { name: 'limit', type: 'number', description: 'Maximum results' },
      { name: 'cursor', type: 'object', description: 'Cursor with block_number and index' },
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
      {
        title: 'System events only',
        description: 'Events from block initialization or finalization',
        params: { source: 'system', limit: 10 }
      },
    ],
    notes: [
      'source: "extrinsic" returns events from the ApplyExtrinsic phase (user-initiated transactions)',
      'source: "system" returns events from Initialization and Finalization phases (block processing)',
      'Without the source filter, all events are returned regardless of their emission phase',
      'Response field extrinsic_index is null for system events (Initialization/Finalization)',
      'Response fields data and error can be null',
    ],
  },

  event: {
    title: 'Get Event',
    description: (
      <p>Retrieves a single event by block number and event index. The event index is unique per block.</p>
    ),
    parameters: [
      { name: 'block_number', type: 'number', required: true, description: 'The block containing the event' },
      { name: 'index', type: 'number', required: true, description: 'The event index within the block' },
    ],
    examples: [
      {
        title: 'Get specific event',
        params: { block_number: 8917224, index: 4 }
      },
    ],
    notes: [
      'Response field extrinsic_index is null for system events (event_phase = Initialization or Finalization)',
      'Response fields data and error can be null',
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

  eventsCount: {
    title: 'Get Events Count',
    description: (
      <p>Returns the count of events matching the specified filters. Supports a single (pallet, variant) pair or multiple pairs OR’d together.</p>
    ),
    parameters: [
      { name: 'block_from', type: 'number', description: 'Minimum block number' },
      { name: 'block_to', type: 'number', description: 'Maximum block number' },
      { name: 'pallet', type: 'string|number', description: 'Filter by pallet (single pair, backwards compatible)' },
      { name: 'variant', type: 'string|number', description: 'Filter by event variant (single pair, backwards compatible)' },
      { name: 'source', type: 'string', description: '"extrinsic" or "system"' },
      { name: 'pairs', type: 'array', description: 'Optional array of {pallet, variant?} pairs (OR’d together, and combined with the single pallet/variant if both are supplied). Each pair must specify a pallet; variant is optional.' },
    ],
    examples: [
      {
        title: 'Count Balances.Transfer events',
        params: { pallet: 'Balances', variant: 'Transfer' }
      },
      {
        title: 'Count across multiple pairs',
        description: 'OR-list across a few pairs',
        params: { pairs: [{ pallet: 'Balances', variant: 'Transfer' }, { pallet: 'Acurast', variant: 'JobRegistrationStored' }] }
      },
    ],
    notes: [
      'When no filter is provided, returns an approximate count from pg_class (instant).',
      'Uses events_pallet_variant_idx; planner does BitmapOr across pairs. Keep the list short (a few pairs).',
    ],
  },

  specVersion: {
    title: 'Get Spec Version',
    description: (
      <>
        <p>Returns the runtime spec version and its SCALE-encoded metadata at a given point. Either lookup by exact <code>spec_version</code> or by <code>block_number</code> (returns the latest spec version active at or before that block).</p>
      </>
    ),
    parameters: [
      { name: 'spec_version', type: 'number', description: 'Exact runtime spec version' },
      { name: 'block_number', type: 'number', description: 'Find the spec version active at or below this block (alternative to spec_version)' },
    ],
    examples: [
      {
        title: 'By exact version',
        params: { spec_version: 1050000 }
      },
      {
        title: 'By block number',
        description: 'Returns the spec version active at this block',
        params: { block_number: 8915500 }
      },
    ],
    notes: [
      'At least one of spec_version or block_number must be provided',
      'Response shape: { spec_version, block_number, block_hash, metadata } — metadata is a 0x-prefixed hex string (SCALE-encoded runtime metadata v15)',
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
        <p className="mt-2 text-gray-400">Supports filtering by storage location, keys, data content, and the triggering extrinsic/event. Epoch-triggered snapshots have their own epoch_index field. Use sampling to keep only some snapshots by time period.</p>
      </>
    ),
    parameters: [
      { name: 'block_from', type: 'number', description: 'Minimum block number' },
      { name: 'block_to', type: 'number', description: 'Maximum block number' },
      { name: 'time_from', type: 'datetime', description: 'Filter by timestamp (ISO 8601)' },
      { name: 'time_to', type: 'datetime', description: 'Filter by timestamp (ISO 8601)' },
      { name: 'pallet', type: 'string|number', description: 'Storage pallet name or index' },
      { name: 'storage_location', type: 'string', description: 'Storage location name (e.g., "StoredJobRegistration")' },
      { name: 'storage_keys', type: 'json', description: 'Positional JSON array. Each element becomes storage_keys->>N equality (or storage_keys->N->>0 for a nested [value] element). Use null to skip a position. Examples: ["x"] → storage_keys->>0 = "x"; ["x","y"] → both positions must match; [null,"y"] → only position 1; [["x"]] → storage_keys->0->>0 = "x" (nested keys, e.g. Commitments).' },
      { name: 'data', type: 'json', description: 'JSON object to match in data (containment)' },
      { name: 'config_rule', type: 'string', description: 'Filter by indexer config rule name' },
      { name: 'exclude_deleted', type: 'boolean', description: 'Exclude entries that were later deleted' },
      { name: 'epoch_index', type: 'number', description: 'Filter by epoch number (for epoch-triggered snapshots)' },
      { name: 'epoch_end', type: 'boolean', description: 'Filter by the epoch_end flag. true=only end-of-epoch snapshots, false=only non-end snapshots (start or mid-epoch), omitted=no filter (returns all)' },
      { name: 'extrinsic.pallet', type: 'string', description: 'Filter by triggering extrinsic pallet' },
      { name: 'extrinsic.method', type: 'string', description: 'Filter by triggering extrinsic method' },
      { name: 'extrinsic.account_id', type: 'string', description: 'Filter by triggering account' },
      { name: 'event.pallet', type: 'string', description: 'Filter by triggering event pallet' },
      { name: 'event.variant', type: 'string', description: 'Filter by triggering event variant' },
      { name: 'include_epochs', type: 'boolean', description: 'Include nested epoch info (epoch, epoch_start, epoch_end, epoch_start_time) in response' },
      { name: 'sample', type: 'string', description: 'Sample by time period: per_epoch, day (~8 epochs), week (~56 epochs), month (~240 epochs)' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc"' },
      { name: 'limit', type: 'number', description: 'Maximum results' },
      { name: 'cursor', type: 'object|number', description: 'Pagination cursor. For non-sampling queries: {"block_number": <i64>, "id": <i64>} taken from the last item of the previous page (single id is insufficient because rows are ordered by (block_number, id) and ids are not strictly co-monotonic with block_number). For `sample` queries: a single number — the previous page\'s epoch_bucket.' },
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
        title: 'Epoch start snapshots',
        description: 'Snapshots taken at the start of epochs',
        params: { epoch_end: false, limit: 10 }
      },
      {
        title: 'Daily samples',
        description: 'One snapshot per day (gaps left empty)',
        params: { sample: 'day', limit: 30 }
      },
    ],
    notes: [
      'The data filter uses PostgreSQL JSONB containment (@>)',
      'storage_keys is a positional array. Each non-null element must match at its position (storage_keys->>N for primitives, storage_keys->N->>0 for [single] nested arrays). This always targets the expression indexes on storage_keys when pallet+storage_location are specified — no GIN containment fallback.',
      'Invalid shapes (non-array input, non-primitive elements, nested arrays with more than one element) return an invalid_params error instead of silently matching broadly.',
      'Deleted entries have data set to JSON null',
      'Epoch-triggered snapshots have epoch_index set (the epoch number) and extrinsic_index is null',
      'Response field epoch_end (boolean) indicates whether the snapshot was captured at the last block of an epoch (true) or earlier (false)',
      'epoch_end filter: true=only end-of-epoch snapshots, false=only non-end snapshots, omitted=no filter',
      'When using sample, response format changes to a dictionary keyed by epoch number',
      'Epoch durations: day ~8, week ~56, month ~240 epochs (~3 hours per epoch)',
      'Nullable response fields: extrinsic_index (null for epoch-triggered snapshots), event_index (null unless the snapshot was triggered by a specific event), epoch (present only when include_epochs or sample is set)',
      'Pagination cursor for non-sampling queries is now {"block_number": ..., "id": ...} (compound). The previous single-id cursor was unsound: rows are ordered by (block_number, id) and a bare id predicate skipped intermediate rows whose ids fell below the cursor.',
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
  // DEPLOYMENTS
  // ============================================
  deployments: {
    title: 'Get Deployments',
    description: (
      <>
        <p>Retrieves deployment details for Acurast jobs. Deployments contain parsed JobRegistration data including schedule and requirements.</p>
        <p className="mt-2 text-gray-400">Populated from JobRegistrationStoredV2 events. Use chain queries for live processor data.</p>
      </>
    ),
    parameters: [
      { name: 'account_id', type: 'string', description: 'Filter by deployer address (hex or SS58)' },
      { name: 'seq_id', type: 'number', description: 'Filter by sequence ID' },
      { name: 'is_active', type: 'boolean', description: 'Filter by active status' },
      { name: 'exclude_addresses', type: 'array', description: 'Exclude deployments deployed by any of these addresses. Accepts hex and SS58 addresses, and the two formats may be mixed in the same list.' },
      { name: 'block_from', type: 'number', description: 'Minimum block number' },
      { name: 'block_to', type: 'number', description: 'Maximum block number' },
      { name: 'related_extrinsics', type: 'boolean', description: 'Include related extrinsics per deployment (default: false). Expensive for list queries — enable only when needed.' },
      { name: 'order_by', type: 'string', description: 'Sort column: block_number, created_block_number, start_time' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc"' },
      { name: 'limit', type: 'number', description: 'Maximum results (default: 50)' },
      { name: 'cursor', type: 'object', description: 'Pagination cursor: {"seq_id": ..., "val": ...}' },
    ],
    examples: [
      {
        title: 'Latest deployments',
        params: { limit: 20 }
      },
      {
        title: 'Active deployments only',
        params: { is_active: true, limit: 20 }
      },
      {
        title: 'By deployer address',
        params: { account_id: '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY', limit: 20 }
      },
      {
        title: 'With related extrinsics',
        description: 'Include extrinsic history for each deployment',
        params: { related_extrinsics: true, limit: 20 }
      },
      {
        title: 'Excluding addresses',
        description: 'Filter out deployments from a blacklist of addresses (hex and SS58 may be mixed)',
        params: { exclude_addresses: ['0x0102030405060708091011121314151617181920212223242526272829303132', '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'], limit: 20 }
      },
    ],
    notes: [
      'related_extrinsics (response field) aggregates all extrinsics related to this deployment from the jobs table. Present only when the related_extrinsics=true parameter is set.',
      'Nullable response fields: snapshot_id, allowed_sources, planned_executions (populated for "Single" assignment_strategy only), min_reputation, processor_version, related_extrinsics (null unless requested)',
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
    notes: [
      'Response field epoch_end is null for the most recent epoch (computed via LEAD over epoch_start)',
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
  // REWARDS
  // ============================================
  baseRewards: {
    title: 'Get Base Rewards',
    description: (
      <>
        <p>
          Returns the base rewards earned by each processor belonging to a manager, aggregated per epoch.
          Each row represents the total <code>Balances::Deposit</code> amount triggered by{' '}
          <code>heartbeat_with_metrics</code> extrinsics for a given processor within an epoch.
        </p>
        <p className="mt-2">
          A processor typically sends 2–3 heartbeats per epoch; amounts are summed so each row
          reflects the true total base reward for that processor in that epoch.
        </p>
      </>
    ),
    parameters: [
      { name: 'manager', type: 'string', required: true, description: 'Manager address (0x hex or SS58)' },
      { name: 'processor', type: 'string', description: 'Optional: filter to a single processor address (0x hex or SS58)' },
      { name: 'epoch_from', type: 'number', description: 'Filter to epochs >= this value' },
      { name: 'epoch_to', type: 'number', description: 'Filter to epochs <= this value' },
      { name: 'limit', type: 'number', description: 'Maximum results to return (default: 50)' },
      { name: 'cursor_epoch', type: 'number', description: 'Pagination cursor — epoch value from previous page cursor' },
      { name: 'cursor_processor', type: 'string', description: 'Pagination cursor — processor value from previous page cursor' },
    ],
    examples: [
      {
        title: 'All processors for a manager (recent epochs)',
        params: { manager: '0x4c069e20ce75ac39ff13124faa3fef366e43cdecf677a85f8b7a376803d83ef5', limit: 10 }
      },
      {
        title: 'Single processor history',
        params: {
          manager: '0x4c069e20ce75ac39ff13124faa3fef366e43cdecf677a85f8b7a376803d83ef5',
          processor: '0x005e2053f1c2146dc8e8b8aba30df73615db7be9ec98a19700cfa58fc1f30b18',
          limit: 10
        }
      },
    ],
    notes: [
      'amount is returned as a string to preserve u128 precision (value in planck, 1 ACU = 10^12 planck)',
      'Results are sorted by epoch DESC, then processor ASC within the same epoch',
      'Use cursor_epoch + cursor_processor from the response cursor object to paginate',
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
      { name: 'min_target_weight_per_compute_utilization', type: 'string', description: 'Minimum target-weight-per-compute utilization (Perbill, can exceed 1_000_000_000)' },
      { name: 'max_target_weight_per_compute_utilization', type: 'string', description: 'Maximum target-weight-per-compute utilization (Perbill)' },
      { name: 'min_combined_utilization', type: 'string', description: 'Minimum combined utilization (Perbill)' },
      { name: 'max_combined_utilization', type: 'string', description: 'Maximum combined utilization (Perbill)' },
      { name: 'min_max_delegation_capacity', type: 'string', description: 'Minimum max_delegation_capacity (self_slash_weight * 9)' },
      { name: 'max_max_delegation_capacity', type: 'string', description: 'Maximum max_delegation_capacity' },
      { name: 'min_min_max_weight_per_compute', type: 'string', description: 'Minimum min_max_weight_per_compute' },
      { name: 'max_min_max_weight_per_compute', type: 'string', description: 'Maximum min_max_weight_per_compute' },
      { name: 'min_remaining_capacity', type: 'string', description: 'Minimum remaining capacity' },
      { name: 'max_remaining_capacity', type: 'string', description: 'Maximum remaining capacity' },
      { name: 'min_cooldown_period', type: 'string', description: 'Minimum cooldown period' },
      { name: 'max_cooldown_period', type: 'string', description: 'Maximum cooldown period' },
      { name: 'order_by', type: 'string', description: 'Column to sort by: commitment_id, stake_amount, stake_rewardable_amount, delegations_total_amount, commission, epoch, block_number, last_scoring_epoch, cooldown_period, delegation_utilization, target_weight_per_compute_utilization, combined_utilization, max_delegation_capacity, min_max_weight_per_compute, remaining_capacity, combined_stake (stake + delegations), combined_weight (delegations_slash_weight + self_slash_weight). Default: stake_amount.' },
      { name: 'sort_order', type: 'string', description: '"asc" or "desc" (default: desc)' },
      { name: 'limit', type: 'number', description: 'Maximum results (default: 50)' },
      { name: 'cursor', type: 'number|object', description: 'When order_by=commitment_id: a bare commitment_id. Otherwise a compound cursor {"id": commitment_id, "val": sort_value} — val must match the order_by column type (string for numeric columns, number for epoch/block_number/last_scoring_epoch/cooldown_period).' },
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
      'Nullable response fields: manager_id, manager_address, snapshot_id, cooldown_started, max_delegation_capacity, min_max_weight_per_compute, delegation_utilization, target_weight_per_compute_utilization, combined_utilization, remaining_capacity, committed_metrics, metrics_epoch_sum',
    ],
  },

  accounts: {
    title: 'Get Accounts',
    description: (
      <>
        <p>Paged, filterable listing of the materialized <code>accounts</code> table, ranked by a balance dimension. With no filters this is the top-N by balance; it also supports role-flag and attestation-classification filters plus keyset pagination via <code>cursor</code>.</p>
      </>
    ),
    parameters: [
      { name: 'sort', type: 'string', description: '"total" (free + reserved), "total_with_locked" (free + reserved + remaining_vesting + remaining_token_claim, default), "transferable" (spendable balance), "free", "reserved", or "frozen" (that balance component only)' },
      { name: 'is_processor', type: 'boolean', description: 'Filter to (or exclude) accounts flagged as processors' },
      { name: 'is_manager', type: 'boolean', description: 'Filter to (or exclude) accounts flagged as managers' },
      { name: 'is_committer', type: 'boolean', description: 'Filter to (or exclude) accounts flagged as committers' },
      { name: 'processor_type', type: 'string', description: 'Exact match: "Core", "Lite", or "Unknown"' },
      { name: 'device_type', type: 'string', description: 'Exact match: "iOS", "Android", or "Unknown"' },
      { name: 'account_id', type: 'string', description: 'Exact match on account_id (hex or SS58)' },
      { name: 'exclude_addresses', type: 'array', description: 'Exclude any of these accounts. Accepts hex and SS58 addresses, and the two formats may be mixed in the same list.' },
      { name: 'cursor', type: 'object', description: 'Keyset cursor from the previous page\'s response: {"sort_value": "<numeric string>", "account_id": "<string>"}' },
      { name: 'limit', type: 'number', description: 'Number of accounts to return (1-100, default: 100)' },
    ],
    examples: [
      {
        title: 'All processors, ranked by whole balance',
        params: { is_processor: true }
      },
      {
        title: 'Lite Android processors',
        params: { is_processor: true, processor_type: 'Lite', device_type: 'Android' }
      },
      {
        title: 'Exclude a blacklist of addresses',
        description: 'Filter out specific accounts (hex and SS58 may be mixed)',
        params: { exclude_addresses: ['0x0102030405060708091011121314151617181920212223242526272829303132', '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'], limit: 20 }
      },
      {
        title: 'Next page',
        description: 'Pass the cursor object from the previous response',
        params: { is_processor: true, cursor: { sort_value: '1000000000000', account_id: '0x...' } }
      },
    ],
    notes: [
      'Balances are returned as strings to preserve full NUMERIC(38,0) precision',
      'Filters are ANDed together',
      'processor_type/device_type are null until an attestation has been classified; "Unknown" means an attestation was seen but not recognized',
      'Response cursor is null on the last page',
      'With no filters, ordering walks a per-dimension DESC index (fast top-N); under heavy filtering it falls back to filter-then-sort — fine for this table\'s size',
    ],
  },

  epochTotals: {
    title: 'Get Epoch Totals',
    description: (
      <>
        <p>Per-epoch network-wide totals time series: total remaining vesting (pallet_vesting), total remaining token-claim (AcurastTokenClaim), total committer self-stake, and total delegated (AcurastCompute). One row per epoch, evaluated at the epoch's end block. Ordered by epoch descending (most recent first).</p>
      </>
    ),
    parameters: [
      { name: 'epoch_from', type: 'number', description: 'Minimum epoch (inclusive)' },
      { name: 'epoch_to', type: 'number', description: 'Maximum epoch (inclusive)' },
      { name: 'limit', type: 'number', description: 'Max rows, most recent first (1-5000, default: 1000)' },
    ],
    examples: [
      {
        title: 'Latest 1000 epochs',
        params: {}
      },
      {
        title: 'Epoch range',
        description: 'Totals for a specific epoch range',
        params: { epoch_from: 3600, epoch_to: 3700 }
      },
    ],
    notes: [
      'Amounts are returned as strings (raw on-chain units) to preserve full NUMERIC(38,0) precision',
      'total_vesting decays with block height and is recomputed at each epoch end (not carried forward)',
      'Staking totals count commitments in cooldown/stale; a commitment drops out only once removed from storage',
      'total_delegated is the runtime-aggregated delegations_total_amount summed across live commitments',
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
