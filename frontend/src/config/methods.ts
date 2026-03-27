import type { MethodConfig } from '@/lib/types'

export const methods: Record<string, MethodConfig> = {
  blocks: {
    name: 'getBlocks',
    category: 'blocks',
    fields: [
      { name: 'block_from', type: 'number', label: 'Block From', placeholder: 'e.g., 1000000' },
      { name: 'block_to', type: 'number', label: 'Block To', placeholder: 'e.g., 1000100' },
      { name: 'time_from', type: 'datetime', label: 'Time From' },
      { name: 'time_to', type: 'datetime', label: 'Time To' },
      { type: 'separator', name: '_sep_options', label: 'Options' },
      { name: 'sort_order', type: 'select', label: 'Sort Order (by block number)', options: ['', 'asc', 'desc'] },
      { name: 'limit', type: 'number', label: 'Limit', default: 10, placeholder: '1-1000' },
      { name: 'cursor', type: 'number', label: 'Cursor', placeholder: 'e.g., 9819' }
    ]
  },
  block: {
    name: 'getBlock',
    category: 'blocks',
    fields: [
      { name: 'hash', type: 'text', label: 'Block Hash', required: true, isParam: true, placeholder: '0x...' }
    ]
  },
  blocksCount: {
    name: 'getBlocksCount',
    category: 'blocks',
    fields: [
      { name: 'block_from', type: 'number', label: 'Block From' },
      { name: 'block_to', type: 'number', label: 'Block To' }
    ]
  },
  extrinsics: {
    name: 'getExtrinsics',
    category: 'extrinsics',
    fields: [
      { name: 'block_from', type: 'number', label: 'Block From' },
      { name: 'block_to', type: 'number', label: 'Block To' },
      { name: 'pallet', type: 'palletCombobox', label: 'Pallet', placeholder: 'Select or type...', metaType: 'extrinsics' },
      { name: 'method', type: 'methodCombobox', label: 'Method', placeholder: 'Select or type...', palletField: 'pallet', metaType: 'extrinsics' },
      { name: 'account_id', type: 'text', label: 'Account ID', placeholder: '0x... or SS58 address' },
      { name: 'data', type: 'json', label: 'Data (JSON)', placeholder: '{"field": "value"}' },
      { type: 'separator', name: '_sep_event', label: 'Event Filters' },
      { name: 'event.pallet', type: 'palletCombobox', label: 'Event Pallet', placeholder: 'Select or type...', metaType: 'events', nested: 'event' },
      { name: 'event.variant', type: 'methodCombobox', label: 'Event Variant', placeholder: 'Select or type...', palletField: 'event.pallet', metaType: 'events', nested: 'event' },
      { type: 'separator', name: '_sep_options', label: 'Options' },
      { name: 'events', type: 'checkbox', label: 'Include Events', default: false },
      { name: 'explode_batch', type: 'checkbox', label: 'Explode Batch Calls', default: false },
      { name: 'sort_order', type: 'select', label: 'Sort Order (by block number)', options: ['', 'asc', 'desc'] },
      { name: 'limit', type: 'number', label: 'Limit', default: 10 },
      { name: 'cursor', type: 'json', label: 'Cursor', placeholder: '{"block_number":...,"index":...}' }
    ]
  },
  extrinsic: {
    name: 'getExtrinsic',
    category: 'extrinsics',
    fields: [
      { name: 'block_number', type: 'number', label: 'Block Number', required: true, placeholder: 'e.g., 1234567' },
      { name: 'index', type: 'number', label: 'Extrinsic Index', required: true, placeholder: 'e.g., 2' },
      { name: 'events', type: 'checkbox', label: 'Include Events', default: false }
    ]
  },
  extrinsicByHash: {
    name: 'getExtrinsicByHash',
    category: 'extrinsics',
    fields: [
      { name: 'tx_hash', type: 'text', label: 'Transaction Hash', required: true, placeholder: '0x...' },
      { name: 'events', type: 'checkbox', label: 'Include Events', default: false }
    ]
  },
  extrinsicsCount: {
    name: 'getExtrinsicsCount',
    category: 'extrinsics',
    fields: [
      { name: 'block_from', type: 'number', label: 'Block From' },
      { name: 'block_to', type: 'number', label: 'Block To' },
      { name: 'pallet', type: 'palletCombobox', label: 'Pallet', placeholder: 'Select or type...', metaType: 'extrinsics' },
      { name: 'method', type: 'methodCombobox', label: 'Method', placeholder: 'Select or type...', palletField: 'pallet', metaType: 'extrinsics' },
      { name: 'account_id', type: 'text', label: 'Account ID' }
    ]
  },
  extrinsicAddresses: {
    name: 'getExtrinsicAddresses',
    category: 'extrinsics',
    fields: [
      { name: 'block_from', type: 'number', label: 'Block From' },
      { name: 'block_to', type: 'number', label: 'Block To' },
      { name: 'pallet', type: 'palletCombobox', label: 'Pallet', placeholder: 'Select or type...', metaType: 'extrinsics' },
      { name: 'method', type: 'methodCombobox', label: 'Method', placeholder: 'Select or type...', palletField: 'pallet', metaType: 'extrinsics' },
      { name: 'account_id', type: 'text', label: 'Account ID', placeholder: 'Filter by specific address' },
      { type: 'separator', name: '_sep_options', label: 'Options' },
      { name: 'sort_order', type: 'select', label: 'Sort Order (by block number)', options: ['', 'asc', 'desc'] },
      { name: 'limit', type: 'number', label: 'Limit', default: 10 },
      { name: 'cursor', type: 'json', label: 'Cursor', placeholder: '{"block_number":...,"index":...}' }
    ]
  },
  extrinsicMetadata: {
    name: 'getExtrinsicMetadata',
    category: 'extrinsics',
    fields: []
  },
  events: {
    name: 'getEvents',
    category: 'events',
    fields: [
      { name: 'block_from', type: 'number', label: 'Block From' },
      { name: 'block_to', type: 'number', label: 'Block To' },
      { name: 'pallet', type: 'palletCombobox', label: 'Pallet', placeholder: 'Select or type...', metaType: 'events' },
      { name: 'variant', type: 'methodCombobox', label: 'Event Variant', placeholder: 'Select or type...', palletField: 'pallet', metaType: 'events' },
      { name: 'account_id', type: 'text', label: 'Account ID', placeholder: '0x... or SS58 address' },
      { name: 'data', type: 'json', label: 'Data (JSON)', placeholder: '{"field": "value"}' },
      { type: 'separator', name: '_sep_job', label: 'Job Filter' },
      { name: 'job', type: 'text', label: 'Job (SS58, hex, or with #seq_id)', placeholder: '5GrwvaEF...#123 or 0xd43593...' },
      { type: 'separator', name: '_sep_options', label: 'Options' },
      { name: 'sort_order', type: 'select', label: 'Sort Order (by block number)', options: ['', 'asc', 'desc'] },
      { name: 'limit', type: 'number', label: 'Limit', default: 10 },
      { name: 'cursor', type: 'json', label: 'Cursor', placeholder: '{"block_number":...,"extrinsic_index":...,"index":...}' }
    ]
  },
  event: {
    name: 'getEvent',
    category: 'events',
    fields: [
      { name: 'block_number', type: 'number', label: 'Block Number', required: true, placeholder: 'e.g., 1234567' },
      { name: 'extrinsic_index', type: 'number', label: 'Extrinsic Index', required: true, placeholder: 'e.g., 2' },
      { name: 'index', type: 'number', label: 'Event Index', required: true, placeholder: 'e.g., 0' }
    ]
  },
  eventMetadata: {
    name: 'getEventMetadata',
    category: 'events',
    fields: []
  },
  specVersion: {
    name: 'getSpecVersion',
    category: 'blocks',
    fields: [
      { name: 'spec_version', type: 'number', label: 'Spec Version', placeholder: 'e.g., 1050000' },
      { name: 'block_number', type: 'number', label: 'Block Number', placeholder: 'Find spec version at or below this block' }
    ]
  },
  storageSnapshots: {
    name: 'getStorageSnapshots',
    category: 'storage',
    fields: [
      { name: 'block_from', type: 'number', label: 'Block From' },
      { name: 'block_to', type: 'number', label: 'Block To' },
      { name: 'time_from', type: 'datetime', label: 'Time From' },
      { name: 'time_to', type: 'datetime', label: 'Time To' },
      { name: 'pallet', type: 'storagePalletCombobox', label: 'Storage Pallet', placeholder: 'Select pallet...' },
      { name: 'storage_location', type: 'storageLocationCombobox', label: 'Storage Location', placeholder: 'Select location...', palletField: 'pallet' },
      { name: 'storage_keys', type: 'json', label: 'Storage Keys (JSON)', placeholder: '["key1", "key2"]' },
      { name: 'data', type: 'json', label: 'Data (JSON)', placeholder: '{"field": "value"}' },
      { name: 'config_rule', type: 'configRuleCombobox', label: 'Config Rule', placeholder: 'Select rule...' },
      { name: 'exclude_deleted', type: 'checkbox', label: 'Exclude Deleted', default: false },
      { type: 'separator', name: '_sep1', label: 'Extrinsic Filters' },
      { name: 'extrinsic.pallet', type: 'palletCombobox', label: 'Extrinsic Pallet', placeholder: 'Select or type...', metaType: 'extrinsics', nested: 'extrinsic' },
      { name: 'extrinsic.method', type: 'methodCombobox', label: 'Extrinsic Method', placeholder: 'Select or type...', palletField: 'extrinsic.pallet', metaType: 'extrinsics', nested: 'extrinsic' },
      { name: 'extrinsic.account_id', type: 'text', label: 'Account ID', placeholder: '0x... or SS58 address', nested: 'extrinsic' },
      { type: 'separator', name: '_sep2', label: 'Event Filters' },
      { name: 'event.pallet', type: 'palletCombobox', label: 'Event Pallet', placeholder: 'Select or type...', metaType: 'events', nested: 'event' },
      { name: 'event.variant', type: 'methodCombobox', label: 'Event Variant', placeholder: 'Select or type...', palletField: 'event.pallet', metaType: 'events', nested: 'event' },
      { type: 'separator', name: '_sep3', label: 'Sampling' },
      { name: 'include_epochs', type: 'checkbox', label: 'Include Epoch Info', default: false },
      { name: 'sample', type: 'select', label: 'Sample By', options: ['', 'per_epoch', 'day', 'week', 'month'] },
      { name: 'fill', type: 'checkbox', label: 'Fill Missing', default: false },
      { type: 'separator', name: '_sep_options', label: 'Options' },
      { name: 'sort_order', type: 'select', label: 'Sort Order (by block number)', options: ['', 'asc', 'desc'] },
      { name: 'limit', type: 'number', label: 'Limit', default: 10 },
      { name: 'cursor', type: 'number', label: 'Cursor', placeholder: 'e.g., 12345' }
    ]
  },
  jobs: {
    name: 'getJobs',
    category: 'jobs',
    fields: [
      { name: 'block_from', type: 'number', label: 'Block From' },
      { name: 'block_to', type: 'number', label: 'Block To' },
      { name: 'job', type: 'text', label: 'Job (SS58, hex, or with #seq_id)', placeholder: '5GrwvaEF...#123 or 0xd43593...' },
      { type: 'separator', name: '_sep_options', label: 'Options' },
      { name: 'sort_order', type: 'select', label: 'Sort Order (by block number)', options: ['', 'asc', 'desc'] },
      { name: 'limit', type: 'number', label: 'Limit', default: 10 },
      { name: 'cursor', type: 'json', label: 'Cursor', placeholder: '{"block_number":...,"index":...}' }
    ]
  },
  epochs: {
    name: 'getEpochs',
    category: 'epochs',
    fields: [
      { name: 'epoch_from', type: 'number', label: 'Epoch From', placeholder: 'e.g., 100' },
      { name: 'epoch_to', type: 'number', label: 'Epoch To', placeholder: 'e.g., 200' },
      { name: 'block_from', type: 'number', label: 'Block From (epoch start)', placeholder: 'e.g., 1000000' },
      { name: 'block_to', type: 'number', label: 'Block To (epoch start)', placeholder: 'e.g., 2000000' },
      { type: 'separator', name: '_sep_options', label: 'Options' },
      { name: 'sort_order', type: 'select', label: 'Sort Order (by epoch)', options: ['', 'asc', 'desc'] },
      { name: 'limit', type: 'number', label: 'Limit', default: 10 },
      { name: 'cursor', type: 'number', label: 'Cursor', placeholder: 'e.g., 9819' }
    ]
  },
  epochMetrics: {
    name: 'getMetricsByManager',
    category: 'managers',
    fields: [
      { name: 'manager', type: 'text', label: 'Manager Address', required: true, placeholder: '0x... or SS58 (required)' },
      { name: 'epoch_from', type: 'number', label: 'Epoch From', placeholder: 'e.g., 100' },
      { name: 'epoch_to', type: 'number', label: 'Epoch To', placeholder: 'e.g., 200' },
      { type: 'separator', name: '_sep_options', label: 'Options' },
      { name: 'limit', type: 'number', label: 'Limit', default: 16 },
      { name: 'cursor', type: 'number', label: 'Cursor', placeholder: 'e.g., 9819' }
    ]
  },
  processorMetrics: {
    name: 'getMetricsByProcessor',
    category: 'managers',
    fields: [
      { name: 'processor', type: 'text', label: 'Processor Address', required: true, placeholder: '0x... or SS58 (required)' },
      { name: 'epoch_from', type: 'number', label: 'Epoch From', placeholder: 'e.g., 100' },
      { name: 'epoch_to', type: 'number', label: 'Epoch To', placeholder: 'e.g., 200' },
      { type: 'separator', name: '_sep_options', label: 'Options' },
      { name: 'limit', type: 'number', label: 'Limit', default: 16 },
      { name: 'cursor', type: 'number', label: 'Cursor', placeholder: 'e.g., 9819' }
    ]
  },
  processorsCountByEpoch: {
    name: 'getProcessorsCountByEpoch',
    category: 'managers',
    fields: [
      { name: 'epoch_from', type: 'number', label: 'Epoch From', placeholder: 'e.g., 100' },
      { name: 'epoch_to', type: 'number', label: 'Epoch To', placeholder: 'e.g., 200' },
      { type: 'separator', name: '_sep_options', label: 'Options' },
      { name: 'sort_order', type: 'select', label: 'Sort Order (by epoch)', options: ['', 'asc', 'desc'] },
      { name: 'limit', type: 'number', label: 'Limit', default: 16 },
      { name: 'cursor', type: 'number', label: 'Cursor', placeholder: 'e.g., 9819' }
    ]
  },
  commitments: {
    name: 'getCommitments',
    category: 'staking',
    fields: [
      { name: 'commitment_id', type: 'number', label: 'Commitment ID', placeholder: 'e.g., 123' },
      { name: 'committer_address', type: 'text', label: 'Committer Address', placeholder: '0x... or SS58 address' },
      { name: 'manager_id', type: 'number', label: 'Manager ID', placeholder: 'e.g., 456' },
      { name: 'manager_address', type: 'text', label: 'Manager Address', placeholder: '0x... or SS58 address' },
      { name: 'is_active', type: 'booleanSelect', label: 'Active Status' },
      { name: 'in_cooldown', type: 'booleanSelect', label: 'In Cooldown' },
      { name: 'separator_filters', type: 'separator', label: 'Range Filters' },
      { name: 'min_stake_amount', type: 'text', label: 'Min Stake Amount', placeholder: 'e.g., 1000000000000' },
      { name: 'max_stake_amount', type: 'text', label: 'Max Stake Amount', placeholder: 'e.g., 10000000000000' },
      { name: 'min_delegations_total_amount', type: 'text', label: 'Min Delegations', placeholder: 'e.g., 0' },
      { name: 'max_delegations_total_amount', type: 'text', label: 'Max Delegations', placeholder: 'e.g., 1000000000000' },
      { name: 'min_commission', type: 'text', label: 'Min Commission', placeholder: 'e.g., 0' },
      { name: 'max_commission', type: 'text', label: 'Max Commission', placeholder: 'e.g., 100000000' },
      { name: 'min_delegation_utilization', type: 'text', label: 'Min Delegation Util', placeholder: 'e.g., 0.0' },
      { name: 'max_delegation_utilization', type: 'text', label: 'Max Delegation Util', placeholder: 'e.g., 1.0' },
      { name: 'min_target_weight_per_compute_utilization', type: 'text', label: 'Min Target Util', placeholder: 'e.g., 0.0' },
      { name: 'max_target_weight_per_compute_utilization', type: 'text', label: 'Max Target Util', placeholder: 'e.g., 1.0' },
      { name: 'min_combined_utilization', type: 'text', label: 'Min Combined Util', placeholder: 'e.g., 0.0' },
      { name: 'max_combined_utilization', type: 'text', label: 'Max Combined Util', placeholder: 'e.g., 1.0' },
      { name: 'min_max_delegation_capacity', type: 'text', label: 'Min Delegation Capacity', placeholder: 'e.g., 0' },
      { name: 'max_max_delegation_capacity', type: 'text', label: 'Max Delegation Capacity', placeholder: 'e.g., 1000000000000' },
      { name: 'min_min_max_weight_per_compute', type: 'text', label: 'Min Target Weight', placeholder: 'e.g., 0' },
      { name: 'max_min_max_weight_per_compute', type: 'text', label: 'Max Target Weight', placeholder: 'e.g., 1000000000000' },
      { name: 'min_remaining_capacity', type: 'text', label: 'Min Remaining Capacity', placeholder: 'e.g., 0' },
      { name: 'max_remaining_capacity', type: 'text', label: 'Max Remaining Capacity', placeholder: 'e.g., 1000000000000' },
      { name: 'min_cooldown_period', type: 'text', label: 'Min Cooldown Period', placeholder: 'e.g., 0' },
      { name: 'max_cooldown_period', type: 'text', label: 'Max Cooldown Period', placeholder: 'e.g., 1000' },
      { name: 'separator_options', type: 'separator', label: 'Ordering & Pagination' },
      { name: 'order_by', type: 'select', label: 'Order By', options: ['', 'stake_amount', 'stake_rewardable_amount', 'delegations_total_amount', 'commission', 'epoch', 'block_number', 'commitment_id', 'last_scoring_epoch', 'delegation_utilization', 'target_weight_per_compute_utilization', 'combined_utilization', 'max_delegation_capacity', 'min_max_weight_per_compute', 'remaining_capacity', 'cooldown_period', 'combined_stake', 'combined_weight'] },
      { name: 'sort_order', type: 'select', label: 'Sort Order', options: ['', 'asc', 'desc'] },
      { name: 'limit', type: 'number', label: 'Limit', default: 50 },
      { name: 'cursor', type: 'number', label: 'Cursor', placeholder: 'commitment_id' }
    ]
  }
}

export type MethodKey = keyof typeof methods
