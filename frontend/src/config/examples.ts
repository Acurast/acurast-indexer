import type { Example } from '@/lib/types'

export const examples: Record<string, Example> = {
  registerJob: {
    method: 'extrinsics',
    values: { pallet: 'Acurast', method: 'register', limit: 10 },
    description: 'Job registration extrinsics'
  },
  fulfillJob: {
    method: 'extrinsics',
    values: { pallet: 'Acurast', method: 'fulfill', limit: 10 },
    description: 'Job fulfillment extrinsics'
  },
  heartbeatExtrinsic: {
    method: 'extrinsics',
    values: { pallet: 'AcurastProcessorManager', method: 'heartbeat_with_version', limit: 10 },
    description: 'Processor heartbeat extrinsics'
  },
  jobRegistrationStored: {
    method: 'events',
    values: { pallet: 'Acurast', variant: 'JobRegistrationStoredV2', limit: 10 },
    description: 'Job registration stored events'
  },
  heartbeat: {
    method: 'events',
    values: { pallet: 'AcurastProcessorManager', variant: 'ProcessorHeartbeatWithVersion', limit: 10 },
    description: 'Processor heartbeat events'
  },
  jobFulfilled: {
    method: 'events',
    values: { pallet: 'Acurast', variant: 'JobExecutionResultV1', limit: 10 },
    description: 'Job execution result events'
  },
  balanceTransfer: {
    method: 'events',
    values: { pallet: 'Balances', variant: 'Transfer', limit: 10 },
    description: 'Balance transfer events'
  },
  storageJobRegistration: {
    method: 'storageSnapshots',
    values: { config_rule: 'job_registration_stored', limit: 10 },
    description: 'Job registration storage snapshots'
  },
  storageMetrics: {
    method: 'storageSnapshots',
    values: { config_rule: 'processor_heartbeat_metrics', limit: 10 },
    description: 'Processor metrics snapshots'
  },
  storageCommitments: {
    method: 'storageSnapshots',
    values: { config_rule: 'commitment_created', limit: 10 },
    description: 'Commitment storage snapshots'
  },
  storageDelegations: {
    method: 'storageSnapshots',
    values: { config_rule: 'delegation_created', limit: 10 },
    description: 'Delegation storage snapshots'
  }
}

export type ExampleKey = keyof typeof examples
