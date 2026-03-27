import { useMutation } from '@tanstack/react-query'
import type { RpcRequest, RpcResponse } from '@/lib/types'

interface ExecuteOptions {
  apiKey: string
  request: RpcRequest
}

export async function executeRpcRequest({ apiKey, request }: ExecuteOptions): Promise<{ response: RpcResponse; time: number }> {
  const startTime = performance.now()

  const res = await fetch('/api/v1/rpc', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'API-Key': apiKey
    },
    body: JSON.stringify(request)
  })

  const endTime = performance.now()

  // Handle non-OK responses (401, 500, etc.)
  if (!res.ok) {
    const errorMessage = res.status === 401
      ? 'Unauthorized: Invalid API key'
      : `Server error: ${res.status} ${res.statusText}`
    throw new Error(errorMessage)
  }

  const data = await res.json()

  return {
    response: data as RpcResponse,
    time: Math.round(endTime - startTime)
  }
}

export function useRpcRequest() {
  return useMutation({
    mutationFn: executeRpcRequest
  })
}

// Batch execution
interface BatchExecuteOptions {
  apiKey: string
  requests: RpcRequest[]
}

async function executeBatchRequest({ apiKey, requests }: BatchExecuteOptions): Promise<{ responses: RpcResponse[]; time: number }> {
  const startTime = performance.now()

  const res = await fetch('/api/v1/rpc', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'API-Key': apiKey
    },
    body: JSON.stringify(requests)
  })

  const endTime = performance.now()

  // Handle non-OK responses (401, 500, etc.)
  if (!res.ok) {
    const errorMessage = res.status === 401
      ? 'Unauthorized: Invalid API key'
      : `Server error: ${res.status} ${res.statusText}`
    throw new Error(errorMessage)
  }

  const data = await res.json()

  return {
    responses: Array.isArray(data) ? data : [data],
    time: Math.round(endTime - startTime)
  }
}

export function useBatchRpcRequest() {
  return useMutation({
    mutationFn: executeBatchRequest
  })
}
