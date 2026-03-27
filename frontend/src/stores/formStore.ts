import { create } from 'zustand'
import type { MethodKey } from '@/config/methods'
import type { RpcResponse, BatchRequest } from '@/lib/types'

interface FormState {
  // Current method
  currentMethod: MethodKey
  setCurrentMethod: (method: MethodKey) => void
  setMethodWithValues: (method: MethodKey, values: Record<string, unknown>) => void

  // Form values
  formValues: Record<string, unknown>
  setFormValue: (name: string, value: unknown) => void
  setFormValues: (values: Record<string, unknown>) => void
  resetFormValues: () => void

  // Pagination
  currentCursor: unknown
  cursorHistory: unknown[]
  currentPage: number
  setCursor: (cursor: unknown) => void
  pushCursor: (cursor: unknown) => void
  popCursor: () => unknown
  resetPagination: () => void

  // Response
  lastResponse: RpcResponse | null
  rawResponse: RpcResponse | null
  responseTime: number | null
  setResponse: (response: RpcResponse, time: number) => void
  clearResponse: () => void
}

export const useFormStore = create<FormState>((set, get) => ({
  // Current method
  currentMethod: 'storageSnapshots',
  setCurrentMethod: (method) => {
    set({
      currentMethod: method,
      formValues: {},
      currentCursor: null,
      cursorHistory: [],
      currentPage: 1
    })
  },
  setMethodWithValues: (method, values) => {
    set({
      currentMethod: method,
      formValues: values,
      currentCursor: null,
      cursorHistory: [],
      currentPage: 1
    })
  },

  // Form values
  formValues: {},
  setFormValue: (name, value) => set((state) => ({
    formValues: { ...state.formValues, [name]: value }
  })),
  setFormValues: (values) => set({ formValues: values }),
  resetFormValues: () => set({
    formValues: {},
    currentCursor: null,
    cursorHistory: [],
    currentPage: 1,
    lastResponse: null,
    rawResponse: null,
    responseTime: null
  }),

  // Pagination
  currentCursor: null as unknown,
  cursorHistory: [] as unknown[],
  currentPage: 1,
  setCursor: (cursor) => set({ currentCursor: cursor }),
  pushCursor: (cursor) => set((state) => ({
    cursorHistory: [...state.cursorHistory, state.currentCursor],
    currentCursor: cursor,
    currentPage: state.currentPage + 1
  })),
  popCursor: () => {
    const state = get()
    if (state.cursorHistory.length === 0) return undefined
    const newHistory = [...state.cursorHistory]
    const prevCursor = newHistory.pop()
    set({
      cursorHistory: newHistory,
      currentCursor: prevCursor ?? null,
      currentPage: state.currentPage - 1
    })
    return prevCursor
  },
  resetPagination: () => set({
    currentCursor: null,
    cursorHistory: [],
    currentPage: 1
  }),

  // Response
  lastResponse: null,
  rawResponse: null,
  responseTime: null,
  setResponse: (response, time) => set({
    lastResponse: response,
    rawResponse: response,
    responseTime: time
  }),
  clearResponse: () => set({
    lastResponse: null,
    rawResponse: null,
    responseTime: null
  })
}))

// Batch requests store (persisted to localStorage)
interface BatchState {
  requests: BatchRequest[]
  nextId: number
  addRequest: (request: Omit<BatchRequest, 'id'>) => void
  removeRequest: (id: number) => void
  clearRequests: () => void
  loadFromStorage: () => void
  saveToStorage: () => void
}

export const useBatchStore = create<BatchState>((set, get) => ({
  requests: [],
  nextId: 1,

  addRequest: (request) => set((state) => {
    const newRequest = { ...request, id: state.nextId }
    const newState = {
      requests: [...state.requests, newRequest],
      nextId: state.nextId + 1
    }
    // Save to localStorage
    localStorage.setItem('batchRequests', JSON.stringify(newState.requests))
    return newState
  }),

  removeRequest: (id) => set((state) => {
    const newRequests = state.requests.filter(r => r.id !== id)
    localStorage.setItem('batchRequests', JSON.stringify(newRequests))
    return { requests: newRequests }
  }),

  clearRequests: () => {
    localStorage.removeItem('batchRequests')
    set({ requests: [], nextId: 1 })
  },

  loadFromStorage: () => {
    const stored = localStorage.getItem('batchRequests')
    if (stored) {
      try {
        const requests = JSON.parse(stored) as BatchRequest[]
        const maxId = requests.reduce((max, r) => Math.max(max, r.id), 0)
        set({ requests, nextId: maxId + 1 })
      } catch {
        set({ requests: [], nextId: 1 })
      }
    }
  },

  saveToStorage: () => {
    const { requests } = get()
    localStorage.setItem('batchRequests', JSON.stringify(requests))
  }
}))
