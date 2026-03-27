import { create } from 'zustand'

interface UIState {
  // Batch sheet
  isBatchSheetOpen: boolean
  openBatchSheet: () => void
  closeBatchSheet: () => void
  toggleBatchSheet: () => void

  // Loading states
  isExecuting: boolean
  setExecuting: (executing: boolean) => void

  // API key visibility
  isApiKeyVisible: boolean
  toggleApiKeyVisibility: () => void
}

export const useUIStore = create<UIState>((set) => ({
  // Batch sheet
  isBatchSheetOpen: false,
  openBatchSheet: () => set({ isBatchSheetOpen: true }),
  closeBatchSheet: () => set({ isBatchSheetOpen: false }),
  toggleBatchSheet: () => set((state) => ({ isBatchSheetOpen: !state.isBatchSheetOpen })),

  // Loading states
  isExecuting: false,
  setExecuting: (executing) => set({ isExecuting: executing }),

  // API key visibility
  isApiKeyVisible: false,
  toggleApiKeyVisibility: () => set((state) => ({ isApiKeyVisible: !state.isApiKeyVisible }))
}))
