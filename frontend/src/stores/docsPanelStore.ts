import { create } from 'zustand'

interface DocsPanelState {
  isOpen: boolean
  setIsOpen: (open: boolean) => void
  toggle: () => void
}

export const useDocsPanelStore = create<DocsPanelState>((set) => ({
  isOpen: true,
  setIsOpen: (open) => set({ isOpen: open }),
  toggle: () => set((state) => ({ isOpen: !state.isOpen }))
}))
