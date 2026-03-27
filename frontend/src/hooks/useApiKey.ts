import { create } from 'zustand'

const API_KEY_STORAGE_KEY = 'acurast_indexer_api_key'

interface ApiKeyState {
  apiKey: string
  setApiKey: (key: string) => void
  loadFromStorage: () => void
}

export const useApiKey = create<ApiKeyState>((set) => ({
  apiKey: '',

  setApiKey: (key: string) => {
    set({ apiKey: key })
    if (key) {
      localStorage.setItem(API_KEY_STORAGE_KEY, key)
    } else {
      localStorage.removeItem(API_KEY_STORAGE_KEY)
    }
  },

  loadFromStorage: () => {
    const stored = localStorage.getItem(API_KEY_STORAGE_KEY)
    if (stored) {
      set({ apiKey: stored })
    }
  }
}))
