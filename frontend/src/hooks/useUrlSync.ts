import { useEffect, useRef } from 'react'
import { useFormStore } from '@/stores/formStore'
import { methods, type MethodKey } from '@/config/methods'

// Encode params to URL-safe base64
function encodeParams(params: Record<string, unknown>): string {
  // Filter out empty values
  const filtered = Object.fromEntries(
    Object.entries(params).filter(([, v]) => v !== '' && v !== null && v !== undefined)
  )
  if (Object.keys(filtered).length === 0) return ''
  try {
    const json = JSON.stringify(filtered)
    // Use URL-safe base64 encoding
    return btoa(json).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
  } catch {
    return ''
  }
}

// Decode params from URL-safe base64
function decodeParams(encoded: string): Record<string, unknown> {
  if (!encoded) return {}
  try {
    // Restore standard base64 from URL-safe version
    let base64 = encoded.replace(/-/g, '+').replace(/_/g, '/')
    // Add padding if needed
    while (base64.length % 4) {
      base64 += '='
    }
    return JSON.parse(atob(base64))
  } catch {
    return {}
  }
}

// Get method key from URL path
function getMethodFromPath(path: string): MethodKey | null {
  const cleanPath = path.replace(/^\//, '').toLowerCase()
  if (!cleanPath) return null

  // Direct match (case insensitive)
  for (const key of Object.keys(methods)) {
    if (key.toLowerCase() === cleanPath) {
      return key as MethodKey
    }
  }

  // Try to find by RPC method name (e.g., getBlocks -> blocks)
  for (const [key, config] of Object.entries(methods)) {
    if (config.name.toLowerCase() === cleanPath) {
      return key as MethodKey
    }
  }

  return null
}

// Get URL path from method key
function getPathFromMethod(method: MethodKey): string {
  return `/${method}`
}

export function useUrlSync() {
  const { currentMethod, formValues, setCurrentMethod, setMethodWithValues } = useFormStore()
  const isInitialized = useRef(false)

  // Initialize from URL on first load
  useEffect(() => {
    if (isInitialized.current) return
    isInitialized.current = true

    const path = window.location.pathname
    const searchParams = new URLSearchParams(window.location.search)
    const encodedParams = searchParams.get('p')

    // Get method from path
    const method = getMethodFromPath(path)
    if (method) {
      // Decode params and set method + values atomically
      if (encodedParams) {
        const params = decodeParams(encodedParams)
        setMethodWithValues(method, params)
      } else {
        setCurrentMethod(method)
      }
    }
  }, [setCurrentMethod, setMethodWithValues])

  // Update URL when method or form values change
  useEffect(() => {
    if (!isInitialized.current) return

    const newPath = getPathFromMethod(currentMethod)
    const encodedParams = encodeParams(formValues)
    const newUrl = encodedParams ? `${newPath}?p=${encodedParams}` : newPath

    // Only update if different from current URL
    const currentUrl = window.location.pathname + window.location.search
    if (newUrl !== currentUrl) {
      window.history.replaceState({}, '', newUrl)
    }
  }, [currentMethod, formValues])

  // Handle browser back/forward navigation
  useEffect(() => {
    const handlePopState = () => {
      const path = window.location.pathname
      const searchParams = new URLSearchParams(window.location.search)
      const encodedParams = searchParams.get('p')

      const method = getMethodFromPath(path)
      if (method) {
        // Decode params and set method + values atomically
        if (encodedParams) {
          const params = decodeParams(encodedParams)
          setMethodWithValues(method, params)
        } else {
          setCurrentMethod(method)
        }
      }
    }

    window.addEventListener('popstate', handlePopState)
    return () => window.removeEventListener('popstate', handlePopState)
  }, [setCurrentMethod, setMethodWithValues])
}
