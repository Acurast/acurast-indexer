import type { ReactNode } from 'react'
import { Header } from './Header'
import { TaskMonitor } from './TaskMonitor'
import { DocsPanel } from '@/components/docs/DocsPanel'
import { useDocsPanelStore } from '@/stores/docsPanelStore'
import { Toaster } from 'sonner'

interface MainLayoutProps {
  children: ReactNode
}

export function MainLayout({ children }: MainLayoutProps) {
  const { isOpen } = useDocsPanelStore()

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 pb-12">
      <Header />
      <DocsPanel />
      <main className={`transition-all duration-300 px-4 py-6 ${isOpen ? 'ml-80' : 'ml-12'}`}>
        <div className="max-w-7xl mx-auto">
          {children}
        </div>
      </main>
      <TaskMonitor />
      <Toaster
        theme="dark"
        position="bottom-right"
        toastOptions={{
          style: {
            background: '#1f2937',
            border: '1px solid #374151',
            color: '#e5e7eb'
          }
        }}
      />
    </div>
  )
}
