import { ChevronLeft, BookOpen } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useFormStore } from '@/stores/formStore'
import { useDocsPanelStore } from '@/stores/docsPanelStore'
import { methods } from '@/config/methods'
import { DocContent, rpcDocs } from '@/docs/rpcDocs'

export function DocsPanel() {
  const { isOpen, setIsOpen } = useDocsPanelStore()
  const { currentMethod } = useFormStore()

  const method = methods[currentMethod]
  const hasDoc = rpcDocs[currentMethod] !== undefined

  if (!isOpen) {
    // Collapsed state - narrow sidebar with icon and vertical text
    return (
      <div className="fixed left-0 top-14 bottom-0 w-12 z-40 bg-gray-800 border-r border-gray-700">
        <div
          onClick={() => setIsOpen(true)}
          className="h-full flex flex-col items-center py-3 cursor-pointer hover:bg-gray-700 transition-colors"
          title="Show documentation"
        >
          <BookOpen className="h-5 w-5 text-blue-400 mb-2" />
          <span className="text-xs text-gray-400 writing-mode-vertical">Docs</span>
        </div>
      </div>
    )
  }

  // Expanded state - full documentation panel
  return (
    <div className="fixed left-0 top-14 bottom-0 w-80 z-40 bg-gray-800 border-r border-gray-700 flex flex-col">
      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-2 border-b border-gray-700 bg-gray-800 shrink-0">
        <BookOpen className="h-4 w-4 text-blue-400 shrink-0" />
        <div className="flex-1 min-w-0">
          <h3 className="text-sm font-medium text-gray-200 truncate">
            {method.name}
          </h3>
        </div>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setIsOpen(false)}
          className="h-6 w-6 p-0 hover:bg-gray-700"
          title="Hide documentation"
        >
          <ChevronLeft className="h-4 w-4" />
        </Button>
      </div>

      {/* Content */}
      <ScrollArea className="flex-1">
        <div className="p-3">
          {hasDoc ? (
            <DocContent methodKey={currentMethod} />
          ) : (
            <div className="text-gray-500 text-sm">
              <p>No documentation available for this endpoint yet.</p>
              <p className="mt-2 text-xs">
                Use the form on the right to configure parameters and execute the request.
              </p>
            </div>
          )}
        </div>
      </ScrollArea>

      {/* Footer */}
      <div className="px-3 py-2 border-t border-gray-700 bg-gray-800/50 shrink-0">
        <p className="text-[10px] text-gray-500">
          Click examples above to load them into the form
        </p>
      </div>
    </div>
  )
}
