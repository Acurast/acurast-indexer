import { Play } from 'lucide-react'
import { toast } from 'sonner'
import { useFormStore } from '@/stores/formStore'
import { methods, type MethodKey } from '@/config/methods'

interface RpcExampleProps {
  methodKey: MethodKey
  title: string
  description?: string
  params: Record<string, unknown>
}

export function RpcExample({ methodKey, title, description, params }: RpcExampleProps) {
  const { setMethodWithValues } = useFormStore()

  const handleClick = () => {
    setMethodWithValues(methodKey, params)
    toast.success(`Loaded example: ${title}`)
  }

  const method = methods[methodKey]
  const jsonBody = {
    jsonrpc: '2.0',
    method: method.name,
    params,
    id: 1
  }

  return (
    <div
      onClick={handleClick}
      className="group cursor-pointer rounded border border-gray-700 hover:border-blue-500/50 hover:bg-blue-500/5 transition-colors"
    >
      <div className="flex items-center justify-between px-2 py-1.5 border-b border-gray-700/50">
        <div className="flex items-center gap-2">
          <Play className="h-3 w-3 text-blue-400 opacity-0 group-hover:opacity-100 transition-opacity" />
          <span className="text-xs font-medium text-gray-300">{title}</span>
        </div>
        <span className="text-[10px] text-gray-500 opacity-0 group-hover:opacity-100 transition-opacity">
          Click to load
        </span>
      </div>
      {description && (
        <div className="px-2 py-1 text-[10px] text-gray-500 border-b border-gray-700/50">
          {description}
        </div>
      )}
      <pre className="px-2 py-1.5 text-[10px] text-gray-400 font-mono overflow-x-auto">
        {JSON.stringify(jsonBody, null, 2)}
      </pre>
    </div>
  )
}
