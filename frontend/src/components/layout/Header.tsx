import { Layers, Eye, EyeOff, ChevronDown, Menu } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { useFormStore, useBatchStore } from '@/stores/formStore'
import { useUIStore } from '@/stores/uiStore'
import { useApiKey } from '@/hooks/useApiKey'
import { methods, type MethodKey } from '@/config/methods'
import { CommandMenu, useCommandMenu } from './CommandMenu'

export function Header() {
  const { currentMethod, setCurrentMethod } = useFormStore()
  const { requests } = useBatchStore()
  const { openBatchSheet, isApiKeyVisible, toggleApiKeyVisibility } = useUIStore()
  const { apiKey, setApiKey } = useApiKey()
  const { open: commandOpen, setOpen: setCommandOpen } = useCommandMenu()

  const getCategoryMethods = (category: string) => {
    return Object.entries(methods)
      .filter(([, config]) => config.category === category)
      .map(([key, config]) => ({ key: key as MethodKey, name: config.name }))
  }

  return (
    <nav className="bg-gray-800 border-b border-gray-700 sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4">
        <div className="flex items-center justify-between h-14">
          <div className="flex items-center space-x-4">
            {/* Menu button - always visible */}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setCommandOpen(true)}
              className="p-2"
              title="Search RPC methods (Ctrl+K)"
            >
              <Menu className="h-5 w-5" />
            </Button>

            <a href="/" className="text-xs sm:text-xl font-bold text-blue-400 hover:text-blue-300">
              Acurast Indexer
            </a>

            {/* Navigation Menus - hidden on small screens */}
            <div className="hidden lg:flex space-x-1">
              {/* Blocks */}
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className={currentMethod.startsWith('block') ? 'bg-gray-700' : ''}
                  >
                    Blocks <ChevronDown className="ml-1 h-3 w-3" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent>
                  {getCategoryMethods('blocks').map(({ key, name }) => (
                    <DropdownMenuItem key={key} onClick={() => setCurrentMethod(key)}>
                      {name}
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>

              {/* Extrinsics */}
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className={methods[currentMethod].category === 'extrinsics' ? 'bg-gray-700' : ''}
                  >
                    Extrinsics <ChevronDown className="ml-1 h-3 w-3" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent>
                  {getCategoryMethods('extrinsics').map(({ key, name }) => (
                    <DropdownMenuItem key={key} onClick={() => setCurrentMethod(key)}>
                      {name}
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>

              {/* Events */}
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className={methods[currentMethod].category === 'events' ? 'bg-gray-700' : ''}
                  >
                    Events <ChevronDown className="ml-1 h-3 w-3" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent>
                  {getCategoryMethods('events').map(({ key, name }) => (
                    <DropdownMenuItem key={key} onClick={() => setCurrentMethod(key)}>
                      {name}
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>

              {/* Storage */}
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className={methods[currentMethod].category === 'storage' ? 'bg-gray-700' : ''}
                  >
                    Storage <ChevronDown className="ml-1 h-3 w-3" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent>
                  {getCategoryMethods('storage').map(({ key, name }) => (
                    <DropdownMenuItem key={key} onClick={() => setCurrentMethod(key)}>
                      {name}
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>

              {/* Jobs */}
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setCurrentMethod('jobs')}
                className={currentMethod === 'jobs' ? 'bg-gray-700' : ''}
              >
                Jobs
              </Button>

              {/* Epochs */}
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setCurrentMethod('epochs')}
                className={currentMethod === 'epochs' ? 'bg-gray-700' : ''}
              >
                Epochs
              </Button>

              {/* Managers */}
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className={methods[currentMethod].category === 'managers' ? 'bg-gray-700' : ''}
                  >
                    Managers & Metrics <ChevronDown className="ml-1 h-3 w-3" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent>
                  {getCategoryMethods('managers').map(({ key, name }) => (
                    <DropdownMenuItem key={key} onClick={() => setCurrentMethod(key)}>
                      {name}
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>

              {/* Staking */}
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className={methods[currentMethod].category === 'staking' ? 'bg-gray-700' : ''}
                  >
                    Staking <ChevronDown className="ml-1 h-3 w-3" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent>
                  {getCategoryMethods('staking').map(({ key, name }) => (
                    <DropdownMenuItem key={key} onClick={() => setCurrentMethod(key)}>
                      {name}
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </div>

          {/* Right side: Batch + API Key */}
          <div className="flex items-center space-x-4">
            {/* Batch Cart */}
            <Button
              variant="ghost"
              size="sm"
              onClick={openBatchSheet}
              className="relative"
            >
              <Layers className="h-5 w-5" />
              {requests.length > 0 && (
                <Badge
                  variant="secondary"
                  className="absolute -top-1 -right-1 h-5 w-5 p-0 flex items-center justify-center bg-purple-600 text-white text-xs"
                >
                  {requests.length}
                </Badge>
              )}
            </Button>

            {/* API Key */}
            <div className="flex items-center space-x-2">
              <label className="text-xs text-gray-400">API Key:</label>
              <Input
                type={isApiKeyVisible ? 'text' : 'password'}
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder="Enter API key"
                className="w-40 h-8 text-sm bg-gray-700 border-gray-600"
                autoComplete="off"
              />
              <Button
                variant="ghost"
                size="sm"
                onClick={toggleApiKeyVisibility}
                className="p-1"
              >
                {isApiKeyVisible ? (
                  <EyeOff className="h-4 w-4 text-gray-400" />
                ) : (
                  <Eye className="h-4 w-4 text-gray-400" />
                )}
              </Button>
            </div>
          </div>
        </div>
      </div>

      {/* Command Menu */}
      <CommandMenu open={commandOpen} onOpenChange={setCommandOpen} />
    </nav>
  )
}
