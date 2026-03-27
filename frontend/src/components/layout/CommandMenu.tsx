import { useEffect, useState } from 'react'
import { Search } from 'lucide-react'
import {
  CommandDialog,
  CommandInput,
  CommandList,
  CommandEmpty,
  CommandGroup,
  CommandItem,
} from '@/components/ui/command'
import { useFormStore } from '@/stores/formStore'
import { methods, type MethodKey } from '@/config/methods'

interface CommandMenuProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

const categoryLabels: Record<string, string> = {
  blocks: 'Blocks',
  extrinsics: 'Extrinsics',
  events: 'Events',
  storage: 'Storage',
  jobs: 'Jobs',
  epochs: 'Epochs',
  managers: 'Managers & Metrics',
  staking: 'Staking',
}

export function CommandMenu({ open, onOpenChange }: CommandMenuProps) {
  const { setCurrentMethod } = useFormStore()

  const handleSelect = (methodKey: string) => {
    setCurrentMethod(methodKey as MethodKey)
    onOpenChange(false)
  }

  // Group methods by category
  const methodsByCategory = Object.entries(methods).reduce((acc, [key, config]) => {
    const category = config.category
    if (!acc[category]) {
      acc[category] = []
    }
    acc[category].push({ key, name: config.name })
    return acc
  }, {} as Record<string, { key: string; name: string }[]>)

  return (
    <CommandDialog open={open} onOpenChange={onOpenChange}>
      <CommandInput placeholder="Search RPC methods..." />
      <CommandList>
        <CommandEmpty>No methods found.</CommandEmpty>
        {Object.entries(methodsByCategory).map(([category, categoryMethods]) => (
          <CommandGroup key={category} heading={categoryLabels[category] || category}>
            {categoryMethods.map(({ key, name }) => (
              <CommandItem
                key={key}
                value={`${name} ${key}`}
                onSelect={() => handleSelect(key)}
                className="cursor-pointer"
              >
                <Search className="mr-2 h-4 w-4 text-gray-500" />
                <span>{name}</span>
                <span className="ml-auto text-xs text-gray-500">{key}</span>
              </CommandItem>
            ))}
          </CommandGroup>
        ))}
      </CommandList>
    </CommandDialog>
  )
}

export function useCommandMenu() {
  const [open, setOpen] = useState(false)

  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === 'k' && (e.metaKey || e.ctrlKey)) {
        e.preventDefault()
        setOpen((open) => !open)
      }
    }

    document.addEventListener('keydown', down)
    return () => document.removeEventListener('keydown', down)
  }, [])

  return { open, setOpen }
}
