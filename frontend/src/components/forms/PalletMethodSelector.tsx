import { useState, useMemo } from 'react'
import { Check, ChevronsUpDown, Plus, X } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList
} from '@/components/ui/command'
import {
  Popover,
  PopoverContent,
  PopoverTrigger
} from '@/components/ui/popover'
import { Badge } from '@/components/ui/badge'
import { extrinsicsMetadata, eventsMetadata, storageMetadata, configRules } from '@/config/metadata'
import type { PalletMetadata, PalletPair } from '@/lib/types'

interface PalletSelectorProps {
  value: string
  onChange: (value: string) => void
  metaType: 'extrinsics' | 'events'
  placeholder?: string
}

export function PalletSelector({ value, onChange, metaType, placeholder = 'Select pallet...' }: PalletSelectorProps) {
  const [open, setOpen] = useState(false)
  const metadata = metaType === 'events' ? eventsMetadata : extrinsicsMetadata

  const pallets = useMemo(() => {
    return Object.keys(metadata).sort()
  }, [metadata])

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between bg-gray-700 border-gray-600 hover:bg-gray-600"
        >
          {value || placeholder}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-full p-0" align="start">
        <Command>
          <CommandInput placeholder="Search pallets..." />
          <CommandList>
            <CommandEmpty>No pallet found.</CommandEmpty>
            <CommandGroup>
              {value && (
                <CommandItem
                  value=""
                  onSelect={() => {
                    onChange('')
                    setOpen(false)
                  }}
                  className="text-gray-400"
                >
                  Clear selection
                </CommandItem>
              )}
              {pallets.map((pallet) => (
                <CommandItem
                  key={pallet}
                  value={pallet}
                  onSelect={() => {
                    onChange(pallet)
                    setOpen(false)
                  }}
                >
                  <Check
                    className={cn(
                      'mr-2 h-4 w-4',
                      value === pallet ? 'opacity-100' : 'opacity-0'
                    )}
                  />
                  {pallet}
                  <Badge variant="secondary" className="ml-auto text-xs">
                    {metadata[pallet].length}
                  </Badge>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}

interface MethodSelectorProps {
  value: string
  onChange: (value: string) => void
  pallet: string
  metaType: 'extrinsics' | 'events'
  placeholder?: string
  label?: string // 'Method' or 'Event Variant'
}

export function MethodSelector({
  value,
  onChange,
  pallet,
  metaType,
  placeholder = 'Select method...',
  label = 'Method'
}: MethodSelectorProps) {
  const [open, setOpen] = useState(false)
  const metadata: PalletMetadata = metaType === 'events' ? eventsMetadata : extrinsicsMetadata

  // If pallet is selected, show only that pallet's methods
  // If no pallet, show ALL methods grouped by pallet
  const groupedOptions = useMemo(() => {
    if (pallet && metadata[pallet]) {
      // Single pallet selected - show flat list
      return [{
        pallet,
        methods: metadata[pallet]
      }]
    }

    // No pallet selected - show all methods grouped by pallet
    return Object.entries(metadata)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([palletName, methods]) => ({
        pallet: palletName,
        methods
      }))
  }, [pallet, metadata])

  const displayValue = value || placeholder

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between bg-gray-700 border-gray-600 hover:bg-gray-600"
        >
          <span className="truncate">{displayValue}</span>
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-80 p-0" align="start">
        <Command>
          <CommandInput placeholder={`Search ${label.toLowerCase()}s...`} />
          <CommandList className="max-h-80">
            <CommandEmpty>No {label.toLowerCase()} found.</CommandEmpty>
            {value && (
              <CommandGroup>
                <CommandItem
                  value=""
                  onSelect={() => {
                    onChange('')
                    setOpen(false)
                  }}
                  className="text-gray-400"
                >
                  Clear selection
                </CommandItem>
              </CommandGroup>
            )}
            {groupedOptions.map(({ pallet: groupPallet, methods }) => (
              <CommandGroup
                key={groupPallet}
                heading={!pallet ? groupPallet : undefined}
              >
                {methods.map((method) => (
                  <CommandItem
                    key={`${groupPallet}::${method}`}
                    value={`${groupPallet}::${method}`}
                    onSelect={() => {
                      onChange(method)
                      setOpen(false)
                    }}
                  >
                    <Check
                      className={cn(
                        'mr-2 h-4 w-4',
                        value === method ? 'opacity-100' : 'opacity-0'
                      )}
                    />
                    {method}
                  </CommandItem>
                ))}
              </CommandGroup>
            ))}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}

// Combobox for arbitrary options (storage location, config rule, etc.)
interface ComboboxSelectorProps {
  value: string
  onChange: (value: string) => void
  options: string[]
  placeholder?: string
}

export function ComboboxSelector({ value, onChange, options, placeholder = 'Select...' }: ComboboxSelectorProps) {
  const [open, setOpen] = useState(false)

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between bg-gray-700 border-gray-600 hover:bg-gray-600"
        >
          {value || placeholder}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-full p-0" align="start">
        <Command>
          <CommandInput placeholder="Search..." />
          <CommandList>
            <CommandEmpty>No option found.</CommandEmpty>
            <CommandGroup>
              {value && (
                <CommandItem
                  value=""
                  onSelect={() => {
                    onChange('')
                    setOpen(false)
                  }}
                  className="text-gray-400"
                >
                  Clear selection
                </CommandItem>
              )}
              {options.map((option) => (
                <CommandItem
                  key={option}
                  value={option}
                  onSelect={() => {
                    onChange(option)
                    setOpen(false)
                  }}
                >
                  <Check
                    className={cn(
                      'mr-2 h-4 w-4',
                      value === option ? 'opacity-100' : 'opacity-0'
                    )}
                  />
                  {option}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}

// Storage Pallet Selector (uses storageMetadata)
interface StoragePalletSelectorProps {
  value: string
  onChange: (value: string) => void
  placeholder?: string
}

export function StoragePalletSelector({ value, onChange, placeholder = 'Select pallet...' }: StoragePalletSelectorProps) {
  const [open, setOpen] = useState(false)

  const pallets = useMemo(() => {
    return Object.keys(storageMetadata).sort()
  }, [])

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between bg-gray-700 border-gray-600 hover:bg-gray-600"
        >
          {value || placeholder}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-full p-0" align="start">
        <Command>
          <CommandInput placeholder="Search pallets..." />
          <CommandList>
            <CommandEmpty>No pallet found.</CommandEmpty>
            <CommandGroup>
              {value && (
                <CommandItem
                  value=""
                  onSelect={() => {
                    onChange('')
                    setOpen(false)
                  }}
                  className="text-gray-400"
                >
                  Clear selection
                </CommandItem>
              )}
              {pallets.map((pallet) => (
                <CommandItem
                  key={pallet}
                  value={pallet}
                  onSelect={() => {
                    onChange(pallet)
                    setOpen(false)
                  }}
                >
                  <Check
                    className={cn(
                      'mr-2 h-4 w-4',
                      value === pallet ? 'opacity-100' : 'opacity-0'
                    )}
                  />
                  {pallet}
                  <Badge variant="secondary" className="ml-auto text-xs">
                    {storageMetadata[pallet].length}
                  </Badge>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}

// Storage Location Selector (dependent on pallet)
interface StorageLocationSelectorProps {
  value: string
  onChange: (value: string) => void
  pallet: string
  placeholder?: string
}

export function StorageLocationSelector({
  value,
  onChange,
  pallet,
  placeholder = 'Select storage location...'
}: StorageLocationSelectorProps) {
  const [open, setOpen] = useState(false)

  // If pallet is selected, show only that pallet's storage locations
  // If no pallet, show ALL storage locations grouped by pallet
  const groupedOptions = useMemo(() => {
    if (pallet && storageMetadata[pallet]) {
      // Single pallet selected - show flat list
      return [{
        pallet,
        locations: storageMetadata[pallet]
      }]
    }

    // No pallet selected - show all storage locations grouped by pallet
    return Object.entries(storageMetadata)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([palletName, locations]) => ({
        pallet: palletName,
        locations
      }))
  }, [pallet])

  const displayValue = value || placeholder

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between bg-gray-700 border-gray-600 hover:bg-gray-600"
        >
          <span className="truncate">{displayValue}</span>
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-80 p-0" align="start">
        <Command>
          <CommandInput placeholder="Search storage locations..." />
          <CommandList className="max-h-80">
            <CommandEmpty>No storage location found.</CommandEmpty>
            {value && (
              <CommandGroup>
                <CommandItem
                  value=""
                  onSelect={() => {
                    onChange('')
                    setOpen(false)
                  }}
                  className="text-gray-400"
                >
                  Clear selection
                </CommandItem>
              </CommandGroup>
            )}
            {groupedOptions.map(({ pallet: groupPallet, locations }) => (
              <CommandGroup
                key={groupPallet}
                heading={!pallet ? groupPallet : undefined}
              >
                {locations.map((location) => (
                  <CommandItem
                    key={`${groupPallet}::${location}`}
                    value={`${groupPallet}::${location}`}
                    onSelect={() => {
                      onChange(location)
                      setOpen(false)
                    }}
                  >
                    <Check
                      className={cn(
                        'mr-2 h-4 w-4',
                        value === location ? 'opacity-100' : 'opacity-0'
                      )}
                    />
                    {location}
                  </CommandItem>
                ))}
              </CommandGroup>
            ))}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}

// Config Rule Selector
interface ConfigRuleSelectorProps {
  value: string
  onChange: (value: string) => void
  placeholder?: string
}

// Repeating (pallet, method|variant) pairs with a + button to add more.
// Used by getExtrinsicsCount and getEventsCount.
interface PalletPairsSelectorProps {
  value: PalletPair[] | undefined
  onChange: (value: PalletPair[]) => void
  metaType: 'extrinsics' | 'events'
}

export function PalletPairsSelector({ value, onChange, metaType }: PalletPairsSelectorProps) {
  const pairs: PalletPair[] = Array.isArray(value) ? value : []
  const subKey: 'method' | 'variant' = metaType === 'events' ? 'variant' : 'method'
  const subLabel = metaType === 'events' ? 'Event Variant' : 'Method'

  const addPair = () => onChange([...pairs, { pallet: '', [subKey]: '' } as PalletPair])
  const removePair = (i: number) => onChange(pairs.filter((_, idx) => idx !== i))
  const updatePair = (i: number, key: 'pallet' | 'method' | 'variant', val: string) => {
    onChange(pairs.map((p, idx) => (idx === i ? { ...p, [key]: val } : p)))
  }

  return (
    <div className="space-y-2">
      {pairs.map((pair, i) => (
        <div key={i} className="flex items-center gap-2">
          <div className="flex-1 min-w-0">
            <PalletSelector
              value={pair.pallet || ''}
              onChange={(v) => updatePair(i, 'pallet', v)}
              metaType={metaType}
              placeholder="Pallet"
            />
          </div>
          <div className="flex-1 min-w-0">
            <MethodSelector
              value={(pair[subKey] as string) || ''}
              onChange={(v) => updatePair(i, subKey, v)}
              pallet={pair.pallet || ''}
              metaType={metaType}
              placeholder={subLabel}
              label={subLabel}
            />
          </div>
          <Button
            type="button"
            size="icon"
            variant="ghost"
            onClick={() => removePair(i)}
            className="shrink-0 h-8 w-8 text-gray-400 hover:text-red-400 hover:bg-gray-700"
            aria-label="Remove pair"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      ))}
      <Button
        type="button"
        size="sm"
        variant="outline"
        onClick={addPair}
        className="w-full border-dashed border-gray-600 bg-transparent text-gray-300 hover:bg-gray-700 hover:text-white"
      >
        <Plus className="h-3 w-3 mr-1.5" />
        Add pair
      </Button>
    </div>
  )
}

// Repeating list of free-text addresses with a + button to add more.
// Accepts hex or SS58 addresses (may be mixed) — used for address blacklist filters.
interface AddressListSelectorProps {
  value: string[] | undefined
  onChange: (value: string[]) => void
  placeholder?: string
}

export function AddressListSelector({ value, onChange, placeholder = '0x... or SS58 address' }: AddressListSelectorProps) {
  const addresses: string[] = Array.isArray(value) ? value : []

  const addAddress = () => onChange([...addresses, ''])
  const removeAddress = (i: number) => onChange(addresses.filter((_, idx) => idx !== i))
  const updateAddress = (i: number, val: string) => onChange(addresses.map((a, idx) => (idx === i ? val : a)))

  return (
    <div className="space-y-2">
      {addresses.map((address, i) => (
        <div key={i} className="flex items-center gap-2">
          <Input
            value={address}
            onChange={(e) => updateAddress(i, e.target.value)}
            placeholder={placeholder}
            className="flex-1 min-w-0 bg-gray-700 border-gray-600 focus:border-blue-500 font-mono text-sm"
          />
          <Button
            type="button"
            size="icon"
            variant="ghost"
            onClick={() => removeAddress(i)}
            className="shrink-0 h-8 w-8 text-gray-400 hover:text-red-400 hover:bg-gray-700"
            aria-label="Remove address"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      ))}
      <Button
        type="button"
        size="sm"
        variant="outline"
        onClick={addAddress}
        className="w-full border-dashed border-gray-600 bg-transparent text-gray-300 hover:bg-gray-700 hover:text-white"
      >
        <Plus className="h-3 w-3 mr-1.5" />
        Add address
      </Button>
    </div>
  )
}

export function ConfigRuleSelector({ value, onChange, placeholder = 'Select config rule...' }: ConfigRuleSelectorProps) {
  const [open, setOpen] = useState(false)

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between bg-gray-700 border-gray-600 hover:bg-gray-600"
        >
          {value || placeholder}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-full p-0" align="start">
        <Command>
          <CommandInput placeholder="Search rules..." />
          <CommandList>
            <CommandEmpty>No rule found.</CommandEmpty>
            <CommandGroup>
              {value && (
                <CommandItem
                  value=""
                  onSelect={() => {
                    onChange('')
                    setOpen(false)
                  }}
                  className="text-gray-400"
                >
                  Clear selection
                </CommandItem>
              )}
              {configRules.map((rule) => (
                <CommandItem
                  key={rule}
                  value={rule}
                  onSelect={() => {
                    onChange(rule)
                    setOpen(false)
                  }}
                >
                  <Check
                    className={cn(
                      'mr-2 h-4 w-4',
                      value === rule ? 'opacity-100' : 'opacity-0'
                    )}
                  />
                  {rule}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
