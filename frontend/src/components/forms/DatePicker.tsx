import { useState } from 'react'
import { format } from 'date-fns'
import { CalendarIcon } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Calendar } from '@/components/ui/calendar'
import { Input } from '@/components/ui/input'
import {
  Popover,
  PopoverContent,
  PopoverTrigger
} from '@/components/ui/popover'

interface DatePickerProps {
  value: string
  onChange: (value: string) => void
  placeholder?: string
}

export function DateTimePicker({ value, onChange, placeholder = 'Pick a date' }: DatePickerProps) {
  const [open, setOpen] = useState(false)

  // Parse the ISO string to a Date object
  const dateValue = value ? new Date(value) : undefined

  // Extract time from value or default to 00:00
  const timeValue = dateValue
    ? format(dateValue, 'HH:mm')
    : '00:00'

  const handleDateSelect = (date: Date | undefined) => {
    if (!date) {
      onChange('')
      return
    }

    // Preserve the time from the current value
    const [hours, minutes] = timeValue.split(':').map(Number)
    date.setHours(hours, minutes, 0, 0)
    onChange(date.toISOString())
  }

  const handleTimeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const time = e.target.value
    if (!dateValue) {
      // If no date selected, use today
      const today = new Date()
      const [hours, minutes] = time.split(':').map(Number)
      today.setHours(hours, minutes, 0, 0)
      onChange(today.toISOString())
    } else {
      // Update time on existing date
      const [hours, minutes] = time.split(':').map(Number)
      const newDate = new Date(dateValue)
      newDate.setHours(hours, minutes, 0, 0)
      onChange(newDate.toISOString())
    }
  }

  const handleClear = () => {
    onChange('')
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          className={cn(
            'w-full justify-start text-left font-normal bg-gray-700 border-gray-600 hover:bg-gray-600',
            !value && 'text-muted-foreground'
          )}
        >
          <CalendarIcon className="mr-2 h-4 w-4" />
          {dateValue ? format(dateValue, 'PPP HH:mm') : placeholder}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-auto p-0" align="start">
        <Calendar
          mode="single"
          selected={dateValue}
          onSelect={handleDateSelect}
          initialFocus
        />
        <div className="border-t border-gray-700 p-3 flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-400">Time:</span>
            <Input
              type="time"
              value={timeValue}
              onChange={handleTimeChange}
              className="w-28 h-8 bg-gray-700 border-gray-600"
            />
          </div>
          {value && (
            <Button
              variant="ghost"
              size="sm"
              onClick={handleClear}
              className="text-gray-400 hover:text-gray-200"
            >
              Clear
            </Button>
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}
