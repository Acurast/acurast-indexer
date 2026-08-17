/**
 * Quote a string for safe use as a single-quoted POSIX shell argument.
 * Single quotes inside the value are escaped via the '\'' idiom so a
 * crafted value (e.g. from a share link) cannot break out of the quoting
 * in generated cURL commands.
 */
export function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'"'"'`)}'`
}
