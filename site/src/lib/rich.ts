// File paths, and table, column and task names (raw.orders, amount_cents,
// contoso_orders) in plain text, so they can be set as code. Odd indexes of the result are names.
const NAMES =
  /(\b[a-z][a-z0-9_]*(?:\/[a-z0-9_.]+)+\.[a-z]+\b|\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+(?:\.[a-z0-9_]+)*\b|\b[a-z][a-z0-9_]*\.[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?\b)/g;

export function splitNames(text: string): string[] {
  return text.split(NAMES);
}
