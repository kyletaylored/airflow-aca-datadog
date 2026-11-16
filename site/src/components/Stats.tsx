import { CalloutValue } from "@datadog/druids/measures/CalloutValue";

export interface Stat {
  value: number;
  label: string;
  unit?: string;
}

// CalloutValue reads matchMedia while rendering, so this renders in the browser only.
export default function Stats({ stats }: { stats: Stat[] }) {
  return (
    <div className="stats">
      {stats.map((s) => (
        <CalloutValue key={s.label} value={s.value} unit={s.unit} label={s.label} />
      ))}
    </div>
  );
}
