import { StatusPill } from "@datadog/druids/pills/StatusPill";
import { lessons } from "../lib/site";
import { useProgress } from "../lib/progress";

// "Not started" / "3 of 8" / "Complete" for one persona module.
export default function ModuleStatus({ slug }: { slug: string }) {
  const { progress } = useProgress();
  const done = progress[slug]?.length ?? 0;
  if (done === 0) return <StatusPill isSoft>Not started</StatusPill>;
  if (done >= lessons.length) return <StatusPill level="success">Complete</StatusPill>;
  return (
    <StatusPill level="in-progress" isSoft>
      {done} of {lessons.length} lessons
    </StatusPill>
  );
}
