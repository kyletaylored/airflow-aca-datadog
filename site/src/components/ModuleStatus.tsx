import { StatusPill } from "@datadog/druids/pills/StatusPill";
import { lessons } from "../lib/site";
import { useProgress } from "../lib/progress";

// "Not started" / "3 of 8" / "Complete" for one persona module. A module is complete when its
// knowledge check is passed (the sign-off), whichever sections were read on the way.
export default function ModuleStatus({ slug }: { slug: string }) {
  const { progress } = useProgress();
  const done = progress[slug] ?? [];
  if (done.includes("check")) return <StatusPill level="success">Complete</StatusPill>;
  if (done.length === 0) return <StatusPill isSoft>Not started</StatusPill>;
  return (
    <StatusPill level="in-progress" isSoft>
      {done.length} of {lessons.length} lessons
    </StatusPill>
  );
}
