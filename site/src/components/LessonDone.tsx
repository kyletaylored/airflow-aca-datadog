import { Checkbox } from "@datadog/druids/form/Checkbox";
import { useProgress } from "../lib/progress";

export default function LessonDone({ slug, lesson }: { slug: string; lesson: string }) {
  const { progress, setDone } = useProgress();
  const isDone = progress[slug]?.includes(lesson) ?? false;
  return (
    <Checkbox
      label="Mark lesson as complete"
      isChecked={isDone}
      onChange={(event) => setDone(slug, lesson, event.target.checked)}
      size="sm"
    />
  );
}
