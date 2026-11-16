import { CheckCircledIcon } from "@datadog/druids/icons/CheckCircled";
import { Text } from "@datadog/druids/typography/Text";
import { lessons } from "../lib/site";
import { useProgress } from "../lib/progress";

// The module's table of contents, with a tick on each finished lesson.
export default function LessonNav({ slug }: { slug: string }) {
  const { progress } = useProgress();
  const done = new Set(progress[slug] ?? []);
  return (
    <nav aria-label="Lessons" className="lesson-nav">
      <Text as="p" size="sm" variant="secondary" weight="bold" marginBottom="xs">
        Lessons · {done.size} of {lessons.length} done
      </Text>
      <div className="progress-track" aria-hidden="true">
        <div className="progress-fill" style={{ width: `${(done.size / lessons.length) * 100}%` }} />
      </div>
      <ol>
        {lessons.map((lesson, i) => (
          <li key={lesson.id} className={done.has(lesson.id) ? "is-done" : undefined}>
            <a href={`#${lesson.id}`}>
              <span className="lesson-index">
                {done.has(lesson.id) ? <CheckCircledIcon aria-label="Done" /> : i + 1}
              </span>
              {lesson.title}
            </a>
          </li>
        ))}
      </ol>
    </nav>
  );
}
