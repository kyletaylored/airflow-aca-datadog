import { useEffect, useState } from "react";
import { CheckCircledIcon } from "@datadog/druids/icons/CheckCircled";
import { Text } from "@datadog/druids/typography/Text";
import { lessons } from "../lib/site";
import { useProgress } from "../lib/progress";

// A section counts as read once its end has stayed on screen this long, so a jump from this nav
// (which scrolls smoothly through the sections in between) doesn't tick them.
const DWELL_MS = 1000;
// The knowledge check is ticked by passing it, not by scrolling to it.
const READ_BY_SCROLLING = lessons.filter((l) => l.id !== "check").map((l) => l.id);

// The module's table of contents. It highlights the section being read, ticks each section once
// the reader reaches its end, and ticks the knowledge check when the reader passes it.
export default function LessonNav({ slug }: { slug: string }) {
  const { progress, setDone } = useProgress();
  const done = new Set(progress[slug] ?? []);
  const [current, setCurrent] = useState<string>(lessons[0].id);

  useEffect(() => {
    const endSeenAt: Record<string, number> = {};
    const marked = new Set<string>();

    function tick() {
      const now = performance.now();
      const viewport = window.innerHeight;
      // The current section: the last one whose top has passed the upper third of the screen.
      let active: string = lessons[0].id;
      for (const lesson of lessons) {
        const el = document.getElementById(lesson.id);
        if (el && el.getBoundingClientRect().top <= viewport / 3) active = lesson.id;
      }
      // At the bottom of the page, the last section is current even if it's short.
      if (window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 4) active = lessons[lessons.length - 1].id;
      setCurrent(active);

      for (const id of READ_BY_SCROLLING) {
        if (marked.has(id)) continue;
        const el = document.getElementById(id);
        if (!el) continue;
        const { bottom } = el.getBoundingClientRect();
        if (bottom > 0 && bottom <= viewport) {
          endSeenAt[id] ??= now;
          if (now - endSeenAt[id] >= DWELL_MS) {
            marked.add(id);
            setDone(slug, id, true);
          }
        } else {
          delete endSeenAt[id];
        }
      }
    }

    tick();
    const timer = window.setInterval(tick, 250);
    window.addEventListener("scroll", tick, { passive: true });
    return () => {
      window.clearInterval(timer);
      window.removeEventListener("scroll", tick);
    };
  }, [slug, setDone]);

  return (
    <nav aria-label="Lessons" className="lesson-nav">
      <Text as="p" size="sm" variant="secondary" weight="bold" marginBottom="xs">
        Lessons · {done.size} of {lessons.length} complete
      </Text>
      <div className="progress-track" aria-hidden="true">
        <div className="progress-fill" style={{ width: `${(done.size / lessons.length) * 100}%` }} />
      </div>
      <ol>
        {lessons.map((lesson, i) => {
          const classes = [done.has(lesson.id) && "is-done", current === lesson.id && "is-current"].filter(Boolean).join(" ");
          return (
            <li key={lesson.id} className={classes || undefined}>
              <a href={`#${lesson.id}`} aria-current={current === lesson.id ? "location" : undefined}>
                <span className="lesson-index">
                  {done.has(lesson.id) ? <CheckCircledIcon aria-label="Complete" /> : i + 1}
                </span>
                {lesson.title}
              </a>
            </li>
          );
        })}
      </ol>
      <Text as="p" size="xs" variant="tertiary" className="lesson-nav-hint">
        Sections are ticked as you read them. Pass the knowledge check to complete the module.
      </Text>
    </nav>
  );
}
