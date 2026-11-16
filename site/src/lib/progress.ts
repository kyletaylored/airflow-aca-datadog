// Per-reader progress, kept in this browser only. Storage can be missing or
// blocked (private windows, previews), so every access is guarded and the site
// works without it.
import { useCallback, useEffect, useState } from "react";

const KEY = "contoso-personas-progress-v1";
const EVENT = "personas-progress";

export type Progress = Record<string, string[]>;

function read(): Progress {
  try {
    const raw = window.localStorage.getItem(KEY);
    return raw ? (JSON.parse(raw) as Progress) : {};
  } catch {
    return {};
  }
}

function write(progress: Progress): void {
  try {
    window.localStorage.setItem(KEY, JSON.stringify(progress));
  } catch {
    // Progress just won't survive a reload.
  }
  window.dispatchEvent(new Event(EVENT));
}

// Shared by every island on the page: a change in one shows in the others.
export function useProgress() {
  const [progress, setProgress] = useState<Progress>({});

  useEffect(() => {
    const sync = () => setProgress(read());
    sync();
    window.addEventListener(EVENT, sync);
    window.addEventListener("storage", sync);
    return () => {
      window.removeEventListener(EVENT, sync);
      window.removeEventListener("storage", sync);
    };
  }, []);

  const setDone = useCallback((slug: string, lesson: string, done: boolean) => {
    const current = read();
    const lessons = new Set(current[slug] ?? []);
    if (done) lessons.add(lesson);
    else lessons.delete(lesson);
    write({ ...current, [slug]: [...lessons] });
  }, []);

  return { progress, setDone };
}
