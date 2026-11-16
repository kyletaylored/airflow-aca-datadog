// Links that work under GitHub Pages' /<repo>/ base path.
export function href(path = ""): string {
  const base = import.meta.env.BASE_URL.replace(/\/$/, "");
  return `${base}/${path.replace(/^\//, "")}`;
}

// The lessons every persona module walks through, in order.
export const lessons = [
  { id: "meet", title: "Meet the persona" },
  { id: "tech", title: "Their stack" },
  { id: "responsibilities", title: "What they own" },
  { id: "problem", title: "The problem" },
  { id: "detect", title: "How Datadog catches it" },
  { id: "investigate", title: "Finding the cause" },
  { id: "resolve", title: "Fixing it" },
  { id: "check", title: "Knowledge check" },
] as const;

export type LessonId = (typeof lessons)[number]["id"];
