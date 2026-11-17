// Links that work under GitHub Pages' /<repo>/ base path.
export function href(path = ""): string {
  const base = import.meta.env.BASE_URL.replace(/\/$/, "");
  return `${base}/${path.replace(/^\//, "")}`;
}

// The lessons every persona module walks through, in order.
export const lessons = [
  { id: "meet", title: "Persona overview" },
  { id: "tech", title: "Technology stack" },
  { id: "responsibilities", title: "Responsibilities" },
  { id: "problem", title: "The incident" },
  { id: "detect", title: "Detection" },
  { id: "investigate", title: "Investigation" },
  { id: "resolve", title: "Resolution" },
  { id: "check", title: "Knowledge check" },
] as const;

export type LessonId = (typeof lessons)[number]["id"];
