# Persona site

A small static course site for the [data pipeline demo](../docs/data-pipeline-demo.md):
one module per person on Contoso's pipeline team. Each module covers what they own,
their stack, the problem in their part of the pipeline, and the Datadog tools that
catch it and find the cause. It ends with a short knowledge check.

Built with [Astro](https://astro.build) and Datadog's
[DRUIDS components](https://www.npmjs.com/package/@datadog/druids) (`@datadog/druids`,
proprietary, under the Datadog EULA). Published to
<https://kyletaylored.com/airflow-aca-datadog/> by
[`.github/workflows/pages.yml`](../.github/workflows/pages.yml) on every push to `main`
that changes `site/`.

## Edit the content

Everything a reader sees comes from two files:

| File | Holds |
|---|---|
| [`src/data/personas.ts`](src/data/personas.ts) | The five personas: role, stack, responsibilities, the problem, the detect and investigate steps (each tagged with its Datadog tool), the fix, and the quiz |
| [`src/data/tools.ts`](src/data/tools.ts) | The six Datadog products: the question each answers, and what it sees in this demo |

The persona content follows "The story" and "Demo scenarios" in
`docs/data-pipeline-demo.md`; change both together. Table, column and task names
in the text (`raw.orders`, `amount_cents`, `contoso_orders`) are set as code
automatically.

## Run it

```bash
npm ci
npm run dev       # http://localhost:4321/airflow-aca-datadog/
npm run build     # static site in dist/
```

## Working with DRUIDS

- **Some components render only in the browser.** DRUIDS icons inject their SVG sprite
  into `document` when they render, `CalloutValue` reads `matchMedia`, and
  `MessageBox`, `CalloutTag`, `Table` and `ProminentTabList` don't load in Node at all.
  So the badges, value tiles, checkboxes, status pills and the quiz use
  `client:only="react"`, each with a plain-text fallback. `Text` and the layout
  components render at build time, so every page reads without JavaScript.
- **React 18.** DRUIDS accepts React 19, but its `react-popper` and `react-table`
  dependencies only declare React 18.
- **`Button` takes its text as `label`**, not as children.
- **Theme:** DRUIDS switches on a `light-mode` or `dark-mode` class on `<body>`. The
  layout sets it before the first paint, from the reader's choice or their system
  setting.

Lesson progress and the theme choice are kept in the reader's browser
(`localStorage`) only.
