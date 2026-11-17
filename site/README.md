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

Most of what a reader sees comes from these files:

| File | Holds |
|---|---|
| [`src/data/personas.ts`](src/data/personas.ts) | The five personas: role, face, stack, where their part starts and ends, responsibilities, the problem, which steps their diagrams light and mark, the monitors that catch it, the investigation steps, the fix and the quiz |
| [`src/data/pipeline.ts`](src/data/pipeline.ts) | Every step an order passes through, its owner and what Datadog watches there, drawn as the interactive diagram |
| [`src/data/monitors.ts`](src/data/monitors.ts) | The monitors the scenarios alert on, as defined in the demo's Datadog org: type, target, query, threshold and message |
| [`src/data/tools.ts`](src/data/tools.ts) | The six Datadog products: the question each answers, and what it sees in this demo |
| [`src/data/setup.ts`](src/data/setup.ts) | The Set up page: what each Datadog product needs in this demo, its settings, and where the repo sets it up. Keep it in step with `infra/main.bicep` and `airflow/dags/contoso_dw/` |
| [`src/data/landscape.ts`](src/data/landscape.ts) and [`src/data/concepts.ts`](src/data/concepts.ts) | The Concepts section: the tool categories, each tool's entry, the vendor-by-job matrix, the lesson list and the glossary. The lessons themselves are in `src/pages/concepts/` |

The persona content follows "The story" and "Demo scenarios" in
`docs/data-pipeline-demo.md`, and the stacks and pipeline steps follow the code in
`services/` and `airflow/dags/`; change them together. Table, column and task names
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

## Diagrams and faces

- **The pipeline diagram** is [React Flow](https://reactflow.dev) (`@xyflow/react`,
  MIT), in `client:only` like the DRUIDS islands. A persona page opens it zoomed on that
  persona's steps and marks; the rest of the pipeline is a pan away.
- **The faces** are generated at build time with [DiceBear](https://www.dicebear.com)
  from each persona's `avatarSeed`. The artwork is "Notionists" by Zoish, under CC0 1.0.
  Change a seed to get a different face.

Lesson progress and the theme choice are kept in the reader's browser
(`localStorage`) only.

## Share image and metadata

Every page sets a canonical URL plus Open Graph and Twitter card tags in
`src/layouts/Base.astro`. The title and description come from each page's `title` and
`description` props. All pages share one image, `public/og-image.png` (1200 × 630). Its
source is [`og/og-image.html`](og/og-image.html). After editing the source, render it
again from `site/`:

```bash
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --hide-scrollbars \
  --force-device-scale-factor=1 --window-size=1200,630 \
  --screenshot="$PWD/public/og-image.png" "file://$PWD/og/og-image.html"
```

On Linux, use `google-chrome` or `chromium` in place of the macOS path.
