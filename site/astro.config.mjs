import { defineConfig } from "astro/config";
import react from "@astrojs/react";

// Served by GitHub Pages at https://kyletaylored.com/airflow-aca-datadog/ (the account's custom domain).
export default defineConfig({
  site: "https://kyletaylored.com",
  base: "/airflow-aca-datadog",
  integrations: [react()],
});
