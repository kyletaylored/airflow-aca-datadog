import { Badge } from "@datadog/druids/pills/Badge";
import { DataJobsMonitoringIcon } from "@datadog/druids/icons/DataJobsMonitoring";
import { DataObservabilityIcon } from "@datadog/druids/icons/DataObservability";
import { TraceIcon } from "@datadog/druids/icons/Trace";
import { DatastreamsMonitoringIcon } from "@datadog/druids/icons/DatastreamsMonitoring";
import { DatabaseIcon } from "@datadog/druids/icons/Database";
import { AiIcon } from "@datadog/druids/icons/Ai";
import { toolById, type ToolId } from "../data/tools";

export const toolIcons = {
  djm: DataJobsMonitoringIcon,
  dataObservability: DataObservabilityIcon,
  apm: TraceIcon,
  dsm: DatastreamsMonitoringIcon,
  dbm: DatabaseIcon,
  bits: AiIcon,
} satisfies Record<ToolId, unknown>;

export default function ToolBadge({ tool, size = "sm" }: { tool: ToolId; size?: "xs" | "sm" | "md" }) {
  return <Badge label={toolById[tool].name} icon={toolIcons[tool]} size={size} />;
}
