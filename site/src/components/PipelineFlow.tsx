// The order pipeline as an interactive React Flow diagram: every step, its
// owner, and what Datadog watches there. Click a step for its details. On a
// persona page, that persona's steps stay lit, and marks show where the
// problem starts, where Datadog alerts and what else it reaches.
import { useEffect, useMemo, useState } from "react";
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { Text } from "@datadog/druids/typography/Text";
import {
  ownerColors,
  pipelineEdges,
  pipelineNodes,
  stages,
  type DiagramMark,
  type NodeId,
  type Owner,
  type PipelineNode,
} from "../data/pipeline";
import { toolById } from "../data/tools";
import { splitNames } from "../lib/rich";
import ToolBadge from "./ToolBadge";

const SIDES = { left: Position.Left, right: Position.Right, top: Position.Top, bottom: Position.Bottom };

const SHORT: Record<string, string> = {
  djm: "DJM",
  dataObservability: "Data Obs",
  apm: "APM",
  dsm: "DSM",
  dbm: "DBM",
  bits: "Bits AI",
};

const MARK_LABEL = { problem: "Problem", alert: "Alert", impact: "Impact" } as const;

type StepData = {
  step: PipelineNode;
  avatar: string;
  ownerName: string;
  dimmed: boolean;
  marks: DiagramMark[];
  selected: boolean;
};

function Rich({ text }: { text: string }) {
  return <>{splitNames(text).map((part, i) => (i % 2 === 1 ? <code key={i}>{part}</code> : part))}</>;
}

function StepNode({ data }: NodeProps<Node<StepData>>) {
  const { step, avatar, ownerName, dimmed, marks, selected } = data;
  const colors = ownerColors[step.owner];
  return (
    <div
      className={`pf-node${dimmed ? " is-dimmed" : ""}${selected ? " is-selected" : ""}`}
      style={{ "--accent": colors.accent, "--soft": colors.soft } as React.CSSProperties}
    >
      {(Object.keys(SIDES) as (keyof typeof SIDES)[]).map((side) => (
        <span key={side}>
          <Handle type="source" id={`${side}-s`} position={SIDES[side]} isConnectable={false} />
          <Handle type="target" id={`${side}-t`} position={SIDES[side]} isConnectable={false} />
        </span>
      ))}
      <div className="pf-owner">
        <img src={avatar} alt="" width={20} height={20} />
        <span>{ownerName}</span>
        <span className="pf-stage">{stageName[step.stage]}</span>
      </div>
      <div className="pf-title">{step.title}</div>
      <div className="pf-kind">{step.kind}</div>
      <div className="pf-watch">
        {step.watchedBy.map((t) => (
          <span key={t} title={toolById[t].name}>
            {SHORT[t]}
          </span>
        ))}
      </div>
      {marks.length > 0 && (
        <div className="pf-marks">
          {marks.map((m) => (
            <span key={m.label} className={`pf-mark pf-mark-${m.kind}`}>
              {MARK_LABEL[m.kind]}: {m.label}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

const stageName = Object.fromEntries(stages.map((s) => [s.id, s.name]));

const nodeTypes = { step: StepNode };

// druids themes on the body class; follow it so the canvas matches.
function useColorMode(): "light" | "dark" {
  const read = () => (document.body.classList.contains("dark-mode") ? "dark" : "light");
  const [mode, setMode] = useState<"light" | "dark">(read);
  useEffect(() => {
    const observer = new MutationObserver(() => setMode(read()));
    observer.observe(document.body, { attributes: true, attributeFilter: ["class"] });
    return () => observer.disconnect();
  }, []);
  return mode;
}

export default function PipelineFlow({
  avatars,
  people,
  focus = [],
  marks = [],
  height = 520,
  initial,
}: {
  avatars: Record<Owner, string>;
  people: Record<Owner, { name: string; role: string; href: string }>;
  focus?: NodeId[];
  marks?: DiagramMark[];
  height?: number;
  initial?: NodeId;
}) {
  const colorMode = useColorMode();
  const [selected, setSelected] = useState<NodeId | undefined>(initial ?? focus[0]);

  const nodes = useMemo<Node<StepData>[]>(
    () =>
      pipelineNodes.map((step) => ({
        id: step.id,
        type: "step",
        position: { x: step.x, y: step.y },
        data: {
          step,
          avatar: avatars[step.owner],
          ownerName: people[step.owner].name,
          dimmed: focus.length > 0 && !focus.includes(step.id) && !marks.some((m) => m.node === step.id),
          marks: marks.filter((m) => m.node === step.id),
          selected: step.id === selected,
        },
      })),
    [avatars, people, focus, marks, selected],
  );

  const edges = useMemo<Edge[]>(
    () =>
      pipelineEdges.map((e) => {
        const lit = focus.length === 0 || focus.includes(e.from) || focus.includes(e.to);
        return {
          id: `${e.from}-${e.to}`,
          source: e.from,
          target: e.to,
          sourceHandle: `${e.fromSide}-s`,
          targetHandle: `${e.toSide}-t`,
          type: "smoothstep",
          label: e.label,
          animated: lit,
          className: lit ? "pf-edge" : "pf-edge is-dimmed",
          markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 },
        };
      }),
    [focus],
  );

  // A persona's diagram opens on their own steps and marks; the rest is a pan away.
  const fitNodes = [...new Set([...focus, ...marks.map((m) => m.node)])].map((id) => ({ id }));

  const current = pipelineNodes.find((n) => n.id === selected);
  const owner = current ? people[current.owner] : undefined;

  return (
    <div className="pipeline-flow">
      <div className="pf-canvas" style={{ height }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          colorMode={colorMode}
          fitView
          fitViewOptions={{ padding: 0.12, nodes: fitNodes.length ? fitNodes : undefined, maxZoom: 1 }}
          minZoom={0.3}
          maxZoom={1.6}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable
          zoomOnScroll={false}
          preventScrolling={false}
          onNodeClick={(_, node) => setSelected(node.id as NodeId)}
        >
          <Background gap={20} size={1} />
          <Controls showInteractive={false} position="top-right" />
        </ReactFlow>
      </div>
      <div className="pf-detail" aria-live="polite">
        {current && owner ? (
          <>
            <div className="pf-detail-head">
              <img src={avatars[current.owner]} alt="" width={40} height={40} />
              <div>
                <Text as="p" weight="bold">
                  <code>{current.title}</code> · {current.kind}
                </Text>
                <Text as="p" size="sm" variant="secondary">
                  Owned by <a href={owner.href}>{owner.name}</a>, {owner.role.toLowerCase()}
                </Text>
              </div>
            </div>
            <Text as="p">
              <Rich text={current.detail} />
            </Text>
            <div className="pf-detail-tools">
              <Text as="span" size="sm" variant="tertiary" weight="bold">
                Watched by
              </Text>
              {current.watchedBy.map((t) => (
                <ToolBadge key={t} tool={t} size="xs" />
              ))}
            </div>
          </>
        ) : (
          <Text as="p" variant="secondary">
            Select a step to see what it does, who owns it and what Datadog watches there.
          </Text>
        )}
      </div>
    </div>
  );
}
