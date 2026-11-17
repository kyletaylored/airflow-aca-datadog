// A small interactive diagram for the concept lessons: boxes colored by role,
// arrows between them, and (when nodes carry details) a panel that shows the
// selected node's details. React Flow needs the browser, so pages render this
// with client:only.
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
import { splitNames } from "../lib/rich";

export type Tone =
  | "app"
  | "stream"
  | "store"
  | "process"
  | "serve"
  | "control"
  | "bronze"
  | "silver"
  | "gold"
  | "warn"
  | "muted";

export interface FlowNode {
  id: string;
  label: string;
  sub?: string;
  x: number;
  y: number;
  tone: Tone;
  width?: number;
  // Shown in the panel when the node is selected.
  detail?: { text: string; items?: string[] };
}

type Side = "left" | "right" | "top" | "bottom";

export interface FlowEdge {
  from: string;
  to: string;
  label?: string;
  fromSide?: Side;
  toSide?: Side;
  dashed?: boolean;
  animated?: boolean;
}

const SIDES = { left: Position.Left, right: Position.Right, top: Position.Top, bottom: Position.Bottom };

type BoxData = FlowNode & { selected: boolean; clickable: boolean };

function Box({ data }: NodeProps<Node<BoxData>>) {
  return (
    <div
      className={`fd-node fd-${data.tone}${data.selected ? " is-selected" : ""}${data.clickable ? " is-clickable" : ""}`}
      style={{ width: data.width ?? 180 }}
    >
      {(Object.keys(SIDES) as Side[]).map((side) => (
        <span key={side}>
          <Handle type="source" id={`${side}-s`} position={SIDES[side]} isConnectable={false} />
          <Handle type="target" id={`${side}-t`} position={SIDES[side]} isConnectable={false} />
        </span>
      ))}
      <div className="fd-label">{data.label}</div>
      {data.sub && <div className="fd-sub">{data.sub}</div>}
    </div>
  );
}

const nodeTypes = { box: Box };

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

export default function FlowDiagram({
  nodes,
  edges,
  height = 360,
  initial,
  hint = "Select a box for details.",
}: {
  nodes: FlowNode[];
  edges: FlowEdge[];
  height?: number;
  initial?: string;
  hint?: string;
}) {
  const colorMode = useColorMode();
  const clickable = nodes.some((n) => n.detail);
  const [selected, setSelected] = useState<string | undefined>(initial);

  const rfNodes = useMemo<Node<BoxData>[]>(
    () =>
      nodes.map((n) => ({
        id: n.id,
        type: "box",
        position: { x: n.x, y: n.y },
        data: { ...n, selected: n.id === selected, clickable: !!n.detail },
      })),
    [nodes, selected],
  );

  const rfEdges = useMemo<Edge[]>(
    () =>
      edges.map((e) => ({
        id: `${e.from}-${e.to}`,
        source: e.from,
        target: e.to,
        sourceHandle: `${e.fromSide ?? "right"}-s`,
        targetHandle: `${e.toSide ?? "left"}-t`,
        type: "smoothstep",
        label: e.label,
        animated: e.animated ?? !e.dashed,
        className: e.dashed ? "fd-edge is-dashed" : "fd-edge",
        markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 },
      })),
    [edges],
  );

  const current = nodes.find((n) => n.id === selected);

  return (
    <div className="flow-diagram">
      <div className="fd-canvas" style={{ height }}>
        <ReactFlow
          nodes={rfNodes}
          edges={rfEdges}
          nodeTypes={nodeTypes}
          colorMode={colorMode}
          fitView
          fitViewOptions={{ padding: 0.1, maxZoom: 1.1 }}
          minZoom={0.3}
          maxZoom={1.8}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={clickable}
          zoomOnScroll={false}
          preventScrolling={false}
          onNodeClick={(_, node) => clickable && setSelected(node.id)}
        >
          <Background gap={20} size={1} />
          <Controls showInteractive={false} position="top-right" />
        </ReactFlow>
      </div>
      {clickable && (
        <div className="fd-detail" aria-live="polite">
          {current?.detail ? (
            <>
              <Text as="p" weight="bold">
                {current.label}
                {current.sub ? ` · ${current.sub}` : ""}
              </Text>
              <Text as="p">
                {splitNames(current.detail.text).map((part, i) => (i % 2 === 1 ? <code key={i}>{part}</code> : part))}
              </Text>
              {current.detail.items && (
                <div className="fd-items">
                  {current.detail.items.map((item) => (
                    <span key={item}>{item}</span>
                  ))}
                </div>
              )}
            </>
          ) : (
            <Text as="p" variant="secondary">
              {hint}
            </Text>
          )}
        </div>
      )}
    </div>
  );
}
