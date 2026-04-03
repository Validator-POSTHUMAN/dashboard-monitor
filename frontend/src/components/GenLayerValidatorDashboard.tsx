import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import {
  AlertTriangle,
  CheckCircle2,
  Clock3,
  Cpu,
  HardDrive,
  MemoryStick,
  RefreshCw,
  ShieldAlert,
  TrendingUp,
  Wifi,
} from "lucide-react";
import UplotReact from "uplot-react";
import type uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { Input } from "./ui/input";

type TimeRange = "1h" | "24h" | "7d";
type ValidatorStatus = "active" | "banned" | "quarantined" | "syncing" | "down";

type SeriesPointLike =
  | {
      ts?: string | number;
      time?: string | number;
      timestamp?: string | number;
      x?: string | number;
      value?: number | string;
      y?: number | string;
    }
  | [string | number, number | string];

type ValidatorRow = {
  id: string;
  moniker: string;
  address: string;
  status: ValidatorStatus;
  uptimePct: number;
  blocksBehind: number;
  latencyMs: number;
  throughputTps: number;
  lastSeen: string;
};

type Summary = {
  network: string;
  validatorStatus: ValidatorStatus;
  overallHealth: "up" | "down";
  protocolVersion: string;
  nodeVersion: string;
  synced: boolean;
  latestBlock: number;
  syncedBlock: number;
  blocksBehind: number;
  uptimeSeconds: number;
  uptimePct24h: number;
  txThroughputTps: number;
  latencyMsP50: number;
  latencyMsP95: number;
  activeValidators: number;
  bannedValidators: number;
  quarantinedValidators: number;
  cpuPct: number;
  memoryPct: number;
  dataSource?: "live" | "sqlite" | string;
  isStale?: boolean;
  snapshotRecordedAt?: string | null;
};

type PackageVersionRow = {
  name: string;
  version: string;
  source: string;
  updatedAt?: string;
};

type History = {
  throughput: SeriesPointLike[];
  latencyP50: SeriesPointLike[];
  latencyP95: SeriesPointLike[];
  blocksBehind: SeriesPointLike[];
  uptimePct: SeriesPointLike[];
  commitRate: SeriesPointLike[];
  revealRate: SeriesPointLike[];
  latestBlock: SeriesPointLike[];
  syncedBlock: SeriesPointLike[];
  acceptedTxRate: SeriesPointLike[];
  cpuNodePct: SeriesPointLike[];
  cpuWebdriverPct: SeriesPointLike[];
  cpuGenvmLlmPct: SeriesPointLike[];
  cpuGenvmWebPct: SeriesPointLike[];
  memoryNodePct: SeriesPointLike[];
  memoryWebdriverPct: SeriesPointLike[];
  memoryNodeRssBytes: SeriesPointLike[];
  diskDbUsagePct: SeriesPointLike[];
  diskLogsUsagePct: SeriesPointLike[];
  networkNodeRxBps: SeriesPointLike[];
  networkNodeTxBps: SeriesPointLike[];
  wsActiveNewHeads: SeriesPointLike[];
  wsMessagesReceivedRate: SeriesPointLike[];
  processOpenFds: SeriesPointLike[];
  goGoroutines: SeriesPointLike[];
};

type ApiPayload = {
  summary: Summary;
  history: History;
  validators: ValidatorRow[];
  packageVersions?: PackageVersionRow[];
};

type GraphMetricDefinition = {
  key: keyof History | string;
  title: string;
  group: string;
  description: string;
  unit: string;
};

type SavedGraph = {
  id: string;
  title: string;
  metricKeys: string[];
};

type GraphConfigPayload = {
  metrics: GraphMetricDefinition[];
  graphs: SavedGraph[];
};

const API_BASE = "/api/genlayer";
const UPLOT_SYNC_KEY = "genlayer-sync";
const CUSTOM_GRAPH_COLORS = [
  "#818cf8",
  "#22c55e",
  "#f59e0b",
  "#60a5fa",
  "#eab308",
  "#34d399",
  "#f472b6",
  "#38bdf8",
  "#fb923c",
  "#a78bfa",
];

const EMPTY_HISTORY: History = {
  throughput: [],
  latencyP50: [],
  latencyP95: [],
  blocksBehind: [],
  uptimePct: [],
  commitRate: [],
  revealRate: [],
  latestBlock: [],
  syncedBlock: [],
  acceptedTxRate: [],
  cpuNodePct: [],
  cpuWebdriverPct: [],
  cpuGenvmLlmPct: [],
  cpuGenvmWebPct: [],
  memoryNodePct: [],
  memoryWebdriverPct: [],
  memoryNodeRssBytes: [],
  diskDbUsagePct: [],
  diskLogsUsagePct: [],
  networkNodeRxBps: [],
  networkNodeTxBps: [],
  wsActiveNewHeads: [],
  wsMessagesReceivedRate: [],
  processOpenFds: [],
  goGoroutines: [],
};

function statusTone(status: ValidatorStatus) {
  switch (status) {
    case "active":
      return "bg-emerald-500/15 text-emerald-400 border-emerald-500/30";
    case "banned":
      return "bg-red-500/15 text-red-400 border-red-500/30";
    case "quarantined":
      return "bg-amber-500/15 text-amber-400 border-amber-500/30";
    case "syncing":
      return "bg-sky-500/15 text-sky-400 border-sky-500/30";
    default:
      return "bg-zinc-500/15 text-zinc-300 border-zinc-500/30";
  }
}

function healthTone(health: "up" | "down" | null) {
  if (health === "up") return "bg-emerald-500/15 text-emerald-400 border-emerald-500/30";
  if (health === "down") return "bg-red-500/15 text-red-400 border-red-500/30";
  return "bg-zinc-500/15 text-zinc-300 border-zinc-500/30";
}

function sourceTone(source?: string, stale?: boolean) {
  if (source === "live" && !stale) return "bg-emerald-500/15 text-emerald-400 border-emerald-500/30";
  if (source === "sqlite" || stale) return "bg-amber-500/15 text-amber-400 border-amber-500/30";
  return "bg-zinc-500/15 text-zinc-300 border-zinc-500/30";
}

function sourceLabel(source?: string, stale?: boolean) {
  if (source === "live" && !stale) return "live";
  if (source === "sqlite") return stale ? "sqlite fallback • stale" : "sqlite fallback";
  if (stale) return "stale";
  return "source pending";
}

function formatDateTime(ts: string | null | undefined) {
  if (!ts) return "—";
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString();
}

function formatUptime(seconds: number) {
  const safe = Number.isFinite(seconds) ? Math.max(0, seconds) : 0;
  const days = Math.floor(safe / 86400);
  const hours = Math.floor((safe % 86400) / 3600);
  const minutes = Math.floor((safe % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

function formatBytes(bytes: number) {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  return `${value.toFixed(value >= 100 ? 0 : value >= 10 ? 1 : 2)} ${units[idx]}`;
}

function formatRateBps(bytesPerSec: number) {
  return `${formatBytes(bytesPerSec)}/s`;
}

function formatMetricByUnit(value: number, unit: string) {
  switch (unit) {
    case "percent":
      return `${value.toFixed(2)}%`;
    case "ms":
      return `${value.toFixed(0)} ms`;
    case "tps":
      return `${value.toFixed(3)} TPS`;
    case "bytes":
      return formatBytes(value);
    case "bytes_per_sec":
      return formatRateBps(value);
    case "blocks":
      return `${value.toFixed(0)} blocks`;
    case "count":
      return `${value.toFixed(0)}`;
    case "rate":
      return `${value.toFixed(3)}`;
    default:
      return `${value.toFixed(2)}`;
  }
}

function axisFormatterByUnit(unit: string) {
  switch (unit) {
    case "percent":
      return (value: number) => `${value.toFixed(0)}%`;
    case "ms":
      return (value: number) => `${value.toFixed(0)} ms`;
    case "bytes":
      return (value: number) => formatBytes(value);
    case "bytes_per_sec":
      return (value: number) => formatBytes(value);
    case "blocks":
    case "count":
      return (value: number) => `${value.toFixed(0)}`;
    case "tps":
      return (value: number) => `${value.toFixed(2)}`;
    case "rate":
      return (value: number) => `${value.toFixed(2)}`;
    default:
      return (value: number) => `${value.toFixed(2)}`;
  }
}

function metricValue(value: string | number | null | undefined, suffix = "") {
  if (value === null || value === undefined || value === "") return "—";
  return `${value}${suffix}`;
}

function normalizeSeries(points?: SeriesPointLike[]) {
  return (points ?? [])
    .map((point) => {
      if (Array.isArray(point) && point.length >= 2) {
        const rawTs = point[0];
        const rawValue = point[1];
        const value = Number(rawValue);
        if (rawTs === null || rawTs === undefined || !Number.isFinite(value)) return null;
        const ts = typeof rawTs === "number" ? rawTs / 1000 : new Date(rawTs).getTime() / 1000;
        if (!Number.isFinite(ts)) return null;
        return [ts, value] as [number, number];
      }

      if (!point || typeof point !== "object") return null;

      const obj = point as Exclude<SeriesPointLike, [string | number, number | string]>;
      const rawTs = obj.ts ?? obj.time ?? obj.timestamp ?? obj.x;
      const rawValue = obj.value ?? obj.y;
      const value = Number(rawValue);
      if (rawTs === null || rawTs === undefined || !Number.isFinite(value)) return null;
      const ts = typeof rawTs === "number" ? rawTs / 1000 : new Date(rawTs).getTime() / 1000;
      if (!Number.isFinite(ts)) return null;
      return [ts, value] as [number, number];
    })
    .filter((item): item is [number, number] => item !== null)
    .sort((a, b) => a[0] - b[0]);
}

function hasSeries(points?: SeriesPointLike[]) {
  return normalizeSeries(points).length > 1;
}

function isHistoryMetricKey(key: string): key is keyof History {
  return key in EMPTY_HISTORY;
}

function seriesForMetric(history: History, key: string): SeriesPointLike[] {
  if (!isHistoryMetricKey(key)) return [];
  return history[key] ?? [];
}

function mixedUnits(metrics: GraphMetricDefinition[]) {
  const units = new Set(metrics.map((metric) => metric.unit));
  return units.size > 1;
}

function parseErrorDetail(body: unknown, status: number) {
  if (body && typeof body === "object" && "detail" in body) {
    const detail = (body as { detail?: unknown }).detail;
    if (typeof detail === "string" && detail.trim()) return detail;
  }
  return `dashboard fetch failed with ${status}`;
}

function normalizeValidators(rows: unknown): ValidatorRow[] {
  if (!Array.isArray(rows)) return [];
  return rows.map((row, idx) => {
    const r = (row ?? {}) as Record<string, unknown>;
    const statusRaw = typeof r.status === "string" ? r.status : "down";
    const status: ValidatorStatus =
      statusRaw === "active" ||
      statusRaw === "banned" ||
      statusRaw === "quarantined" ||
      statusRaw === "syncing" ||
      statusRaw === "down"
        ? statusRaw
        : "down";

    return {
      id: typeof r.id === "string" ? r.id : String(idx + 1),
      moniker: typeof r.moniker === "string" ? r.moniker : "unknown",
      address: typeof r.address === "string" ? r.address : "—",
      status,
      uptimePct: Number(r.uptimePct ?? 0),
      blocksBehind: Number(r.blocksBehind ?? 0),
      latencyMs: Number(r.latencyMs ?? 0),
      throughputTps: Number(r.throughputTps ?? 0),
      lastSeen: typeof r.lastSeen === "string" ? r.lastSeen : "",
    };
  });
}

function useWindowWidth() {
  const [width, setWidth] = useState<number>(typeof window !== "undefined" ? window.innerWidth : 1440);

  useEffect(() => {
    const onResize = () => setWidth(window.innerWidth);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  return width;
}

function MetricCard({
  title,
  value,
  hint,
  icon,
}: {
  title: string;
  value: string;
  hint: string;
  icon: ReactNode;
}) {
  return (
    <Card className="rounded-2xl border-zinc-800 bg-zinc-950/70 shadow-sm">
      <CardContent className="p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-sm text-zinc-400">{title}</p>
            <p className="mt-2 text-2xl font-semibold tracking-tight text-white">{value}</p>
            <p className="mt-1 text-xs text-zinc-500">{hint}</p>
          </div>
          <div className="rounded-2xl border border-zinc-800 bg-zinc-900 p-3 text-zinc-300">
            {icon}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function MiniBar({ title, value, max = 100 }: { title: string; value: number; max?: number }) {
  const pct = Math.max(0, Math.min(100, (value / max) * 100));
  return (
    <div>
      <div className="mb-2 flex items-center justify-between text-sm text-zinc-300">
        <span>{title}</span>
        <span>{value.toFixed(1)}%</span>
      </div>
      <div className="h-2 rounded-full bg-zinc-900">
        <div className="h-2 rounded-full bg-zinc-300" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function buildUplotOptions(args: {
  width: number;
  height?: number;
  seriesCount: number;
  colors: string[];
  valueFormatter?: (value: number) => string;
  axisFormatter?: (value: number) => string;
}): uPlot.Options {
  const { width, height = 300, seriesCount, colors, valueFormatter, axisFormatter } = args;

  return {
    width,
    height,
    legend: {
      show: false,
    },
    cursor: {
      sync: {
        key: UPLOT_SYNC_KEY,
      },
      drag: {
        x: true,
        y: false,
        setScale: true,
      },
      x: true,
      y: true,
    },
    select: {
      show: true,
      left: 0,
      top: 0,
      width: 0,
      height: 0,
    },
    tzDate: (ts: number) => new Date(ts * 1000),
    scales: {
      x: { time: true },
      y: { auto: true },
    },
    axes: [
      {
        stroke: "#52525b",
        grid: { stroke: "#18181b" },
        size: 40,
        space: 24,
        font: "10px Inter, sans-serif",
        values: (_u: uPlot, splits: number[]) =>
          splits.map((v) => {
            const d = new Date(v * 1000);
            return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
          }),
      },
      {
        stroke: "#52525b",
        grid: { stroke: "#18181b" },
        size: 46,
        space: 28,
        font: "10px Inter, sans-serif",
        values: (_u: uPlot, splits: number[]) =>
          splits.map((v) => (axisFormatter ? axisFormatter(v) : String(Number(v.toFixed(2))))),
      },
    ],
    series: [
      {},
      ...Array.from({ length: seriesCount }, (_, i) => ({
        label: `s${i + 1}`,
        stroke: colors[i] ?? "#a1a1aa",
        width: 2,
        points: { show: false },
        value: (_u: uPlot, raw: number | null) => {
          if (raw === null || raw === undefined) return "—";
          return valueFormatter ? valueFormatter(raw) : String(raw);
        },
      })),
    ],
  };
}

type ChartSeries = {
  label: string;
  points?: SeriesPointLike[];
  stroke: string;
  formatter?: (value: number) => string;
};

function UplotPanel({
  title,
  series,
  formatter,
  axisFormatter,
  height = 300,
}: {
  title: string;
  series: ChartSeries[];
  formatter?: (value: number) => string;
  axisFormatter?: (value: number) => string;
  height?: number;
}) {
  const windowWidth = useWindowWidth();
  const [wrapWidth, setWrapWidth] = useState(300);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const width = useMemo(() => Math.max(300, wrapWidth - 8), [wrapWidth]);

  const normalized = useMemo(() => {
    return series.map((item) => ({
      label: item.label,
      stroke: item.stroke,
      rows: normalizeSeries(item.points),
    }));
  }, [series]);

  const empty = useMemo(() => normalized.every((item) => item.rows.length < 2), [normalized]);

  const data = useMemo<uPlot.AlignedData>(() => {
    const x = Array.from(new Set(normalized.flatMap((item) => item.rows.map((row) => row[0])))).sort((a, b) => a - b);
    const out: [number[], ...((number | null)[])[]] = [x];

    for (const item of normalized) {
      const map = new Map(item.rows.map((r) => [r[0], r[1]]));
      out.push(x.map((ts) => map.get(ts) ?? null));
    }

    return out;
  }, [normalized]);

  const options = useMemo(() => {
    const opts = buildUplotOptions({
      width,
      height,
      seriesCount: series.length,
      colors: series.map((s) => s.stroke),
      valueFormatter: formatter,
      axisFormatter,
    });

    opts.series = [
      {},
      ...series.map((s) => ({
        label: s.label,
        stroke: s.stroke,
        width: 2,
        points: { show: false },
        value: (_u: uPlot, raw: number | null) => {
          if (raw === null || raw === undefined) return "—";
          return formatter ? formatter(raw) : String(raw);
        },
      })),
    ];

    opts.hooks = {
      setCursor: [
        (u: uPlot) => {
          const idx = u.cursor.idx;
          setHoverIdx(typeof idx === "number" ? idx : null);
        },
      ],
    };

    return opts;
  }, [width, height, formatter, axisFormatter, series]);

  const hoverTime = useMemo(() => {
    if (hoverIdx === null) return null;
    const ts = data?.[0]?.[hoverIdx];
    if (typeof ts !== "number") return null;
    return new Date(ts * 1000);
  }, [hoverIdx, data]);

  return (
    <div
      ref={(el) => {
        if (!el) return;
        const next = el.clientWidth;
        if (next && next !== wrapWidth) setWrapWidth(next);
      }}
      className="rounded-2xl border border-zinc-800 bg-zinc-950/70 p-2 shadow-sm"
    >
      <div className="mb-2 flex items-center justify-between gap-3">
        <div className="text-sm font-medium text-white">{title}</div>
        <div className="flex flex-wrap items-center gap-3 text-[11px] text-zinc-400">
          {series.map((s) => (
            <div key={s.label} className="flex items-center gap-1.5">
              <span
                className="inline-block h-2 w-2 rounded-full"
                style={{ backgroundColor: s.stroke }}
              />
              <span>{s.label}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="relative">
        {empty ? (
          <div className="flex h-[300px] items-center justify-center text-sm text-zinc-500">
            No historical data yet
          </div>
        ) : (
          <>
            <UplotReact options={options} data={data} />
            {hoverIdx !== null && hoverTime ? (
              <div className="pointer-events-none absolute left-3 top-3 rounded-xl border border-zinc-700 bg-zinc-950/95 px-3 py-2 text-[11px] shadow-lg">
                <div className="mb-1 text-zinc-400">
                  {hoverTime.toLocaleDateString()}{" "}
                  {hoverTime.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
                </div>
                <div className="space-y-1">
                  {series.map((s, i) => {
                    const raw = data[i + 1]?.[hoverIdx];
                    const valueFormatter = s.formatter ?? formatter;
                    const value =
                      typeof raw === "number"
                        ? valueFormatter
                          ? valueFormatter(raw)
                          : String(raw)
                        : "—";

                    return (
                      <div key={s.label} className="flex items-center gap-2 text-zinc-200">
                        <span
                          className="inline-block h-2 w-2 rounded-full"
                          style={{ backgroundColor: s.stroke }}
                        />
                        <span className="min-w-[72px] text-zinc-400">{s.label}</span>
                        <span>{value}</span>
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : null}
          </>
        )}
      </div>
    </div>
  );
}

function PackageVersionsPanel({ rows }: { rows: PackageVersionRow[] }) {
  if (!rows.length) return null;

  return (
    <Card className="rounded-3xl border-zinc-800 bg-zinc-950/70 shadow-sm">
      <CardHeader className="px-5 pt-5 pb-3">
        <CardTitle className="text-base">Package versions</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3 px-5 pb-5 pt-0">
        {rows.map((row, idx) => (
          <div key={`${row.name}-${row.source}-${idx}`} className="flex items-center justify-between gap-3 text-sm">
            <div>
              <div className="font-medium text-white">{row.name}</div>
              <div className="text-xs text-zinc-500">{row.source}</div>
            </div>
            <div className="text-right">
              <div className="text-zinc-200">{row.version}</div>
              <div className="text-xs text-zinc-500">{formatDateTime(row.updatedAt)}</div>
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

export default function GenLayerValidatorDashboard() {
  const [range, setRange] = useState<TimeRange>("24h");
  const [payload, setPayload] = useState<ApiPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [query, setQuery] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);
  const [graphMetrics, setGraphMetrics] = useState<GraphMetricDefinition[]>([]);
  const [savedGraphs, setSavedGraphs] = useState<SavedGraph[]>([]);
  const [graphsLoading, setGraphsLoading] = useState(true);
  const [graphsSaving, setGraphsSaving] = useState(false);
  const [graphTitle, setGraphTitle] = useState("");
  const [selectedMetricKeys, setSelectedMetricKeys] = useState<string[]>([]);

  const load = useCallback(
    async (mode: "initial" | "refresh" = "refresh") => {
      const isInitial = mode === "initial";
      if (isInitial) setLoading(true);
      else setRefreshing(true);

      try {
        const res = await fetch(`${API_BASE}/dashboard?range=${range}`, { cache: "no-store" });
        const body = (await res.json().catch(() => null)) as unknown;
        if (!res.ok) throw new Error(parseErrorDetail(body, res.status));

        const raw = (body ?? {}) as Record<string, unknown>;
        const nextPayload: ApiPayload = {
          summary: (raw.summary ?? {}) as Summary,
          history: (raw.history ?? EMPTY_HISTORY) as History,
          validators: normalizeValidators(raw.validators),
          packageVersions: Array.isArray(raw.packageVersions)
            ? (raw.packageVersions as PackageVersionRow[])
            : [],
        };

        setPayload(nextPayload);
        setError(null);
        setLastUpdated(new Date().toISOString());
      } catch (err) {
        const message = err instanceof Error ? err.message : "Failed to load dashboard data";
        setError(message);
      } finally {
        if (isInitial) setLoading(false);
        else setRefreshing(false);
      }
    },
    [range],
  );

  useEffect(() => {
    void load("initial");
  }, [range, load]);

  useEffect(() => {
    let active = true;

    const run = async () => {
      setGraphsLoading(true);
      try {
        const res = await fetch(`${API_BASE}/graph-config`, { cache: "no-store" });
        const body = (await res.json().catch(() => null)) as GraphConfigPayload | null;
        if (!res.ok || !body) throw new Error(`graph-config fetch failed with ${res.status}`);
        if (!active) return;
        setGraphMetrics(Array.isArray(body.metrics) ? body.metrics : []);
        setSavedGraphs(Array.isArray(body.graphs) ? body.graphs : []);
      } catch (err) {
        console.error(err);
      } finally {
        if (active) setGraphsLoading(false);
      }
    };

    void run();
    return () => {
      active = false;
    };
  }, []);

  const summary = payload?.summary ?? null;
  const history = payload?.history ?? EMPTY_HISTORY;
  const validators = payload?.validators ?? [];
  const packageVersions = payload?.packageVersions ?? [];

  const persistGraphs = useCallback(async (graphs: SavedGraph[]) => {
    setGraphsSaving(true);
    try {
      const res = await fetch(`${API_BASE}/graph-config`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ graphs }),
      });
      const body = (await res.json().catch(() => null)) as GraphConfigPayload | null;
      if (!res.ok || !body) throw new Error(`graph-config save failed with ${res.status}`);
      setGraphMetrics(Array.isArray(body.metrics) ? body.metrics : []);
      setSavedGraphs(Array.isArray(body.graphs) ? body.graphs : []);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to save graph config";
      setError(message);
    } finally {
      setGraphsSaving(false);
    }
  }, []);

  const toggleDraftMetric = useCallback((key: string) => {
    setSelectedMetricKeys((current) =>
      current.includes(key) ? current.filter((item) => item !== key) : [...current, key],
    );
  }, []);

  const createCustomGraph = useCallback(async () => {
    const title = graphTitle.trim();
    if (!title || selectedMetricKeys.length === 0) return;
    const graph: SavedGraph = {
      id: `graph-${Date.now()}`,
      title,
      metricKeys: selectedMetricKeys,
    };
    await persistGraphs([...savedGraphs, graph]);
    setGraphTitle("");
    setSelectedMetricKeys([]);
  }, [graphTitle, selectedMetricKeys, persistGraphs, savedGraphs]);

  const deleteCustomGraph = useCallback(
    async (graphId: string) => {
      await persistGraphs(savedGraphs.filter((graph) => graph.id !== graphId));
    },
    [persistGraphs, savedGraphs],
  );

  const filteredValidators = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return validators;
    return validators.filter(
      (v) =>
        v.moniker.toLowerCase().includes(q) ||
        v.address.toLowerCase().includes(q) ||
        v.status.toLowerCase().includes(q),
    );
  }, [validators, query]);

  const metricMap = useMemo(() => {
    return new Map(graphMetrics.map((metric) => [metric.key, metric]));
  }, [graphMetrics]);

  const customGraphPanels = useMemo(() => {
    return savedGraphs.map((graph) => {
      const metrics = graph.metricKeys
        .map((key) => metricMap.get(key))
        .filter((metric): metric is GraphMetricDefinition => Boolean(metric));

      const panelSeries: ChartSeries[] = metrics.map((metric, index) => ({
        label: metric.title,
        points: seriesForMetric(history, metric.key),
        stroke: CUSTOM_GRAPH_COLORS[index % CUSTOM_GRAPH_COLORS.length],
        formatter: (value: number) => formatMetricByUnit(value, metric.unit),
      }));

      const axisFormatter = metrics.length > 0 && !mixedUnits(metrics) ? axisFormatterByUnit(metrics[0].unit) : undefined;

      return {
        id: graph.id,
        title: graph.title,
        metrics,
        panelSeries,
        axisFormatter,
      };
    });
  }, [savedGraphs, metricMap, history]);

  return (
    <div className="min-h-screen bg-black text-white">
      <div className="mx-auto max-w-[1680px] px-4 py-6 md:px-5 md:py-6">
        <div className="mb-8 grid gap-4 lg:grid-cols-[1.35fr_0.65fr]">
          <Card className="rounded-3xl border-zinc-800 bg-gradient-to-br from-zinc-950 to-zinc-900 shadow-sm">
            <CardContent className="flex flex-col gap-6 p-6 md:flex-row md:items-center md:justify-between">
              <div>
                <div className="flex flex-wrap items-center gap-3">
                  <h1 className="text-3xl font-semibold tracking-tight">GenLayer Network Dashboard</h1>
                  <Badge className={statusTone(summary?.validatorStatus ?? "down")}>
                    {summary?.validatorStatus ?? "unknown"}
                  </Badge>
                  <Badge variant="outline" className="border-zinc-700 text-zinc-300">
                    {summary?.network ?? "network pending"}
                  </Badge>
                  <Badge className={healthTone(summary?.overallHealth ?? null)}>
                    {summary?.overallHealth === "up"
                      ? "health OK"
                      : summary?.overallHealth === "down"
                        ? "health DOWN"
                        : "health pending"}
                  </Badge>
                  <Badge className={sourceTone(summary?.dataSource, summary?.isStale)}>
                    {sourceLabel(summary?.dataSource, summary?.isStale)}
                  </Badge>
                </div>

                <p className="mt-3 max-w-3xl text-sm text-zinc-400">
                  uPlot time series with Grafana-like behavior. Data refreshes only on first open,
                  range change, or the Refresh button.
                </p>

                <div className="mt-4 flex flex-wrap gap-2 text-xs text-zinc-500">
                  <span>node {summary?.nodeVersion ?? "—"}</span>
                  <span>•</span>
                  <span>protocol {summary?.protocolVersion ?? "—"}</span>
                  <span>•</span>
                  <span>last update {formatDateTime(lastUpdated)}</span>
                  <span>•</span>
                  <span>snapshot {formatDateTime(summary?.snapshotRecordedAt)}</span>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 md:grid-cols-2 xl:grid-cols-4">
                <Button
                  variant={range === "1h" ? "default" : "outline"}
                  onClick={() => setRange("1h")}
                  className="rounded-2xl"
                >
                  1h
                </Button>
                <Button
                  variant={range === "24h" ? "default" : "outline"}
                  onClick={() => setRange("24h")}
                  className="rounded-2xl"
                >
                  24h
                </Button>
                <Button
                  variant={range === "7d" ? "default" : "outline"}
                  onClick={() => setRange("7d")}
                  className="rounded-2xl"
                >
                  7d
                </Button>
                <Button
                  variant="outline"
                  onClick={() => void load("refresh")}
                  className="rounded-2xl"
                  disabled={refreshing}
                >
                  <RefreshCw className={`mr-2 h-4 w-4 ${refreshing ? "animate-spin" : ""}`} />
                  Refresh
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card className="rounded-3xl border-zinc-800 bg-zinc-950/70 shadow-sm">
            <CardHeader className="px-5 pt-5 pb-3">
              <CardTitle className="text-base">Node snapshot</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 px-5 pb-5 pt-0 text-sm text-zinc-300">
              <div className="flex items-center justify-between">
                <span>Synced</span>
                <span>{summary ? (summary.synced ? "yes" : "no") : "—"}</span>
              </div>
              <div className="flex items-center justify-between">
                <span>Latest block</span>
                <span>{summary ? summary.latestBlock.toLocaleString() : "—"}</span>
              </div>
              <div className="flex items-center justify-between">
                <span>Synced block</span>
                <span>{summary ? summary.syncedBlock.toLocaleString() : "—"}</span>
              </div>
              <div className="flex items-center justify-between">
                <span>Blocks behind</span>
                <span>{summary?.blocksBehind ?? "—"}</span>
              </div>
              <div className="flex items-center justify-between">
                <span>Uptime</span>
                <span>{summary ? formatUptime(summary.uptimeSeconds) : "—"}</span>
              </div>
              <div className="flex items-center justify-between">
                <span>24h uptime</span>
                <span>{summary ? `${summary.uptimePct24h.toFixed(2)}%` : "—"}</span>
              </div>
            </CardContent>
          </Card>
        </div>

        {error ? (
          <Card className="mb-4 rounded-3xl border-amber-500/30 bg-amber-500/10 shadow-sm">
            <CardContent className="flex items-start gap-3 p-4 text-sm text-amber-100">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <div>
                <div className="font-medium">Data source warning</div>
                <p className="mt-1 text-amber-100/80">{error}</p>
              </div>
            </CardContent>
          </Card>
        ) : null}

        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-6">
          <MetricCard
            title="Throughput"
            value={summary ? `${summary.txThroughputTps.toFixed(2)} TPS` : "—"}
            hint="Accepted + synced transactions"
            icon={<TrendingUp className="h-5 w-5" />}
          />
          <MetricCard
            title="Latency"
            value={summary ? `${summary.latencyMsP50} / ${summary.latencyMsP95} ms` : "—"}
            hint="p50 / p95 request latency"
            icon={<Clock3 className="h-5 w-5" />}
          />
          <MetricCard
            title="Sync lag"
            value={summary ? `${summary.blocksBehind} blocks` : "—"}
            hint="Difference from latest known block"
            icon={<Wifi className="h-5 w-5" />}
          />
          <MetricCard
            title="Validator set"
            value={summary ? `${summary.activeValidators} active` : "—"}
            hint={
              summary
                ? `${summary.bannedValidators} banned • ${summary.quarantinedValidators} quarantined`
                : "Validator state pending"
            }
            icon={<ShieldAlert className="h-5 w-5" />}
          />
          <MetricCard
            title="CPU"
            value={summary ? metricValue(summary.cpuPct.toFixed(1), "%") : "—"}
            hint="Node CPU usage"
            icon={<Cpu className="h-5 w-5" />}
          />
          <MetricCard
            title="Memory"
            value={summary ? metricValue(summary.memoryPct.toFixed(1), "%") : "—"}
            hint="Node memory usage"
            icon={<MemoryStick className="h-5 w-5" />}
          />
        </div>

        <div className="mt-6 grid gap-4 xl:grid-cols-3">
          <UplotPanel
            title="Transaction throughput"
            series={[{ label: "TPS", points: history.throughput, stroke: "#818cf8" }]}
            formatter={(v) => `${v.toFixed(3)} TPS`}
            axisFormatter={(v) => `${v.toFixed(2)}`}
          />
          <UplotPanel
            title="Blocks behind"
            series={[{ label: "lag", points: history.blocksBehind, stroke: "#f59e0b" }]}
            formatter={(v) => `${v.toFixed(0)} blocks`}
            axisFormatter={(v) => `${v.toFixed(0)}`}
          />
          <UplotPanel
            title="Historical uptime"
            series={[{ label: "uptime", points: history.uptimePct, stroke: "#22c55e" }]}
            formatter={(v) => `${v.toFixed(2)}%`}
            axisFormatter={(v) => `${v.toFixed(0)}%`}
          />

          <UplotPanel
            title="Latency p50 / p95"
            series={[
              { label: "p50", points: history.latencyP50, stroke: "#60a5fa" },
              { label: "p95", points: history.latencyP95, stroke: "#eab308" },
            ]}
            formatter={(v) => `${v.toFixed(0)} ms`}
            axisFormatter={(v) => `${v.toFixed(0)} ms`}
          />
          <UplotPanel
            title="Commit / reveal rate"
            series={[
              { label: "commit", points: history.commitRate, stroke: "#a78bfa" },
              { label: "reveal", points: history.revealRate, stroke: "#34d399" },
            ]}
            formatter={(v) => `${v.toFixed(3)}`}
            axisFormatter={(v) => `${v.toFixed(1)}`}
          />
          <UplotPanel
            title="Latest / synced block"
            series={[
              { label: "latest", points: history.latestBlock, stroke: "#60a5fa" },
              { label: "synced", points: history.syncedBlock, stroke: "#a3e635" },
            ]}
            formatter={(v) => `${v.toFixed(0)}`}
            axisFormatter={(v) => `${Math.round(v)}`}
          />

          <UplotPanel
            title="CPU by process"
            series={[
              { label: "node", points: history.cpuNodePct, stroke: "#818cf8" },
              { label: "webdriver", points: history.cpuWebdriverPct, stroke: "#a3e635" },
              { label: "genvm-llm", points: history.cpuGenvmLlmPct, stroke: "#94a3b8" },
              { label: "genvm-web", points: history.cpuGenvmWebPct, stroke: "#fb923c" },
            ]}
            formatter={(v) => `${v.toFixed(1)}%`}
            axisFormatter={(v) => `${v.toFixed(0)}%`}
          />
          <UplotPanel
            title="Memory usage"
            series={[
              { label: "node", points: history.memoryNodePct, stroke: "#818cf8" },
              { label: "webdriver", points: history.memoryWebdriverPct, stroke: "#a3e635" },
            ]}
            formatter={(v) => `${v.toFixed(1)}%`}
            axisFormatter={(v) => `${v.toFixed(0)}%`}
          />
          <UplotPanel
            title="Node RSS memory"
            series={[{ label: "rss", points: history.memoryNodeRssBytes, stroke: "#818cf8" }]}
            formatter={(v) => formatBytes(v)}
            axisFormatter={(v) => formatBytes(v)}
          />

          <UplotPanel
            title="Disk usage"
            series={[
              { label: "db", points: history.diskDbUsagePct, stroke: "#60a5fa" },
              { label: "logs", points: history.diskLogsUsagePct, stroke: "#f59e0b" },
            ]}
            formatter={(v) => `${v.toFixed(1)}%`}
            axisFormatter={(v) => `${v.toFixed(0)}%`}
          />
          <UplotPanel
            title="Network throughput"
            series={[
              { label: "RX", points: history.networkNodeRxBps, stroke: "#22c55e" },
              { label: "TX", points: history.networkNodeTxBps, stroke: "#60a5fa" },
            ]}
            formatter={(v) => formatRateBps(v)}
            axisFormatter={(v) => formatBytes(v)}
          />
          <UplotPanel
            title="WS newHeads"
            series={[
              { label: "active", points: history.wsActiveNewHeads, stroke: "#eab308" },
              { label: "msg rate", points: history.wsMessagesReceivedRate, stroke: "#34d399" },
            ]}
            formatter={(v) => `${v.toFixed(2)}`}
            axisFormatter={(v) => `${v.toFixed(0)}`}
          />

          <UplotPanel
            title="Process open FDs"
            series={[{ label: "fds", points: history.processOpenFds, stroke: "#f472b6" }]}
            formatter={(v) => `${v.toFixed(0)}`}
            axisFormatter={(v) => `${v.toFixed(0)}`}
          />
          <UplotPanel
            title="Go goroutines"
            series={[{ label: "goroutines", points: history.goGoroutines, stroke: "#38bdf8" }]}
            formatter={(v) => `${v.toFixed(0)}`}
            axisFormatter={(v) => `${v.toFixed(0)}`}
          />
          <Card className="rounded-3xl border-zinc-800 bg-zinc-950/70 shadow-sm">
            <CardHeader className="px-5 pt-5 pb-3">
              <CardTitle className="text-base">Resource snapshot</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4 px-5 pb-5 pt-0">
              <MiniBar title="CPU total" value={summary?.cpuPct ?? 0} />
              <MiniBar title="Memory total" value={summary?.memoryPct ?? 0} />
              <div className="grid grid-cols-2 gap-3 pt-2 text-sm">
                <div className="rounded-2xl border border-zinc-800 bg-zinc-900/50 p-3">
                  <div className="text-zinc-500">Accepted tx rate</div>
                  <div className="mt-1 text-lg font-semibold text-white">
                    {hasSeries(history.acceptedTxRate) ? "active" : "—"}
                  </div>
                </div>
                <div className="rounded-2xl border border-zinc-800 bg-zinc-900/50 p-3">
                  <div className="text-zinc-500">Disk</div>
                  <div className="mt-1 text-lg font-semibold text-white">
                    <HardDrive className="inline h-4 w-4" /> healthy
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="mt-6 grid gap-4 xl:grid-cols-[1fr_340px]">
          <Card className="rounded-3xl border-zinc-800 bg-zinc-950/70 shadow-sm">
            <CardHeader className="flex flex-col gap-4 px-5 pt-5 pb-4 md:flex-row md:items-center md:justify-between">
              <CardTitle className="text-base">Validators</CardTitle>
              <Input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search by moniker, address, or status"
                className="w-full rounded-2xl border-zinc-700 bg-zinc-900 md:w-80"
              />
            </CardHeader>
            <CardContent className="px-5 pb-5 pt-0">
              {loading && !payload ? (
                <div className="rounded-2xl border border-zinc-800 bg-zinc-950/70 p-8 text-center text-sm text-zinc-500">
                  Loading dashboard data…
                </div>
              ) : filteredValidators.length === 0 ? (
                <div className="rounded-2xl border border-zinc-800 bg-zinc-950/70 p-8 text-center text-sm text-zinc-500">
                  No validators matched your search.
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="min-w-full text-sm">
                    <thead>
                      <tr className="border-b border-zinc-800 text-left text-zinc-400">
                        <th className="px-3 py-3 font-medium">Validator</th>
                        <th className="px-3 py-3 font-medium">Status</th>
                        <th className="px-3 py-3 font-medium">Uptime</th>
                        <th className="px-3 py-3 font-medium">Behind</th>
                        <th className="px-3 py-3 font-medium">Latency</th>
                        <th className="px-3 py-3 font-medium">TPS</th>
                        <th className="px-3 py-3 font-medium">Last seen</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredValidators.map((row) => (
                        <tr key={row.id} className="border-b border-zinc-900/80 text-zinc-200">
                          <td className="px-3 py-4">
                            <div className="font-medium text-white">{row.moniker}</div>
                            <div className="mt-1 text-xs text-zinc-500">{row.address}</div>
                          </td>
                          <td className="px-3 py-4">
                            <Badge className={statusTone(row.status)}>{row.status}</Badge>
                          </td>
                          <td className="px-3 py-4">{row.uptimePct.toFixed(2)}%</td>
                          <td className="px-3 py-4">{row.blocksBehind}</td>
                          <td className="px-3 py-4">{row.latencyMs} ms</td>
                          <td className="px-3 py-4">{row.throughputTps.toFixed(2)}</td>
                          <td className="px-3 py-4 text-zinc-500">{formatDateTime(row.lastSeen)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>

          <PackageVersionsPanel rows={packageVersions} />
        </div>

        <div className="mt-6 grid gap-4 md:grid-cols-3">
          <Card className="rounded-3xl border-zinc-800 bg-zinc-950/70 shadow-sm">
            <CardContent className="flex items-start gap-3 p-5">
              <CheckCircle2 className="mt-0.5 h-5 w-5 text-zinc-300" />
              <div>
                <div className="font-medium">Healthy node</div>
                <p className="mt-1 text-sm text-zinc-500">
                  Green when health is up, sync is stable, and blocks behind stays within threshold.
                </p>
              </div>
            </CardContent>
          </Card>

          <Card className="rounded-3xl border-zinc-800 bg-zinc-950/70 shadow-sm">
            <CardContent className="flex items-start gap-3 p-5">
              <AlertTriangle className="mt-0.5 h-5 w-5 text-zinc-300" />
              <div>
                <div className="font-medium">Warning</div>
                <p className="mt-1 text-sm text-zinc-500">
                  Use warning for elevated p95 latency, growing lag, or quarantine risk before a hard failure.
                </p>
              </div>
            </CardContent>
          </Card>

          <Card className="rounded-3xl border-zinc-800 bg-zinc-950/70 shadow-sm">
            <CardContent className="flex items-start gap-3 p-5">
              <ShieldAlert className="mt-0.5 h-5 w-5 text-zinc-300" />
              <div>
                <div className="font-medium">Critical</div>
                <p className="mt-1 text-sm text-zinc-500">
                  Trigger critical when banned, down, unsynced for prolonged time, or telemetry is absent.
                </p>
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="mt-8">
          <Card className="rounded-3xl border-zinc-800 bg-zinc-950/70 shadow-sm">
            <CardHeader className="px-5 pt-5">
              <CardTitle className="text-base">Custom graphs</CardTitle>
              <p className="mt-1 text-sm text-zinc-500">
                Create additional graphs from any historical metric. New graphs are appended after all built-in charts.
              </p>
            </CardHeader>
            <CardContent className="space-y-5 px-5 pb-5">
              <div className="grid gap-3 md:grid-cols-[1fr_auto]">
                <Input
                  value={graphTitle}
                  onChange={(e) => setGraphTitle(e.target.value)}
                  placeholder="Custom graph title"
                  className="w-full rounded-2xl border-zinc-700 bg-zinc-900"
                />
                <Button
                  onClick={() => void createCustomGraph()}
                  disabled={!graphTitle.trim() || selectedMetricKeys.length === 0 || graphsSaving}
                  className="rounded-2xl"
                >
                  {graphsSaving ? "Saving..." : "Add graph"}
                </Button>
              </div>

              <div className="flex flex-wrap gap-2 text-xs text-zinc-500">
                <span>{selectedMetricKeys.length} metric(s) selected</span>
                <span>•</span>
                <span>{savedGraphs.length} saved graph(s)</span>
                {graphsLoading ? (
                  <>
                    <span>•</span>
                    <span>loading metric catalog</span>
                  </>
                ) : null}
              </div>

              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                {graphMetrics.map((metric) => {
                  const selected = selectedMetricKeys.includes(metric.key);
                  return (
                    <button
                      key={metric.key}
                      type="button"
                      onClick={() => toggleDraftMetric(metric.key)}
                      className={`rounded-2xl border p-4 text-left transition ${
                        selected
                          ? "border-zinc-300 bg-zinc-800 text-white"
                          : "border-zinc-800 bg-zinc-900/60 text-zinc-300"
                      }`}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="font-medium text-white">{metric.title}</div>
                          <div className="mt-1 text-xs text-zinc-500">{metric.group} • {metric.unit}</div>
                        </div>
                        <div className={`rounded-full px-2 py-1 text-[10px] ${selected ? "bg-zinc-100 text-zinc-900" : "bg-zinc-800 text-zinc-400"}`}>
                          {selected ? "selected" : "pick"}
                        </div>
                      </div>
                      <p className="mt-3 text-sm text-zinc-400">{metric.description}</p>
                    </button>
                  );
                })}
              </div>
            </CardContent>
          </Card>

          {customGraphPanels.length > 0 ? (
            <div className="mt-6 grid gap-4 xl:grid-cols-3">
              {customGraphPanels.map((graph) => (
                <div key={graph.id} className="space-y-2">
                  <div className="flex items-center justify-between gap-3 px-1">
                    <div>
                      <div className="text-sm font-medium text-white">{graph.title}</div>
                      <div className="text-xs text-zinc-500">
                        {graph.metrics.map((metric) => metric.title).join(" • ")}
                      </div>
                    </div>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => void deleteCustomGraph(graph.id)}
                      disabled={graphsSaving}
                      className="rounded-2xl"
                    >
                      Delete
                    </Button>
                  </div>
                  <UplotPanel
                    title={graph.title}
                    series={graph.panelSeries}
                    axisFormatter={graph.axisFormatter}
                  />
                </div>
              ))}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
