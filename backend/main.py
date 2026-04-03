from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from prometheus_client.parser import text_string_to_metric_families


TimeRange = Literal["1h", "24h", "7d"]
ValidatorStatus = Literal["active", "banned", "quarantined", "syncing", "down"]
HEX_40_RE = re.compile(r"0x[a-fA-F0-9]{40}")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "GenLayer Dashboard API"
    allowed_origins: str = "*"

    ops_base_url: str = "http://127.0.0.1:9153"
    latency_target_url: str = "http://127.0.0.1:9153/health"
    latency_http_method: str = "GET"

    validator_address: str | None = None
    validator_moniker: str = "POSTHUMAN"
    genlayer_cli_bin: str = "genlayer"

    prometheus_url: str | None = None
    prometheus_username: str | None = None
    prometheus_password: str | None = None
    prometheus_bearer_token: str | None = None
    prometheus_timeout_seconds: float = 8.0
    prom_node_label: str | None = None

    promql_latency_p50: str = ""
    promql_latency_p95: str = ""

    history_db_path: str = "./history/history.db"
    graph_config_path: str = "./graphs/dashboard_graphs.json"

    blocks_behind_warning: int = 10
    dashboard_cache_seconds: int = 5

    @property
    def cors_origins(self) -> list[str]:
        if self.allowed_origins.strip() == "*":
            return ["*"]
        return [x.strip() for x in self.allowed_origins.split(",") if x.strip()]


settings = Settings()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger(__name__)


class SeriesPoint(BaseModel):
    ts: str
    value: float


class PackageVersion(BaseModel):
    name: str
    version: str
    source: str
    updatedAt: str


class RpcMethodSeries(BaseModel):
    method: str
    requestRate: list[SeriesPoint]
    avgLatencyMs: list[SeriesPoint]
    requestBytesRate: list[SeriesPoint]
    responseBytesRate: list[SeriesPoint]


class WsDisconnectionSeries(BaseModel):
    reason: str
    values: list[SeriesPoint]


class ValidatorRow(BaseModel):
    id: str
    moniker: str
    address: str
    status: ValidatorStatus
    uptimePct: float
    blocksBehind: int
    latencyMs: int
    throughputTps: float
    lastSeen: str


class Summary(BaseModel):
    network: str
    validatorStatus: ValidatorStatus
    overallHealth: Literal["up", "down"]
    protocolVersion: str
    nodeVersion: str
    synced: bool
    latestBlock: int
    syncedBlock: int
    blocksBehind: int
    uptimeSeconds: int
    uptimePct24h: float
    txThroughputTps: float
    latencyMsP50: int
    latencyMsP95: int
    activeValidators: int
    bannedValidators: int
    quarantinedValidators: int
    cpuPct: float
    memoryPct: float
    dataSource: Literal["live", "sqlite"] = "live"
    isStale: bool = False
    snapshotRecordedAt: str | None = None


class History(BaseModel):
    throughput: list[SeriesPoint]
    latencyP50: list[SeriesPoint]
    latencyP95: list[SeriesPoint]
    blocksBehind: list[SeriesPoint]
    uptimePct: list[SeriesPoint]
    commitRate: list[SeriesPoint]
    revealRate: list[SeriesPoint]

    latestBlock: list[SeriesPoint]
    syncedBlock: list[SeriesPoint]
    acceptedTxRate: list[SeriesPoint]

    cpuNodePct: list[SeriesPoint]
    cpuWebdriverPct: list[SeriesPoint]
    cpuGenvmLlmPct: list[SeriesPoint]
    cpuGenvmWebPct: list[SeriesPoint]

    memoryNodePct: list[SeriesPoint]
    memoryWebdriverPct: list[SeriesPoint]
    memoryNodeRssBytes: list[SeriesPoint]

    diskDbUsagePct: list[SeriesPoint]
    diskLogsUsagePct: list[SeriesPoint]

    networkNodeRxBps: list[SeriesPoint]
    networkNodeTxBps: list[SeriesPoint]

    wsActiveNewHeads: list[SeriesPoint]
    wsMessagesReceivedRate: list[SeriesPoint]

    processOpenFds: list[SeriesPoint]
    goGoroutines: list[SeriesPoint]


class DashboardPayload(BaseModel):
    summary: Summary
    history: History
    packageVersions: list[PackageVersion]
    rpcMethods: list[RpcMethodSeries]
    wsDisconnections: list[WsDisconnectionSeries]
    validators: list[ValidatorRow]


class GraphMetricDefinition(BaseModel):
    key: str
    title: str
    group: str
    description: str
    unit: str


class GraphConfigItem(BaseModel):
    id: str
    title: str
    metricKeys: list[str]


class GraphConfigPayload(BaseModel):
    metrics: list[GraphMetricDefinition]
    graphs: list[GraphConfigItem]


class GraphConfigUpdateRequest(BaseModel):
    graphs: list[GraphConfigItem]


@dataclass(slots=True)
class RangeSpec:
    key: TimeRange
    delta: timedelta
    step_seconds: int
    points: int


RANGE_SPECS: dict[TimeRange, RangeSpec] = {
    "1h": RangeSpec(key="1h", delta=timedelta(hours=1), step_seconds=60, points=60),
    "24h": RangeSpec(key="24h", delta=timedelta(hours=24), step_seconds=15 * 60, points=96),
    "7d": RangeSpec(key="7d", delta=timedelta(days=7), step_seconds=2 * 60 * 60, points=84),
}


METRIC_CATALOG: dict[str, GraphMetricDefinition] = {
    "throughput": GraphMetricDefinition(key="throughput", title="Transaction throughput", group="Transactions", description="Historical transaction throughput across the node.", unit="tps"),
    "acceptedTxRate": GraphMetricDefinition(key="acceptedTxRate", title="Accepted tx rate", group="Transactions", description="Accepted transaction rate derived from historical snapshots.", unit="tps"),
    "commitRate": GraphMetricDefinition(key="commitRate", title="Commit rate", group="Transactions", description="Commit events rate over time.", unit="rate"),
    "revealRate": GraphMetricDefinition(key="revealRate", title="Reveal rate", group="Transactions", description="Reveal events rate over time.", unit="rate"),
    "latencyP50": GraphMetricDefinition(key="latencyP50", title="Latency p50", group="Latency", description="Median request latency in milliseconds.", unit="ms"),
    "latencyP95": GraphMetricDefinition(key="latencyP95", title="Latency p95", group="Latency", description="Tail request latency in milliseconds.", unit="ms"),
    "blocksBehind": GraphMetricDefinition(key="blocksBehind", title="Blocks behind", group="Sync", description="How many blocks the node lags behind the latest known block.", unit="blocks"),
    "latestBlock": GraphMetricDefinition(key="latestBlock", title="Latest block", group="Sync", description="Latest known block height reported by the network.", unit="count"),
    "syncedBlock": GraphMetricDefinition(key="syncedBlock", title="Synced block", group="Sync", description="Block height fully synced by the local node.", unit="count"),
    "uptimePct": GraphMetricDefinition(key="uptimePct", title="Historical uptime", group="Health", description="Node uptime percentage over time.", unit="percent"),
    "cpuNodePct": GraphMetricDefinition(key="cpuNodePct", title="CPU node", group="CPU", description="CPU usage percent for the main node process.", unit="percent"),
    "cpuWebdriverPct": GraphMetricDefinition(key="cpuWebdriverPct", title="CPU webdriver", group="CPU", description="CPU usage percent for the webdriver sidecar.", unit="percent"),
    "cpuGenvmLlmPct": GraphMetricDefinition(key="cpuGenvmLlmPct", title="CPU genvm-llm", group="CPU", description="CPU usage percent for the genvm llm worker.", unit="percent"),
    "cpuGenvmWebPct": GraphMetricDefinition(key="cpuGenvmWebPct", title="CPU genvm-web", group="CPU", description="CPU usage percent for the genvm web worker.", unit="percent"),
    "memoryNodePct": GraphMetricDefinition(key="memoryNodePct", title="Memory node", group="Memory", description="Memory usage percent for the main node process.", unit="percent"),
    "memoryWebdriverPct": GraphMetricDefinition(key="memoryWebdriverPct", title="Memory webdriver", group="Memory", description="Memory usage percent for the webdriver sidecar.", unit="percent"),
    "memoryNodeRssBytes": GraphMetricDefinition(key="memoryNodeRssBytes", title="Node RSS memory", group="Memory", description="Resident memory used by the main node process.", unit="bytes"),
    "diskDbUsagePct": GraphMetricDefinition(key="diskDbUsagePct", title="Disk DB usage", group="Disk", description="Disk utilization percent for the database storage path.", unit="percent"),
    "diskLogsUsagePct": GraphMetricDefinition(key="diskLogsUsagePct", title="Disk logs usage", group="Disk", description="Disk utilization percent for the logs storage path.", unit="percent"),
    "networkNodeRxBps": GraphMetricDefinition(key="networkNodeRxBps", title="Network RX", group="Network", description="Inbound network throughput for the node.", unit="bytes_per_sec"),
    "networkNodeTxBps": GraphMetricDefinition(key="networkNodeTxBps", title="Network TX", group="Network", description="Outbound network throughput for the node.", unit="bytes_per_sec"),
    "wsActiveNewHeads": GraphMetricDefinition(key="wsActiveNewHeads", title="WS active newHeads", group="WebSocket", description="Active WebSocket newHeads subscriptions.", unit="count"),
    "wsMessagesReceivedRate": GraphMetricDefinition(key="wsMessagesReceivedRate", title="WS message rate", group="WebSocket", description="Rate of messages received over WebSocket subscriptions.", unit="rate"),
    "processOpenFds": GraphMetricDefinition(key="processOpenFds", title="Open file descriptors", group="Process", description="Open file descriptors for the node process.", unit="count"),
    "goGoroutines": GraphMetricDefinition(key="goGoroutines", title="Go goroutines", group="Process", description="Number of active goroutines in the Go runtime.", unit="count"),
}


class TTLCache:
    def __init__(self) -> None:
        self._data: dict[str, tuple[float, Any]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        async with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            expires_at, value = item
            if expires_at < time.time():
                self._data.pop(key, None)
                return None
            return value

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        async with self._lock:
            self._data[key] = (time.time() + ttl_seconds, value)


cache = TTLCache()


class PrometheusClient:
    def __init__(
        self,
        base_url: str,
        timeout: float,
        username: str | None,
        password: str | None,
        bearer: str | None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.username = username
        self.password = password
        self.bearer = bearer
        self.client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        headers: dict[str, str] = {}
        auth: tuple[str, str] | None = None
        if self.bearer:
            headers["Authorization"] = f"Bearer {self.bearer}"
        elif self.username and self.password:
            auth = (self.username, self.password)
        self.client = httpx.AsyncClient(timeout=self.timeout, headers=headers, auth=auth)

    async def stop(self) -> None:
        if self.client:
            await self.client.aclose()

    async def query(self, query: str) -> list[dict[str, Any]]:
        if not self.client:
            raise RuntimeError("Prometheus client not started")
        resp = await self.client.get(f"{self.base_url}/api/v1/query", params={"query": query})
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus query failed: {payload}")
        return payload.get("data", {}).get("result", [])

    async def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[dict[str, Any]]:
        if not self.client:
            raise RuntimeError("Prometheus client not started")
        params = {
            "query": query,
            "start": start.timestamp(),
            "end": end.timestamp(),
            "step": step_seconds,
        }
        resp = await self.client.get(f"{self.base_url}/api/v1/query_range", params=params)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus range query failed: {payload}")
        return payload.get("data", {}).get("result", [])


class OpsMetricsClient:
    def __init__(self, ops_base_url: str) -> None:
        self.ops_base_url = ops_base_url.rstrip("/")
        self.client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self.client = httpx.AsyncClient(timeout=5.0)

    async def stop(self) -> None:
        if self.client:
            await self.client.aclose()

    async def health(self) -> dict[str, Any]:
        if not self.client:
            raise RuntimeError("Ops client not started")
        resp = await self.client.get(f"{self.ops_base_url}/health")
        resp.raise_for_status()
        return resp.json()

    async def scrape_metrics(self) -> str:
        if not self.client:
            raise RuntimeError("Ops client not started")
        resp = await self.client.get(f"{self.ops_base_url}/metrics")
        resp.raise_for_status()
        return resp.text


prom_client: PrometheusClient | None = None
ops_client = OpsMetricsClient(settings.ops_base_url)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global prom_client
    await ops_client.start()
    if settings.prometheus_url:
        prom_client = PrometheusClient(
            base_url=settings.prometheus_url,
            timeout=settings.prometheus_timeout_seconds,
            username=settings.prometheus_username,
            password=settings.prometheus_password,
            bearer=settings.prometheus_bearer_token,
        )
        await prom_client.start()
    try:
        yield
    finally:
        await ops_client.stop()
        if prom_client:
            await prom_client.stop()


app = FastAPI(title=settings.app_name, version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def build_matchers(extra: dict[str, str] | None = None) -> str:
    labels: dict[str, str] = {}
    if settings.prom_node_label:
        labels["node"] = settings.prom_node_label
    if extra:
        labels.update(extra)
    if not labels:
        return ""
    joined = ",".join(f'{k}="{v}"' for k, v in labels.items())
    return "{" + joined + "}"


def prom_expr_for_metric(metric: str, extra: dict[str, str] | None = None, reduce: str = "max") -> str:
    selector = build_matchers(extra)
    return f"{reduce}({metric}{selector})"


def window_seconds(window: str) -> int:
    match = re.fullmatch(r"(\d+)([smhd])", window)
    if not match:
        raise ValueError(f"Unsupported window: {window}")
    value = int(match.group(1))
    unit = match.group(2)
    factor = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return value * factor


def prom_expr_delta_rate(metric: str, window: str = "5m", extra: dict[str, str] | None = None) -> str:
    selector = build_matchers(extra)
    return f"clamp_min(delta({metric}{selector}[{window}]), 0) / {window_seconds(window)}"


def empty_series(range_spec: RangeSpec, value: float = 0.0) -> list[SeriesPoint]:
    end = now_utc()
    start = end - range_spec.delta
    out: list[SeriesPoint] = []
    current = start
    while current <= end:
        out.append(SeriesPoint(ts=iso_z(current), value=round(value, 4)))
        current += timedelta(seconds=range_spec.step_seconds)
    return out[-range_spec.points:]


def flatten_json_search(obj: Any, wanted_keys: set[str]) -> dict[str, Any]:
    found: dict[str, Any] = {}

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                key = k.lower().replace("-", "_")
                if key in wanted_keys and key not in found:
                    found[key] = v
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(obj)
    return found


def parse_health_versions(health: dict[str, Any]) -> tuple[str, str, str, str]:
    found = flatten_json_search(
        health,
        {"status", "version", "node_version", "protocol_version", "consensus_version", "network", "network_name"},
    )
    overall = str(found.get("status", "up")).lower()
    overall_health = "up" if overall in {"ok", "up", "healthy", "pass", "ready"} else "down"
    node_version = str(found.get("node_version") or found.get("version") or "unknown")
    protocol_version = str(found.get("protocol_version") or found.get("consensus_version") or "unknown")
    network = str(found.get("network") or found.get("network_name") or os.getenv("NETWORK_NAME", "genlayer"))
    return overall_health, node_version, protocol_version, network


def parse_validator_flags_from_health(health: dict[str, Any]) -> tuple[bool, bool]:
    found = flatten_json_search(
        health,
        {"banned", "permanent_ban", "permanent", "quarantined", "validator_quarantined"},
    )
    banned = bool(found.get("banned") or found.get("permanent_ban") or found.get("permanent"))
    quarantined = bool(found.get("quarantined") or found.get("validator_quarantined"))
    return banned, quarantined


def infer_local_status(
    overall_health: str,
    banned: bool,
    quarantined: bool,
    synced: bool,
    blocks_behind: int,
) -> ValidatorStatus:
    if overall_health == "down":
        return "down"
    if banned:
        return "banned"
    if quarantined:
        return "quarantined"
    if (not synced) or blocks_behind >= settings.blocks_behind_warning:
        return "syncing"
    return "active"


def sample_value_from_prometheus(result: list[dict[str, Any]]) -> float:
    if not result:
        return 0.0
    first = result[0]
    value = first.get("value", [None, 0])
    if len(value) < 2:
        return 0.0
    return safe_float(value[1])


def series_from_prometheus_matrix(result: list[dict[str, Any]]) -> list[SeriesPoint]:
    if not result:
        return []
    first = result[0]
    values = first.get("values", [])
    out: list[SeriesPoint] = []
    for ts, val in values:
        try:
            dt = datetime.fromtimestamp(float(ts), tz=UTC)
            out.append(SeriesPoint(ts=iso_z(dt), value=round(safe_float(val), 4)))
        except Exception:
            continue
    return out


def mean_value(points: list[SeriesPoint]) -> float:
    if not points:
        return 0.0
    return sum(p.value for p in points) / len(points)


def tail_value(points: list[SeriesPoint], default: float = 0.0) -> float:
    return points[-1].value if points else default


def choose_metric_value(
    samples: list[tuple[str, dict[str, str], float]],
    metric_name: str,
    target_node: str | None,
    required_labels: dict[str, str] | None = None,
) -> float:
    def collect(require_target_node: bool) -> list[float]:
        candidates: list[float] = []
        for name, labels, value in samples:
            if name != metric_name:
                continue
            if required_labels:
                ok = True
                for k, v in required_labels.items():
                    if labels.get(k) != v:
                        ok = False
                        break
                if not ok:
                    continue
            if require_target_node and target_node and labels.get("node", "").lower() != target_node:
                continue
            candidates.append(value)
        return candidates

    candidates = collect(require_target_node=True)
    if not candidates and target_node:
        candidates = collect(require_target_node=False)
    return max(candidates) if candidates else 0.0


def current_package_versions_from_snapshot(snapshot: dict[str, Any]) -> list[PackageVersion]:
    updated_at = iso_z(now_utc())
    rows = [
        ("genlayer-node", str(snapshot.get("metric_node_version") or "")),
        ("genlayer-protocol", str(snapshot.get("metric_protocol_version") or "")),
        ("genvm", str(snapshot.get("metric_genvm_version") or "")),
        ("genlayer-commit", str(snapshot.get("metric_commit") or "")),
        ("go-runtime", str(snapshot.get("metric_go_version") or "")),
    ]
    out: list[PackageVersion] = []
    for name, version in rows:
        if version:
            out.append(PackageVersion(name=name, version=version, source="live_metrics", updatedAt=updated_at))
    return out


def sqlite_exists() -> bool:
    return Path(settings.history_db_path).exists()


def sqlite_rows(query: str, args: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    if not sqlite_exists():
        return []
    conn = sqlite3.connect(settings.history_db_path)
    conn.row_factory = sqlite3.Row
    try:
        return list(conn.execute(query, args).fetchall())
    finally:
        conn.close()


def sqlite_latest_snapshot_row(preferred_network: str | None = None) -> sqlite3.Row | None:
    rows: list[sqlite3.Row] = []
    if preferred_network:
        rows = sqlite_rows(
            """
            SELECT *
            FROM metric_snapshots
            WHERE network = ?
            ORDER BY recorded_at DESC
            LIMIT 1
            """,
            (preferred_network,),
        )
        if rows:
            return rows[0]

    rows = sqlite_rows(
        """
        SELECT *
        FROM metric_snapshots
        ORDER BY recorded_at DESC
        LIMIT 1
        """
    )
    return rows[0] if rows else None


def snapshot_from_sqlite_row(row: sqlite3.Row) -> dict[str, Any]:
    package_map = {pkg.name: pkg.version for pkg in sqlite_packages()}
    return {
        "blocks_behind": safe_float(row["blocks_behind"]),
        "latest_block": safe_float(row["latest_block"]),
        "synced_block": safe_float(row["synced_block"]),
        "synced": safe_float(row["synced"]),
        "processing_block": safe_float(row["processing_block"]),
        "uptime_seconds": safe_float(row["uptime_seconds"]),
        "accepted_total": safe_float(row["accepted_total"]),
        "activated_total": safe_float(row["activated_total"]),
        "leader_proposed_total": safe_float(row["leader_proposed_total"]),
        "leader_revealed_total": safe_float(row["leader_revealed_total"]),
        "commit_total": safe_float(row["validator_commit_total"]),
        "reveal_total": safe_float(row["validator_reveal_total"]),
        "cpu_pct": safe_float(row["cpu_node_pct"]),
        "memory_pct": safe_float(row["memory_node_pct"]),
        "cpu_node_pct": safe_float(row["cpu_node_pct"]),
        "cpu_webdriver_pct": safe_float(row["cpu_webdriver_pct"]),
        "cpu_genvm_llm_pct": safe_float(row["cpu_genvm_llm_pct"]),
        "cpu_genvm_web_pct": safe_float(row["cpu_genvm_web_pct"]),
        "memory_node_pct": safe_float(row["memory_node_pct"]),
        "memory_webdriver_pct": safe_float(row["memory_webdriver_pct"]),
        "memory_node_rss_bytes": safe_float(row["memory_node_rss_bytes"]),
        "memory_node_vms_bytes": safe_float(row["memory_node_vms_bytes"]),
        "memory_webdriver_usage_bytes": safe_float(row["memory_webdriver_usage_bytes"]),
        "disk_db_usage_pct": safe_float(row["disk_db_usage_pct"]),
        "disk_logs_usage_pct": safe_float(row["disk_logs_usage_pct"]),
        "network_node_rx_total": safe_float(row["network_node_rx_total"]),
        "network_node_tx_total": safe_float(row["network_node_tx_total"]),
        "network_webdriver_rx_total": safe_float(row["network_webdriver_rx_total"]),
        "network_webdriver_tx_total": safe_float(row["network_webdriver_tx_total"]),
        "rollup_rpc_inflight_requests": safe_float(row["rollup_rpc_inflight_requests"]),
        "ws_active_newheads": safe_float(row["ws_active_newheads"]),
        "ws_messages_received_newheads": safe_float(row["ws_messages_received_newheads"]),
        "go_goroutines": safe_float(row["go_goroutines"]),
        "go_threads": safe_float(row["go_threads"]),
        "process_open_fds": safe_float(row["process_open_fds"]),
        "process_resident_memory_bytes": safe_float(row["process_resident_memory_bytes"]),
        "process_virtual_memory_bytes": safe_float(row["process_virtual_memory_bytes"]),
        "metric_network": str(row["network"]),
        "metric_node_version": package_map.get("genlayer-node", ""),
        "metric_protocol_version": package_map.get("genlayer-protocol", ""),
        "metric_genvm_version": package_map.get("genvm", ""),
        "metric_commit": package_map.get("genlayer-commit", ""),
        "metric_go_version": package_map.get("go-runtime", ""),
        "recorded_at": str(row["recorded_at"]),
    }


def sqlite_has_metric_history(network: str, range_spec: RangeSpec) -> bool:
    start = iso_z(now_utc() - range_spec.delta)
    rows = sqlite_rows(
        """
        SELECT COUNT(*) AS cnt
        FROM metric_snapshots
        WHERE network = ? AND recorded_at >= ?
        """,
        (network, start),
    )
    return bool(rows and safe_int(rows[0]["cnt"]) > 1)


def sqlite_metric_series(network: str, range_spec: RangeSpec, column: str, scale: float = 1.0) -> list[SeriesPoint]:
    start = iso_z(now_utc() - range_spec.delta)
    rows = sqlite_rows(
        f"""
        SELECT recorded_at, {column} AS value
        FROM metric_snapshots
        WHERE network = ? AND recorded_at >= ?
        ORDER BY recorded_at ASC
        """,
        (network, start),
    )
    return [
        SeriesPoint(ts=str(r["recorded_at"]), value=round(safe_float(r["value"]) * scale, 4))
        for r in rows
    ]


def sqlite_counter_rate_series(network: str, range_spec: RangeSpec, column: str) -> list[SeriesPoint]:
    start = iso_z(now_utc() - range_spec.delta)
    rows = sqlite_rows(
        f"""
        SELECT recorded_at, {column} AS value
        FROM metric_snapshots
        WHERE network = ? AND recorded_at >= ?
        ORDER BY recorded_at ASC
        """,
        (network, start),
    )
    if len(rows) < 2:
        return []

    out: list[SeriesPoint] = []
    prev_ts: str | None = None
    prev_val: float | None = None

    for row in rows:
        ts = str(row["recorded_at"])
        val = safe_float(row["value"])
        if prev_ts is not None and prev_val is not None:
            t1 = datetime.fromisoformat(prev_ts.replace("Z", "+00:00"))
            t2 = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            seconds = max((t2 - t1).total_seconds(), 1.0)
            rate = max((val - prev_val) / seconds, 0.0)
            out.append(SeriesPoint(ts=ts, value=round(rate, 4)))
        prev_ts = ts
        prev_val = val

    return out


def sqlite_counter_delta_series_by_reason(
    table: str,
    network: str,
    range_spec: RangeSpec,
) -> list[WsDisconnectionSeries]:
    start = iso_z(now_utc() - range_spec.delta)
    rows = sqlite_rows(
        f"""
        SELECT recorded_at, reason, disconnections_total
        FROM {table}
        WHERE network = ? AND recorded_at >= ?
        ORDER BY reason ASC, recorded_at ASC
        """,
        (network, start),
    )
    if not rows:
        return []

    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["reason"]), []).append(row)

    out: list[WsDisconnectionSeries] = []
    for reason, reason_rows in grouped.items():
        values: list[SeriesPoint] = []
        prev_val: float | None = None
        for row in reason_rows:
            ts = str(row["recorded_at"])
            val = safe_float(row["disconnections_total"])
            if prev_val is not None:
                delta = max(val - prev_val, 0.0)
                values.append(SeriesPoint(ts=ts, value=round(delta, 4)))
            prev_val = val
        out.append(WsDisconnectionSeries(reason=reason, values=values))
    return out


def sqlite_packages() -> list[PackageVersion]:
    rows = sqlite_rows(
        """
        SELECT name, version, source, updated_at
        FROM package_versions
        ORDER BY name ASC
        """
    )
    return [
        PackageVersion(
            name=str(r["name"]),
            version=str(r["version"]),
            source=str(r["source"]),
            updatedAt=str(r["updated_at"]),
        )
        for r in rows
    ]


def sqlite_rpc_methods(network: str, range_spec: RangeSpec) -> list[RpcMethodSeries]:
    start = iso_z(now_utc() - range_spec.delta)
    rows = sqlite_rows(
        """
        SELECT recorded_at, method, requests_total, request_bytes_total, response_bytes_total,
               duration_sum_seconds, duration_count
        FROM rpc_method_snapshots
        WHERE network = ? AND recorded_at >= ?
        ORDER BY method ASC, recorded_at ASC
        """,
        (network, start),
    )
    if not rows:
        return []

    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["method"]), []).append(row)

    priority = {"eth_call": 0, "eth_getLogs": 1, "eth_blockNumber": 2, "batch": 3}

    def counter_rate_series(rows_for_method: list[sqlite3.Row], key: str) -> list[SeriesPoint]:
        out: list[SeriesPoint] = []
        prev_ts: str | None = None
        prev_val: float | None = None
        for row in rows_for_method:
            ts = str(row["recorded_at"])
            val = safe_float(row[key])
            if prev_ts is not None and prev_val is not None:
                t1 = datetime.fromisoformat(prev_ts.replace("Z", "+00:00"))
                t2 = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                seconds = max((t2 - t1).total_seconds(), 1.0)
                rate = max((val - prev_val) / seconds, 0.0)
                out.append(SeriesPoint(ts=ts, value=round(rate, 4)))
            prev_ts = ts
            prev_val = val
        return out

    def avg_latency_series(rows_for_method: list[sqlite3.Row]) -> list[SeriesPoint]:
        out: list[SeriesPoint] = []
        prev_sum: float | None = None
        prev_count: float | None = None
        for row in rows_for_method:
            ts = str(row["recorded_at"])
            cur_sum = safe_float(row["duration_sum_seconds"])
            cur_count = safe_float(row["duration_count"])
            if prev_sum is not None and prev_count is not None:
                ds = cur_sum - prev_sum
                dc = cur_count - prev_count
                value = (ds / dc) * 1000.0 if dc > 0 else 0.0
                out.append(SeriesPoint(ts=ts, value=round(max(value, 0.0), 4)))
            prev_sum = cur_sum
            prev_count = cur_count
        return out

    items: list[RpcMethodSeries] = []
    for method, method_rows in grouped.items():
        items.append(
            RpcMethodSeries(
                method=method,
                requestRate=counter_rate_series(method_rows, "requests_total"),
                avgLatencyMs=avg_latency_series(method_rows),
                requestBytesRate=counter_rate_series(method_rows, "request_bytes_total"),
                responseBytesRate=counter_rate_series(method_rows, "response_bytes_total"),
            )
        )

    items.sort(key=lambda x: (priority.get(x.method, 999), x.method))
    return items


def sqlite_ws_disconnections(network: str, range_spec: RangeSpec) -> list[WsDisconnectionSeries]:
    return sqlite_counter_delta_series_by_reason("ws_disconnection_snapshots", network, range_spec)


async def fetch_instant_prom(query: str) -> float:
    if not prom_client or not settings.prometheus_url:
        return 0.0
    result = await prom_client.query(query)
    return sample_value_from_prometheus(result)


async def fetch_range_prom(query: str, range_spec: RangeSpec) -> list[SeriesPoint]:
    if not prom_client or not settings.prometheus_url:
        return []
    end = now_utc()
    start = end - range_spec.delta
    result = await prom_client.query_range(query, start, end, range_spec.step_seconds)
    return series_from_prometheus_matrix(result)


async def probe_latency_ms() -> float:
    method = settings.latency_http_method.upper().strip()
    start = time.perf_counter()
    async with httpx.AsyncClient(timeout=5.0) as client:
        if method == "POST":
            resp = await client.post(settings.latency_target_url)
        else:
            resp = await client.get(settings.latency_target_url)
        resp.raise_for_status()
    return (time.perf_counter() - start) * 1000.0


async def run_cli(*args: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    out = stdout.decode("utf-8", errors="replace").strip()
    err = stderr.decode("utf-8", errors="replace").strip()
    if proc.returncode != 0:
        raise RuntimeError(err or out or f"CLI command failed: {' '.join(args)}")
    return out


def parse_active_validators_output(raw: str) -> list[dict[str, str]]:
    try:
        data = json.loads(raw)
        extracted: list[dict[str, str]] = []

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                values = {k.lower(): v for k, v in node.items()}
                address = (
                    values.get("validator")
                    or values.get("address")
                    or values.get("validatorwallet")
                    or values.get("validator_wallet")
                )
                moniker = values.get("moniker") or values.get("name") or values.get("identity") or "Validator"
                if isinstance(address, str) and HEX_40_RE.fullmatch(address):
                    extracted.append({"address": address, "moniker": str(moniker)})
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(data)
        if extracted:
            dedup: dict[str, dict[str, str]] = {x["address"].lower(): x for x in extracted}
            return list(dedup.values())
    except Exception:
        pass

    rows: list[dict[str, str]] = []
    for line in raw.splitlines():
        addresses = HEX_40_RE.findall(line)
        if not addresses:
            continue
        address = addresses[0]
        moniker = line.replace(address, "").strip(" -:\t") or f"Validator {address[:8]}"
        rows.append({"address": address, "moniker": moniker})
    dedup = {x["address"].lower(): x for x in rows}
    return list(dedup.values())


async def load_active_validators() -> list[dict[str, str]]:
    try:
        raw = await run_cli(settings.genlayer_cli_bin, "staking", "active-validators")
        return parse_active_validators_output(raw)
    except Exception as exc:
        logger.warning("failed to load active validators via %s: %s", settings.genlayer_cli_bin, exc)
        return []


async def scrape_snapshot_metrics() -> dict[str, Any]:
    text = await ops_client.scrape_metrics()
    target_node = settings.prom_node_label.lower() if settings.prom_node_label else None
    samples: list[tuple[str, dict[str, str], float]] = []
    info_labels: dict[str, str] = {}
    go_labels: dict[str, str] = {}

    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            labels = {k: str(v) for k, v in sample.labels.items()}
            name = sample.name
            value = safe_float(sample.value)
            samples.append((name, labels, value))
            if name == "genlayer_node_info" and not info_labels:
                info_labels = labels
            if name == "go_info" and not go_labels:
                go_labels = labels

    return {
        "blocks_behind": choose_metric_value(samples, "genlayer_node_blocks_behind", target_node),
        "latest_block": choose_metric_value(samples, "genlayer_node_latest_block", target_node),
        "synced_block": choose_metric_value(samples, "genlayer_node_synced_block", target_node),
        "synced": choose_metric_value(samples, "genlayer_node_synced", target_node),
        "processing_block": choose_metric_value(samples, "genlayer_node_processing_block", target_node),
        "uptime_seconds": choose_metric_value(samples, "genlayer_node_uptime_seconds", target_node, {"component": "node"}),

        "accepted_total": choose_metric_value(samples, "genlayer_node_transactions_accepted_synced_total", target_node),
        "activated_total": choose_metric_value(samples, "genlayer_node_transactions_activated_total", target_node),
        "leader_proposed_total": choose_metric_value(samples, "genlayer_node_transactions_leader_proposed_total", target_node),
        "leader_revealed_total": choose_metric_value(samples, "genlayer_node_transactions_leader_revealed_total", target_node),
        "commit_total": choose_metric_value(samples, "genlayer_node_transactions_validator_commit_total", target_node),
        "reveal_total": choose_metric_value(samples, "genlayer_node_transactions_validator_reveal_total", target_node),

        "cpu_pct": choose_metric_value(samples, "genlayer_node_cpu_usage_percent", target_node, {"component": "node"}),
        "memory_pct": choose_metric_value(samples, "genlayer_node_memory_percent", target_node, {"component": "node"}),

        "cpu_node_pct": choose_metric_value(samples, "genlayer_node_cpu_usage_percent", target_node, {"component": "node"}),
        "cpu_webdriver_pct": choose_metric_value(samples, "genlayer_node_cpu_usage_percent", target_node, {"component": "webdriver"}),
        "cpu_genvm_llm_pct": choose_metric_value(samples, "genlayer_node_cpu_usage_percent", target_node, {"component": "genvm-llm"}),
        "cpu_genvm_web_pct": choose_metric_value(samples, "genlayer_node_cpu_usage_percent", target_node, {"component": "genvm-web"}),

        "memory_node_pct": choose_metric_value(samples, "genlayer_node_memory_percent", target_node, {"component": "node"}),
        "memory_webdriver_pct": choose_metric_value(samples, "genlayer_node_memory_percent", target_node, {"component": "webdriver"}),
        "memory_node_rss_bytes": choose_metric_value(samples, "genlayer_node_memory_rss_bytes", target_node, {"component": "node"}),
        "memory_node_vms_bytes": choose_metric_value(samples, "genlayer_node_memory_vms_bytes", target_node, {"component": "node"}),
        "memory_webdriver_usage_bytes": choose_metric_value(samples, "genlayer_node_memory_usage_bytes", target_node, {"component": "webdriver"}),

        "disk_db_usage_pct": choose_metric_value(samples, "genlayer_node_disk_usage_percent", target_node, {"component": "node", "directory": "genlayer.db"}),
        "disk_logs_usage_pct": choose_metric_value(samples, "genlayer_node_disk_usage_percent", target_node, {"component": "node", "directory": "logs"}),

        "network_node_rx_total": choose_metric_value(samples, "genlayer_node_network_rx_bytes_total", target_node, {"component": "node"}),
        "network_node_tx_total": choose_metric_value(samples, "genlayer_node_network_tx_bytes_total", target_node, {"component": "node"}),
        "network_webdriver_rx_total": choose_metric_value(samples, "genlayer_node_network_rx_bytes_total", target_node, {"component": "webdriver"}),
        "network_webdriver_tx_total": choose_metric_value(samples, "genlayer_node_network_tx_bytes_total", target_node, {"component": "webdriver"}),

        "rollup_rpc_inflight_requests": choose_metric_value(samples, "genlayer_rollup_rpc_inflight_requests", target_node),
        "ws_active_newheads": choose_metric_value(samples, "genlayer_rollup_ws_active_subscriptions", target_node, {"subscription_type": "newHeads"}),
        "ws_messages_received_newheads": choose_metric_value(samples, "genlayer_rollup_ws_messages_received_total", target_node, {"subscription_type": "newHeads"}),

        "go_goroutines": choose_metric_value(samples, "go_goroutines", None),
        "go_threads": choose_metric_value(samples, "go_threads", None),
        "process_open_fds": choose_metric_value(samples, "process_open_fds", None),
        "process_resident_memory_bytes": choose_metric_value(samples, "process_resident_memory_bytes", None),
        "process_virtual_memory_bytes": choose_metric_value(samples, "process_virtual_memory_bytes", None),

        "metric_network": info_labels.get("network", ""),
        "metric_node_version": info_labels.get("version", ""),
        "metric_protocol_version": info_labels.get("protocol_version", ""),
        "metric_genvm_version": info_labels.get("genvm_version", ""),
        "metric_commit": info_labels.get("commit", ""),
        "metric_go_version": go_labels.get("version", ""),
    }


async def load_snapshot_metrics() -> dict[str, Any]:
    return await scrape_snapshot_metrics()


def snapshot_history(range_spec: RangeSpec, latency_now: float, snapshot: dict[str, Any]) -> History:
    current_blocks = safe_float(snapshot.get("blocks_behind", 0.0))
    current_synced = safe_float(snapshot.get("synced", 0.0))
    uptime_pct = 100.0 if current_synced >= 1.0 else 0.0
    current_latest = safe_float(snapshot.get("latest_block", 0.0))
    current_synced_block = safe_float(snapshot.get("synced_block", 0.0))

    accepted = empty_series(range_spec, 0.0)
    return History(
        throughput=accepted,
        latencyP50=empty_series(range_spec, latency_now),
        latencyP95=empty_series(range_spec, latency_now * 1.25),
        blocksBehind=empty_series(range_spec, current_blocks),
        uptimePct=empty_series(range_spec, uptime_pct),
        commitRate=empty_series(range_spec, 0.0),
        revealRate=empty_series(range_spec, 0.0),

        latestBlock=empty_series(range_spec, current_latest),
        syncedBlock=empty_series(range_spec, current_synced_block),
        acceptedTxRate=accepted,

        cpuNodePct=empty_series(range_spec, safe_float(snapshot.get("cpu_node_pct", 0.0))),
        cpuWebdriverPct=empty_series(range_spec, safe_float(snapshot.get("cpu_webdriver_pct", 0.0))),
        cpuGenvmLlmPct=empty_series(range_spec, safe_float(snapshot.get("cpu_genvm_llm_pct", 0.0))),
        cpuGenvmWebPct=empty_series(range_spec, safe_float(snapshot.get("cpu_genvm_web_pct", 0.0))),

        memoryNodePct=empty_series(range_spec, safe_float(snapshot.get("memory_node_pct", 0.0))),
        memoryWebdriverPct=empty_series(range_spec, safe_float(snapshot.get("memory_webdriver_pct", 0.0))),
        memoryNodeRssBytes=empty_series(range_spec, safe_float(snapshot.get("memory_node_rss_bytes", 0.0))),

        diskDbUsagePct=empty_series(range_spec, safe_float(snapshot.get("disk_db_usage_pct", 0.0))),
        diskLogsUsagePct=empty_series(range_spec, safe_float(snapshot.get("disk_logs_usage_pct", 0.0))),

        networkNodeRxBps=empty_series(range_spec, 0.0),
        networkNodeTxBps=empty_series(range_spec, 0.0),

        wsActiveNewHeads=empty_series(range_spec, safe_float(snapshot.get("ws_active_newheads", 0.0))),
        wsMessagesReceivedRate=empty_series(range_spec, 0.0),

        processOpenFds=empty_series(range_spec, safe_float(snapshot.get("process_open_fds", 0.0))),
        goGoroutines=empty_series(range_spec, safe_float(snapshot.get("go_goroutines", 0.0))),
    )


def history_from_sqlite(range_spec: RangeSpec, network: str, snapshot: dict[str, Any], latency_now: float) -> History | None:
    if not sqlite_has_metric_history(network, range_spec):
        return None

    accepted_rate = sqlite_counter_rate_series(network, range_spec, "accepted_total")
    return History(
        throughput=accepted_rate,
        latencyP50=empty_series(range_spec, latency_now),
        latencyP95=empty_series(range_spec, latency_now * 1.25),
        blocksBehind=sqlite_metric_series(network, range_spec, "blocks_behind"),
        uptimePct=sqlite_metric_series(network, range_spec, "synced", scale=100.0),
        commitRate=sqlite_counter_rate_series(network, range_spec, "validator_commit_total"),
        revealRate=sqlite_counter_rate_series(network, range_spec, "validator_reveal_total"),

        latestBlock=sqlite_metric_series(network, range_spec, "latest_block"),
        syncedBlock=sqlite_metric_series(network, range_spec, "synced_block"),
        acceptedTxRate=accepted_rate,

        cpuNodePct=sqlite_metric_series(network, range_spec, "cpu_node_pct"),
        cpuWebdriverPct=sqlite_metric_series(network, range_spec, "cpu_webdriver_pct"),
        cpuGenvmLlmPct=sqlite_metric_series(network, range_spec, "cpu_genvm_llm_pct"),
        cpuGenvmWebPct=sqlite_metric_series(network, range_spec, "cpu_genvm_web_pct"),

        memoryNodePct=sqlite_metric_series(network, range_spec, "memory_node_pct"),
        memoryWebdriverPct=sqlite_metric_series(network, range_spec, "memory_webdriver_pct"),
        memoryNodeRssBytes=sqlite_metric_series(network, range_spec, "memory_node_rss_bytes"),

        diskDbUsagePct=sqlite_metric_series(network, range_spec, "disk_db_usage_pct"),
        diskLogsUsagePct=sqlite_metric_series(network, range_spec, "disk_logs_usage_pct"),

        networkNodeRxBps=sqlite_counter_rate_series(network, range_spec, "network_node_rx_total"),
        networkNodeTxBps=sqlite_counter_rate_series(network, range_spec, "network_node_tx_total"),

        wsActiveNewHeads=sqlite_metric_series(network, range_spec, "ws_active_newheads"),
        wsMessagesReceivedRate=sqlite_counter_rate_series(network, range_spec, "ws_messages_received_newheads"),

        processOpenFds=sqlite_metric_series(network, range_spec, "process_open_fds"),
        goGoroutines=sqlite_metric_series(network, range_spec, "go_goroutines"),
    )


async def history_from_prometheus(range_spec: RangeSpec, snapshot: dict[str, Any], latency_now: float) -> History | None:
    if not settings.prometheus_url or not prom_client:
        return None

    queries = {
        "accepted": prom_expr_delta_rate("genlayer_node_transactions_accepted_synced_total"),
        "commit": prom_expr_delta_rate("genlayer_node_transactions_validator_commit_total"),
        "reveal": prom_expr_delta_rate("genlayer_node_transactions_validator_reveal_total"),
        "blocks": prom_expr_for_metric("genlayer_node_blocks_behind"),
        "latest": prom_expr_for_metric("genlayer_node_latest_block"),
        "synced_block": prom_expr_for_metric("genlayer_node_synced_block"),
        "uptime_pct": f"100 * avg_over_time(({prom_expr_for_metric('genlayer_node_synced')})[30m:])",
        "cpu_node": prom_expr_for_metric("genlayer_node_cpu_usage_percent", {"component": "node"}),
        "cpu_webdriver": prom_expr_for_metric("genlayer_node_cpu_usage_percent", {"component": "webdriver"}),
        "cpu_llm": prom_expr_for_metric("genlayer_node_cpu_usage_percent", {"component": "genvm-llm"}),
        "cpu_web": prom_expr_for_metric("genlayer_node_cpu_usage_percent", {"component": "genvm-web"}),
        "memory_node": prom_expr_for_metric("genlayer_node_memory_percent", {"component": "node"}),
        "memory_webdriver": prom_expr_for_metric("genlayer_node_memory_percent", {"component": "webdriver"}),
        "memory_node_rss": prom_expr_for_metric("genlayer_node_memory_rss_bytes", {"component": "node"}),
        "disk_db": prom_expr_for_metric("genlayer_node_disk_usage_percent", {"component": "node", "directory": "genlayer.db"}),
        "disk_logs": prom_expr_for_metric("genlayer_node_disk_usage_percent", {"component": "node", "directory": "logs"}),
        "rx_bps": prom_expr_delta_rate("genlayer_node_network_rx_bytes_total", extra={"component": "node"}),
        "tx_bps": prom_expr_delta_rate("genlayer_node_network_tx_bytes_total", extra={"component": "node"}),
        "ws_active": prom_expr_for_metric("genlayer_rollup_ws_active_subscriptions", {"subscription_type": "newHeads"}),
        "ws_messages_rate": prom_expr_delta_rate("genlayer_rollup_ws_messages_received_total", extra={"subscription_type": "newHeads"}),
        "open_fds": prom_expr_for_metric("process_open_fds"),
        "go_goroutines": prom_expr_for_metric("go_goroutines"),
    }

    try:
        results = await asyncio.gather(*(fetch_range_prom(q, range_spec) for q in queries.values()))
    except Exception:
        return None

    mapped = dict(zip(queries.keys(), results, strict=True))
    latency_p50 = (
        await fetch_range_prom(settings.promql_latency_p50, range_spec)
        if settings.promql_latency_p50
        else empty_series(range_spec, latency_now)
    )
    latency_p95 = (
        await fetch_range_prom(settings.promql_latency_p95, range_spec)
        if settings.promql_latency_p95
        else empty_series(range_spec, latency_now * 1.25)
    )

    accepted = mapped["accepted"] or empty_series(range_spec, 0.0)
    return History(
        throughput=accepted,
        latencyP50=latency_p50,
        latencyP95=latency_p95,
        blocksBehind=mapped["blocks"] or empty_series(range_spec, safe_float(snapshot.get("blocks_behind", 0.0))),
        uptimePct=mapped["uptime_pct"] or empty_series(range_spec, 100.0 if safe_float(snapshot.get("synced", 0.0)) >= 1 else 0.0),
        commitRate=mapped["commit"] or empty_series(range_spec, 0.0),
        revealRate=mapped["reveal"] or empty_series(range_spec, 0.0),

        latestBlock=mapped["latest"] or empty_series(range_spec, safe_float(snapshot.get("latest_block", 0.0))),
        syncedBlock=mapped["synced_block"] or empty_series(range_spec, safe_float(snapshot.get("synced_block", 0.0))),
        acceptedTxRate=accepted,

        cpuNodePct=mapped["cpu_node"] or empty_series(range_spec, safe_float(snapshot.get("cpu_node_pct", 0.0))),
        cpuWebdriverPct=mapped["cpu_webdriver"] or empty_series(range_spec, safe_float(snapshot.get("cpu_webdriver_pct", 0.0))),
        cpuGenvmLlmPct=mapped["cpu_llm"] or empty_series(range_spec, safe_float(snapshot.get("cpu_genvm_llm_pct", 0.0))),
        cpuGenvmWebPct=mapped["cpu_web"] or empty_series(range_spec, safe_float(snapshot.get("cpu_genvm_web_pct", 0.0))),

        memoryNodePct=mapped["memory_node"] or empty_series(range_spec, safe_float(snapshot.get("memory_node_pct", 0.0))),
        memoryWebdriverPct=mapped["memory_webdriver"] or empty_series(range_spec, safe_float(snapshot.get("memory_webdriver_pct", 0.0))),
        memoryNodeRssBytes=mapped["memory_node_rss"] or empty_series(range_spec, safe_float(snapshot.get("memory_node_rss_bytes", 0.0))),

        diskDbUsagePct=mapped["disk_db"] or empty_series(range_spec, safe_float(snapshot.get("disk_db_usage_pct", 0.0))),
        diskLogsUsagePct=mapped["disk_logs"] or empty_series(range_spec, safe_float(snapshot.get("disk_logs_usage_pct", 0.0))),

        networkNodeRxBps=mapped["rx_bps"] or empty_series(range_spec, 0.0),
        networkNodeTxBps=mapped["tx_bps"] or empty_series(range_spec, 0.0),

        wsActiveNewHeads=mapped["ws_active"] or empty_series(range_spec, safe_float(snapshot.get("ws_active_newheads", 0.0))),
        wsMessagesReceivedRate=mapped["ws_messages_rate"] or empty_series(range_spec, 0.0),

        processOpenFds=mapped["open_fds"] or empty_series(range_spec, safe_float(snapshot.get("process_open_fds", 0.0))),
        goGoroutines=mapped["go_goroutines"] or empty_series(range_spec, safe_float(snapshot.get("go_goroutines", 0.0))),
    )


async def load_history(range_spec: RangeSpec, network: str, snapshot: dict[str, Any], latency_now: float) -> History:
    sqlite_history = history_from_sqlite(range_spec, network, snapshot, latency_now)
    if sqlite_history is not None:
        return sqlite_history

    prom_history = await history_from_prometheus(range_spec, snapshot, latency_now)
    if prom_history is not None:
        return prom_history

    return snapshot_history(range_spec, latency_now, snapshot)


def load_package_versions(snapshot: dict[str, Any]) -> list[PackageVersion]:
    packages = sqlite_packages()
    if packages:
        return packages
    return current_package_versions_from_snapshot(snapshot)


def load_rpc_methods(network: str, range_spec: RangeSpec) -> list[RpcMethodSeries]:
    return sqlite_rpc_methods(network, range_spec)


def load_ws_disconnections(network: str, range_spec: RangeSpec) -> list[WsDisconnectionSeries]:
    return sqlite_ws_disconnections(network, range_spec)


async def build_dashboard_payload(range_key: TimeRange) -> DashboardPayload:
    range_spec = RANGE_SPECS[range_key]

    health_task = asyncio.create_task(ops_client.health())
    snapshot_task = asyncio.create_task(load_snapshot_metrics())
    active_validators_task = asyncio.create_task(load_active_validators())
    latency_task = asyncio.create_task(probe_latency_ms())

    try:
        health = await health_task
    except Exception:
        health = {"status": "down"}

    snapshot_source: Literal["live", "sqlite"] = "live"
    snapshot_recorded_at: str | None = None
    snapshot_is_stale = False

    try:
        snapshot = await snapshot_task
    except Exception as exc:
        preferred_network = os.getenv("NETWORK_NAME") or None
        row = sqlite_latest_snapshot_row(preferred_network=preferred_network)
        if row is None:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to load metrics snapshot and no SQLite fallback available: {exc}",
            ) from exc
        snapshot = snapshot_from_sqlite_row(row)
        snapshot_source = "sqlite"
        snapshot_recorded_at = str(row["recorded_at"])
        snapshot_is_stale = True
        logger.warning(
            "live metrics unavailable, using sqlite fallback from %s for network=%s",
            snapshot_recorded_at,
            row["network"],
        )

    try:
        active_validators = await active_validators_task
    except Exception:
        active_validators = []

    try:
        latency_now = await latency_task
    except Exception:
        latency_now = 0.0

    overall_health, node_version, protocol_version, network = parse_health_versions(health)
    banned_local, quarantined_local = parse_validator_flags_from_health(health)

    metric_network = str(snapshot.get("metric_network") or "")
    metric_node_version = str(snapshot.get("metric_node_version") or "")
    metric_protocol_version = str(snapshot.get("metric_protocol_version") or "")

    if network == "genlayer" and metric_network:
        network = metric_network
    if node_version == "unknown" and metric_node_version:
        node_version = metric_node_version
    if protocol_version == "unknown" and metric_protocol_version:
        protocol_version = metric_protocol_version

    history = await load_history(range_spec, network, snapshot, latency_now)

    blocks_behind = safe_int(snapshot.get("blocks_behind", 0))
    latest_block = safe_int(snapshot.get("latest_block", 0))
    synced_block = safe_int(snapshot.get("synced_block", 0))
    synced = safe_float(snapshot.get("synced", 0)) >= 1.0
    uptime_seconds = safe_int(snapshot.get("uptime_seconds", 0))

    status_health = overall_health
    if snapshot_source == "sqlite" and overall_health == "down":
        if latest_block > 0 or synced_block > 0 or uptime_seconds > 0:
            status_health = "up"

    local_status = infer_local_status(
        overall_health=status_health,
        banned=banned_local,
        quarantined=quarantined_local,
        synced=synced,
        blocks_behind=blocks_behind,
    )

    throughput_tps = tail_value(history.acceptedTxRate, default=0.0)
    latency_p50 = tail_value(history.latencyP50, default=latency_now)
    latency_p95 = tail_value(history.latencyP95, default=latency_now * 1.25)
    uptime_pct_24h = clamp(mean_value(history.uptimePct), 0, 100)

    local_address = settings.validator_address or settings.prom_node_label or "local-node"
    local_row = ValidatorRow(
        id="local",
        moniker=settings.validator_moniker,
        address=local_address,
        status=local_status,
        uptimePct=round(uptime_pct_24h, 2),
        blocksBehind=blocks_behind,
        latencyMs=safe_int(latency_p50),
        throughputTps=round(throughput_tps, 2),
        lastSeen=iso_z(now_utc()),
    )

    rows: list[ValidatorRow] = [local_row]
    seen_addresses = {local_address.lower()}

    for idx, item in enumerate(active_validators, start=1):
        address = item.get("address", "")
        if not address or address.lower() in seen_addresses:
            continue
        rows.append(
            ValidatorRow(
                id=f"active-{idx}",
                moniker=item.get("moniker") or f"Validator {idx}",
                address=address,
                status="active",
                uptimePct=100.0,
                blocksBehind=0,
                latencyMs=0,
                throughputTps=0.0,
                lastSeen=iso_z(now_utc()),
            )
        )
        seen_addresses.add(address.lower())

    active_count = len(active_validators) if active_validators else (1 if local_status == "active" else 0)
    banned_count = 1 if local_status == "banned" else 0
    quarantined_count = 1 if local_status == "quarantined" else 0

    summary = Summary(
        network=network,
        validatorStatus=local_status,
        overallHealth=status_health,
        protocolVersion=protocol_version,
        nodeVersion=node_version,
        synced=synced,
        latestBlock=latest_block,
        syncedBlock=synced_block,
        blocksBehind=blocks_behind,
        uptimeSeconds=uptime_seconds,
        uptimePct24h=round(uptime_pct_24h, 2),
        txThroughputTps=round(throughput_tps, 2),
        latencyMsP50=safe_int(latency_p50),
        latencyMsP95=safe_int(latency_p95),
        activeValidators=active_count,
        bannedValidators=banned_count,
        quarantinedValidators=quarantined_count,
        cpuPct=round(safe_float(snapshot.get("cpu_pct", 0.0)), 2),
        memoryPct=round(safe_float(snapshot.get("memory_pct", 0.0)), 2),
        dataSource=snapshot_source,
        isStale=snapshot_is_stale,
        snapshotRecordedAt=snapshot_recorded_at or str(snapshot.get("recorded_at") or None),
    )

    return DashboardPayload(
        summary=summary,
        history=history,
        packageVersions=load_package_versions(snapshot),
        rpcMethods=load_rpc_methods(network, range_spec),
        wsDisconnections=load_ws_disconnections(network, range_spec),
        validators=rows,
    )


def graph_config_path() -> Path:
    return Path(settings.graph_config_path)


def metric_catalog_list() -> list[GraphMetricDefinition]:
    return sorted(METRIC_CATALOG.values(), key=lambda item: (item.group.lower(), item.title.lower()))


def load_graph_config() -> list[GraphConfigItem]:
    path = graph_config_path()
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("failed to parse graph config json at %s", path)
        return []
    rows = raw.get("graphs") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        return []

    out: list[GraphConfigItem] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        graph_id = str(item.get("id") or "").strip()
        title = str(item.get("title") or "").strip()
        metric_keys_raw = item.get("metricKeys") or item.get("metric_keys") or []
        if not graph_id or not title or not isinstance(metric_keys_raw, list):
            continue
        metric_keys = [str(key).strip() for key in metric_keys_raw if str(key).strip() in METRIC_CATALOG]
        if not metric_keys:
            continue
        out.append(GraphConfigItem(id=graph_id, title=title, metricKeys=metric_keys))
    return out


def save_graph_config(graphs: list[GraphConfigItem]) -> None:
    path = graph_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updatedAt": iso_z(now_utc()),
        "graphs": [graph.model_dump() for graph in graphs],
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")




@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "GenLayer Dashboard API is running"}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/genlayer/dashboard", response_model=DashboardPayload)
async def dashboard(range: TimeRange = Query(default="24h")) -> DashboardPayload:
    cache_key = f"dashboard:{range}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    payload = await build_dashboard_payload(range)
    await cache.set(cache_key, payload, settings.dashboard_cache_seconds)
    return payload


@app.get("/api/genlayer/graph-config", response_model=GraphConfigPayload)
async def get_graph_config() -> GraphConfigPayload:
    return GraphConfigPayload(metrics=metric_catalog_list(), graphs=load_graph_config())


@app.put("/api/genlayer/graph-config", response_model=GraphConfigPayload)
async def update_graph_config(body: GraphConfigUpdateRequest) -> GraphConfigPayload:
    cleaned: list[GraphConfigItem] = []
    seen_ids: set[str] = set()
    for graph in body.graphs:
        graph_id = graph.id.strip()
        title = graph.title.strip()
        metric_keys = []
        for key in graph.metricKeys:
            cleaned_key = key.strip()
            if cleaned_key in METRIC_CATALOG and cleaned_key not in metric_keys:
                metric_keys.append(cleaned_key)
        if not graph_id or graph_id in seen_ids or not title or not metric_keys:
            continue
        cleaned.append(GraphConfigItem(id=graph_id, title=title, metricKeys=metric_keys))
        seen_ids.add(graph_id)

    save_graph_config(cleaned)
    return GraphConfigPayload(metrics=metric_catalog_list(), graphs=cleaned)
