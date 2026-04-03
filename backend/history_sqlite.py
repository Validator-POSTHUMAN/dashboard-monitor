from __future__ import annotations

import os
import sqlite3
from pathlib import Path


SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS metric_snapshots (
    recorded_at TEXT NOT NULL,
    network TEXT NOT NULL,

    latest_block INTEGER,
    synced_block INTEGER,
    blocks_behind INTEGER,
    synced INTEGER,
    processing_block INTEGER,
    uptime_seconds REAL,

    cpu_node_pct REAL,
    cpu_webdriver_pct REAL,
    cpu_genvm_llm_pct REAL,
    cpu_genvm_web_pct REAL,

    memory_node_pct REAL,
    memory_webdriver_pct REAL,
    memory_node_rss_bytes REAL,
    memory_node_vms_bytes REAL,
    memory_webdriver_usage_bytes REAL,

    disk_db_usage_bytes REAL,
    disk_logs_usage_bytes REAL,
    disk_db_usage_pct REAL,
    disk_logs_usage_pct REAL,
    disk_db_free_bytes REAL,
    disk_logs_free_bytes REAL,
    disk_db_total_bytes REAL,
    disk_logs_total_bytes REAL,

    network_node_rx_total REAL,
    network_node_tx_total REAL,
    network_webdriver_rx_total REAL,
    network_webdriver_tx_total REAL,

    accepted_total REAL,
    activated_total REAL,
    leader_proposed_total REAL,
    leader_revealed_total REAL,
    validator_commit_total REAL,
    validator_reveal_total REAL,

    rollup_rpc_inflight_requests REAL,
    ws_active_newheads REAL,
    ws_messages_received_newheads REAL,

    go_goroutines REAL,
    go_threads REAL,
    process_open_fds REAL,
    process_resident_memory_bytes REAL,
    process_virtual_memory_bytes REAL,
    process_cpu_seconds_total REAL,
    process_network_receive_bytes_total REAL,
    process_network_transmit_bytes_total REAL,

    PRIMARY KEY (recorded_at, network)
);

CREATE INDEX IF NOT EXISTS idx_metric_snapshots_network_recorded_at
ON metric_snapshots(network, recorded_at);

CREATE INDEX IF NOT EXISTS idx_metric_snapshots_recorded_at
ON metric_snapshots(recorded_at);

CREATE TABLE IF NOT EXISTS package_versions (
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    source TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (name, source)
);

CREATE TABLE IF NOT EXISTS rpc_method_snapshots (
    recorded_at TEXT NOT NULL,
    network TEXT NOT NULL,
    method TEXT NOT NULL,
    requests_total REAL,
    request_bytes_total REAL,
    response_bytes_total REAL,
    duration_sum_seconds REAL,
    duration_count REAL,
    inflight_requests REAL,
    PRIMARY KEY (recorded_at, network, method)
);

CREATE INDEX IF NOT EXISTS idx_rpc_method_snapshots_network_recorded_at
ON rpc_method_snapshots(network, recorded_at);

CREATE TABLE IF NOT EXISTS rpc_duration_buckets (
    recorded_at TEXT NOT NULL,
    network TEXT NOT NULL,
    method TEXT NOT NULL,
    le REAL NOT NULL,
    bucket_count REAL NOT NULL,
    PRIMARY KEY (recorded_at, network, method, le)
);

CREATE INDEX IF NOT EXISTS idx_rpc_duration_buckets_network_recorded_at
ON rpc_duration_buckets(network, recorded_at);

CREATE TABLE IF NOT EXISTS ws_subscription_snapshots (
    recorded_at TEXT NOT NULL,
    network TEXT NOT NULL,
    subscription_type TEXT NOT NULL,
    active_subscriptions REAL,
    connections_total REAL,
    messages_received_total REAL,
    PRIMARY KEY (recorded_at, network, subscription_type)
);

CREATE INDEX IF NOT EXISTS idx_ws_subscription_snapshots_network_recorded_at
ON ws_subscription_snapshots(network, recorded_at);

CREATE TABLE IF NOT EXISTS ws_disconnection_snapshots (
    recorded_at TEXT NOT NULL,
    network TEXT NOT NULL,
    subscription_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    disconnections_total REAL,
    PRIMARY KEY (recorded_at, network, subscription_type, reason)
);

CREATE INDEX IF NOT EXISTS idx_ws_disconnection_snapshots_network_recorded_at
ON ws_disconnection_snapshots(network, recorded_at);
"""


DEFAULT_DB_PATH = str(
    Path(os.getenv("HISTORY_DB_PATH", Path(__file__).resolve().parent / "history" / "history.db"))
)


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_file) as conn:
        conn.executescript(SCHEMA)
        conn.commit()


def get_conn(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    return conn


if __name__ == "__main__":
    init_db(DEFAULT_DB_PATH)
    print(f"SQLite DB initialized: {DEFAULT_DB_PATH}")
