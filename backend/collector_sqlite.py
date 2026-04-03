from __future__ import annotations

import logging
import os
from datetime import UTC, datetime

import requests
from dotenv import load_dotenv
from prometheus_client.parser import text_string_to_metric_families

from history_sqlite import get_conn, init_db


load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger(__name__)

OPS_BASE_URL = os.getenv("OPS_BASE_URL", "http://127.0.0.1:9153").rstrip("/")
HISTORY_DB_PATH = os.getenv("HISTORY_DB_PATH", "./history/history.db")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))


def iso_z() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_float(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def scrape_metrics_text() -> str:
    resp = requests.get(f"{OPS_BASE_URL}/metrics", timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def parse_samples(text: str) -> list[tuple[str, dict[str, str], float]]:
    samples: list[tuple[str, dict[str, str], float]] = []
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            labels = {k: str(v) for k, v in sample.labels.items()}
            samples.append((sample.name, labels, safe_float(sample.value)))
    return samples


def pick(samples: list[tuple[str, dict[str, str], float]], name: str, **required_labels: str) -> float:
    out: list[float] = []
    for metric_name, labels, value in samples:
        if metric_name != name:
            continue
        ok = True
        for k, v in required_labels.items():
            if labels.get(k) != str(v):
                ok = False
                break
        if ok:
            out.append(value)
    return max(out) if out else 0.0


def first_labels(samples: list[tuple[str, dict[str, str], float]], name: str) -> dict[str, str]:
    for metric_name, labels, _ in samples:
        if metric_name == name:
            return labels
    return {}


def collect_rpc_methods(
    samples: list[tuple[str, dict[str, str], float]],
    network: str,
    recorded_at: str,
    conn,
) -> None:
    methods: set[str] = set()
    for name, labels, _ in samples:
        if name.startswith("genlayer_rollup_rpc_") and labels.get("network") == network and "method" in labels:
            methods.add(labels["method"])

    inflight = pick(samples, "genlayer_rollup_rpc_inflight_requests", network=network)

    for method in sorted(methods):
        requests_total = 0.0
        request_bytes_total = 0.0
        response_bytes_total = 0.0
        duration_sum_seconds = 0.0
        duration_count = 0.0

        for name, labels, value in samples:
            if labels.get("network") != network or labels.get("method") != method:
                continue

            if name == "genlayer_rollup_rpc_requests_total" and labels.get("status") == "success":
                requests_total += value
            elif name == "genlayer_rollup_rpc_request_bytes_total":
                request_bytes_total += value
            elif name == "genlayer_rollup_rpc_response_bytes_total":
                response_bytes_total += value
            elif name in (
                "genlayer_rollup_rpc_request_duration_seconds_sum",
                "genlayer_rollup_rpc_request_duration_seconds_sum_total",
            ):
                duration_sum_seconds += value
            elif name in (
                "genlayer_rollup_rpc_request_duration_seconds_count",
                "genlayer_rollup_rpc_request_duration_seconds_count_total",
            ):
                duration_count += value

        conn.execute(
            """
            INSERT OR REPLACE INTO rpc_method_snapshots (
                recorded_at, network, method, requests_total, request_bytes_total,
                response_bytes_total, duration_sum_seconds, duration_count, inflight_requests
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recorded_at,
                network,
                method,
                requests_total,
                request_bytes_total,
                response_bytes_total,
                duration_sum_seconds,
                duration_count,
                inflight,
            ),
        )

        for name, labels, value in samples:
            if name not in (
                "genlayer_rollup_rpc_request_duration_seconds_bucket",
                "genlayer_rollup_rpc_request_duration_seconds_bucket_total",
            ):
                continue
            if labels.get("network") != network or labels.get("method") != method:
                continue

            le = labels.get("le", "")
            if le == "+Inf":
                continue

            try:
                le_value = float(le)
            except Exception:
                continue

            conn.execute(
                """
                INSERT OR REPLACE INTO rpc_duration_buckets (
                    recorded_at, network, method, le, bucket_count
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (recorded_at, network, method, le_value, value),
            )


def collect_ws(
    samples: list[tuple[str, dict[str, str], float]],
    network: str,
    recorded_at: str,
    conn,
) -> None:
    sub_types: set[str] = set()
    for name, labels, _ in samples:
        if name.startswith("genlayer_rollup_ws_") and labels.get("network") == network and "subscription_type" in labels:
            sub_types.add(labels["subscription_type"])

    for sub_type in sorted(sub_types):
        conn.execute(
            """
            INSERT OR REPLACE INTO ws_subscription_snapshots (
                recorded_at, network, subscription_type,
                active_subscriptions, connections_total, messages_received_total
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                recorded_at,
                network,
                sub_type,
                pick(samples, "genlayer_rollup_ws_active_subscriptions", network=network, subscription_type=sub_type),
                pick(samples, "genlayer_rollup_ws_connections_total", network=network, subscription_type=sub_type),
                pick(samples, "genlayer_rollup_ws_messages_received_total", network=network, subscription_type=sub_type),
            ),
        )

        reasons: set[str] = set()
        for name, labels, _ in samples:
            if (
                name == "genlayer_rollup_ws_disconnections_total"
                and labels.get("network") == network
                and labels.get("subscription_type") == sub_type
            ):
                reasons.add(labels.get("reason", "unknown"))

        for reason in sorted(reasons):
            conn.execute(
                """
                INSERT OR REPLACE INTO ws_disconnection_snapshots (
                    recorded_at, network, subscription_type, reason, disconnections_total
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    recorded_at,
                    network,
                    sub_type,
                    reason,
                    pick(
                        samples,
                        "genlayer_rollup_ws_disconnections_total",
                        network=network,
                        subscription_type=sub_type,
                        reason=reason,
                    ),
                ),
            )


def upsert_package_versions(
    samples: list[tuple[str, dict[str, str], float]],
    updated_at: str,
    conn,
) -> None:
    info = first_labels(samples, "genlayer_node_info")
    go_info = first_labels(samples, "go_info")

    packages = [
        ("genlayer-node", info.get("version", ""), "genlayer_node_info"),
        ("genlayer-protocol", info.get("protocol_version", ""), "genlayer_node_info"),
        ("genvm", info.get("genvm_version", ""), "genlayer_node_info"),
        ("genlayer-commit", info.get("commit", ""), "genlayer_node_info"),
        ("go-runtime", go_info.get("version", ""), "go_info"),
    ]

    for name, version, source in packages:
        if not version:
            continue
        conn.execute(
            """
            INSERT OR REPLACE INTO package_versions (name, version, source, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (name, version, source, updated_at),
        )


def main() -> None:
    init_db(HISTORY_DB_PATH)
    text = scrape_metrics_text()
    samples = parse_samples(text)

    recorded_at = iso_z()
    info = first_labels(samples, "genlayer_node_info")
    network = info.get("network") or os.getenv("NETWORK_NAME") or "genlayer"

    with get_conn(HISTORY_DB_PATH) as conn:
        snapshot_row = (
            recorded_at,
            network,
            int(pick(samples, "genlayer_node_latest_block", network=network)),
            int(pick(samples, "genlayer_node_synced_block", network=network)),
            int(pick(samples, "genlayer_node_blocks_behind", network=network)),
            int(pick(samples, "genlayer_node_synced", network=network)),
            int(pick(samples, "genlayer_node_processing_block", network=network)),
            pick(samples, "genlayer_node_uptime_seconds", network=network, component="node"),

            pick(samples, "genlayer_node_cpu_usage_percent", network=network, component="node"),
            pick(samples, "genlayer_node_cpu_usage_percent", network=network, component="webdriver"),
            pick(samples, "genlayer_node_cpu_usage_percent", network=network, component="genvm-llm"),
            pick(samples, "genlayer_node_cpu_usage_percent", network=network, component="genvm-web"),

            pick(samples, "genlayer_node_memory_percent", network=network, component="node"),
            pick(samples, "genlayer_node_memory_percent", network=network, component="webdriver"),
            pick(samples, "genlayer_node_memory_rss_bytes", network=network, component="node"),
            pick(samples, "genlayer_node_memory_vms_bytes", network=network, component="node"),
            pick(samples, "genlayer_node_memory_usage_bytes", network=network, component="webdriver"),

            pick(samples, "genlayer_node_disk_usage_bytes", network=network, component="node", directory="genlayer.db"),
            pick(samples, "genlayer_node_disk_usage_bytes", network=network, component="node", directory="logs"),
            pick(samples, "genlayer_node_disk_usage_percent", network=network, component="node", directory="genlayer.db"),
            pick(samples, "genlayer_node_disk_usage_percent", network=network, component="node", directory="logs"),
            pick(samples, "genlayer_node_disk_free_bytes", network=network, component="node", directory="genlayer.db"),
            pick(samples, "genlayer_node_disk_free_bytes", network=network, component="node", directory="logs"),
            pick(samples, "genlayer_node_disk_total_bytes", network=network, component="node", directory="genlayer.db"),
            pick(samples, "genlayer_node_disk_total_bytes", network=network, component="node", directory="logs"),

            pick(samples, "genlayer_node_network_rx_bytes_total", network=network, component="node"),
            pick(samples, "genlayer_node_network_tx_bytes_total", network=network, component="node"),
            pick(samples, "genlayer_node_network_rx_bytes_total", network=network, component="webdriver"),
            pick(samples, "genlayer_node_network_tx_bytes_total", network=network, component="webdriver"),

            pick(samples, "genlayer_node_transactions_accepted_synced_total", network=network),
            pick(samples, "genlayer_node_transactions_activated_total", network=network),
            pick(samples, "genlayer_node_transactions_leader_proposed_total", network=network),
            pick(samples, "genlayer_node_transactions_leader_revealed_total", network=network),
            pick(samples, "genlayer_node_transactions_validator_commit_total", network=network),
            pick(samples, "genlayer_node_transactions_validator_reveal_total", network=network),

            pick(samples, "genlayer_rollup_rpc_inflight_requests", network=network),
            pick(samples, "genlayer_rollup_ws_active_subscriptions", network=network, subscription_type="newHeads"),
            pick(samples, "genlayer_rollup_ws_messages_received_total", network=network, subscription_type="newHeads"),

            pick(samples, "go_goroutines"),
            pick(samples, "go_threads"),
            pick(samples, "process_open_fds"),
            pick(samples, "process_resident_memory_bytes"),
            pick(samples, "process_virtual_memory_bytes"),
            pick(samples, "process_cpu_seconds_total"),
            pick(samples, "process_network_receive_bytes_total"),
            pick(samples, "process_network_transmit_bytes_total"),
        )

        metric_snapshot_columns = """
            recorded_at, network,
            latest_block, synced_block, blocks_behind, synced, processing_block, uptime_seconds,
            cpu_node_pct, cpu_webdriver_pct, cpu_genvm_llm_pct, cpu_genvm_web_pct,
            memory_node_pct, memory_webdriver_pct, memory_node_rss_bytes, memory_node_vms_bytes, memory_webdriver_usage_bytes,
            disk_db_usage_bytes, disk_logs_usage_bytes, disk_db_usage_pct, disk_logs_usage_pct,
            disk_db_free_bytes, disk_logs_free_bytes, disk_db_total_bytes, disk_logs_total_bytes,
            network_node_rx_total, network_node_tx_total, network_webdriver_rx_total, network_webdriver_tx_total,
            accepted_total, activated_total, leader_proposed_total, leader_revealed_total, validator_commit_total, validator_reveal_total,
            rollup_rpc_inflight_requests, ws_active_newheads, ws_messages_received_newheads,
            go_goroutines, go_threads, process_open_fds, process_resident_memory_bytes, process_virtual_memory_bytes,
            process_cpu_seconds_total, process_network_receive_bytes_total, process_network_transmit_bytes_total
        """

        placeholders = ", ".join(["?"] * len(snapshot_row))

        conn.execute(
            f"""
            INSERT OR REPLACE INTO metric_snapshots (
                {metric_snapshot_columns}
            ) VALUES ({placeholders})
            """,
            snapshot_row,
        )

        upsert_package_versions(samples, recorded_at, conn)
        collect_rpc_methods(samples, network, recorded_at, conn)
        collect_ws(samples, network, recorded_at, conn)

        conn.commit()

    print(f"[OK] snapshot written: {HISTORY_DB_PATH} @ {recorded_at} network={network}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("collector failed")
        raise
