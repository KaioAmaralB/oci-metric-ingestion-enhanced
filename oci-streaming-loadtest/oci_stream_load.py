#!/usr/bin/env python3
"""Small OCI Streaming producer/consumer load generator.

Targets a message rate (messages per second), not one HTTP request per message.
Uses PutMessages batching and consumer groups, which is representative of a
normal OCI Streaming client.
"""

import argparse
import base64
import json
import math
import os
import signal
import socket
import statistics
import sys
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

try:
    import oci
except ImportError as exc:  # pragma: no cover - startup validation
    raise SystemExit(
        "OCI SDK not installed. Run: python3 -m pip install -r requirements.txt"
    ) from exc


MIB = 1024 * 1024
SAFE_REQUEST_BYTES = 900 * 1024


class Metrics:
    """Thread-safe counters plus short rolling latency samples."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = defaultdict(int)
        self._latencies: Dict[str, Deque[float]] = {
            "put": deque(maxlen=5000),
            "get": deque(maxlen=5000),
        }

    def add(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] += value

    def observe(self, name: str, seconds: float) -> None:
        with self._lock:
            self._latencies[name].append(seconds)

    def snapshot(self, clear_latencies: bool = False) -> Tuple[Dict[str, int], Dict[str, List[float]]]:
        with self._lock:
            counters = dict(self._counters)
            latencies = {key: list(values) for key, values in self._latencies.items()}
            if clear_latencies:
                for values in self._latencies.values():
                    values.clear()
        return counters, latencies


class RunControl:
    def __init__(self, duration_seconds: int) -> None:
        self.duration_seconds = duration_seconds
        self.start_event = threading.Event()
        self.stop_event = threading.Event()
        self.started_at: Optional[float] = None
        self.ends_at: Optional[float] = None

    def start(self) -> None:
        self.started_at = time.monotonic()
        self.ends_at = self.started_at + self.duration_seconds
        self.start_event.set()

    def should_stop(self) -> bool:
        if self.stop_event.is_set():
            return True
        return self.ends_at is not None and time.monotonic() >= self.ends_at

    def wait(self, seconds: float) -> bool:
        """Wait, returning True when the run should stop."""
        if seconds <= 0:
            return self.should_stop()
        self.stop_event.wait(seconds)
        return self.should_stop()

    def stop(self) -> None:
        self.stop_event.set()


class ClientFactory:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args

    def create(self):
        no_retry = oci.retry.NoneRetryStrategy()
        if self.args.auth == "instance_principal":
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return oci.streaming.StreamClient(
                {},
                signer=signer,
                service_endpoint=self.args.endpoint,
                retry_strategy=no_retry,
            )

        config = oci.config.from_file(self.args.config_file, self.args.profile)
        return oci.streaming.StreamClient(
            config,
            service_endpoint=self.args.endpoint,
            retry_strategy=no_retry,
        )


def percentile_ms(values: List[float], percentile: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1))
    return round(ordered[index] * 1000, 2)


def build_payload(message_bytes: int, worker_id: int, sequence: int) -> bytes:
    header = f"w={worker_id}|seq={sequence}|ts={time.time_ns()}|".encode("utf-8")
    if len(header) >= message_bytes:
        return header[:message_bytes]
    return header + (b"x" * (message_bytes - len(header)))


def build_batch(
    worker_id: int,
    first_sequence: int,
    batch_size: int,
    message_bytes: int,
):
    entries = []
    for offset in range(batch_size):
        sequence = first_sequence + offset
        # Unique keys distribute traffic across partitions. Reusing one key would
        # intentionally pin all messages to one partition.
        raw_key = f"load-{worker_id}-{sequence}".encode("utf-8")
        raw_value = build_payload(message_bytes, worker_id, sequence)
        entries.append(
            oci.streaming.models.PutMessagesDetailsEntry(
                key=base64.b64encode(raw_key).decode("ascii"),
                value=base64.b64encode(raw_value).decode("ascii"),
            )
        )
    return oci.streaming.models.PutMessagesDetails(messages=entries)


def producer_worker(
    worker_id: int,
    args: argparse.Namespace,
    factory: ClientFactory,
    metrics: Metrics,
    control: RunControl,
) -> None:
    try:
        client = factory.create()
        worker_rate = args.message_rate / args.producer_workers
        sequence = worker_id * 10**12
        next_due = time.monotonic()
        control.start_event.wait()

        while not control.should_stop():
            details = build_batch(worker_id, sequence, args.batch_size, args.message_bytes)
            sequence += args.batch_size
            started = time.monotonic()

            try:
                response = client.put_messages(args.stream_id, details)
                elapsed = time.monotonic() - started
                entries = response.data.entries or []
                failed = sum(1 for entry in entries if entry.error)
                succeeded = args.batch_size - failed
                metrics.add("put_requests")
                metrics.add("produced", succeeded)
                metrics.add("produce_failed", failed)
                metrics.observe("put", elapsed)
            except oci.exceptions.ServiceError as exc:
                metrics.add("put_requests")
                metrics.add("put_errors")
                if exc.status == 429:
                    metrics.add("put_throttled")
                print(
                    json.dumps(
                        {
                            "event": "put_error",
                            "worker": worker_id,
                            "status": exc.status,
                            "code": exc.code,
                            "message": exc.message,
                        }
                    ),
                    flush=True,
                )
            except Exception as exc:  # Keep test running but make failure visible.
                metrics.add("put_requests")
                metrics.add("put_errors")
                print(
                    json.dumps(
                        {
                            "event": "put_exception",
                            "worker": worker_id,
                            "type": type(exc).__name__,
                            "message": str(exc),
                        }
                    ),
                    flush=True,
                )

            # Pace messages, not API calls. A batch of 100 at 750 msg/s per
            # worker results in 7.5 PutMessages calls/s for that worker.
            next_due += args.batch_size / worker_rate
            now = time.monotonic()
            delay = next_due - now
            if delay > 0:
                control.wait(delay)
            elif delay < -1.0:
                # Avoid a large catch-up burst after a slow/throttled request.
                next_due = now
    except Exception as exc:
        metrics.add("producer_worker_fatal")
        print(
            json.dumps(
                {
                    "event": "producer_worker_fatal",
                    "worker": worker_id,
                    "type": type(exc).__name__,
                    "message": str(exc),
                }
            ),
            flush=True,
        )
        control.stop()


def consumer_worker(
    worker_id: int,
    args: argparse.Namespace,
    factory: ClientFactory,
    metrics: Metrics,
    control: RunControl,
) -> None:
    try:
        # Stagger group-cursor creation because CreateGroupCursor itself is rate-limited.
        time.sleep(worker_id * 0.25)
        client = factory.create()
        instance_name = f"{socket.gethostname()}-{worker_id}-{uuid.uuid4().hex[:8]}"
        cursor_type = (
            oci.streaming.models.CreateGroupCursorDetails.TYPE_LATEST
            if args.start_position == "latest"
            else oci.streaming.models.CreateGroupCursorDetails.TYPE_TRIM_HORIZON
        )
        cursor_details = oci.streaming.models.CreateGroupCursorDetails(
            group_name=args.group_name,
            instance_name=instance_name,
            type=cursor_type,
            commit_on_get=True,
        )
        cursor = client.create_group_cursor(args.stream_id, cursor_details).data.value
        metrics.add("consumer_workers_ready")
        next_due = time.monotonic()
        control.start_event.wait()

        while not control.should_stop():
            started = time.monotonic()
            try:
                response = client.get_messages(
                    args.stream_id,
                    cursor,
                    limit=args.get_limit,
                )
                elapsed = time.monotonic() - started
                cursor = response.headers["opc-next-cursor"]
                count = len(response.data or [])
                metrics.add("get_requests")
                metrics.add("consumed", count)
                if count == 0:
                    metrics.add("get_empty")
                metrics.observe("get", elapsed)
            except oci.exceptions.ServiceError as exc:
                metrics.add("get_requests")
                metrics.add("get_errors")
                if exc.status == 429:
                    metrics.add("get_throttled")
                print(
                    json.dumps(
                        {
                            "event": "get_error",
                            "worker": worker_id,
                            "status": exc.status,
                            "code": exc.code,
                            "message": exc.message,
                        }
                    ),
                    flush=True,
                )
            except Exception as exc:
                metrics.add("get_requests")
                metrics.add("get_errors")
                print(
                    json.dumps(
                        {
                            "event": "get_exception",
                            "worker": worker_id,
                            "type": type(exc).__name__,
                            "message": str(exc),
                        }
                    ),
                    flush=True,
                )

            # OCI allows 5 GetMessages requests/s per partition per consumer group.
            # The recommended topology is one worker per partition, each capped at 5 rps.
            next_due += 1.0 / args.get_rps_per_worker
            now = time.monotonic()
            delay = next_due - now
            if delay > 0:
                control.wait(delay)
            elif delay < -1.0:
                next_due = now
    except Exception as exc:
        metrics.add("consumer_worker_fatal")
        print(
            json.dumps(
                {
                    "event": "consumer_worker_fatal",
                    "worker": worker_id,
                    "type": type(exc).__name__,
                    "message": str(exc),
                }
            ),
            flush=True,
        )
        control.stop()


def reporter(metrics: Metrics, control: RunControl, interval: float) -> None:
    previous: Dict[str, int] = {}
    control.start_event.wait()
    last_time = time.monotonic()

    while not control.should_stop():
        if control.wait(interval):
            break
        now = time.monotonic()
        elapsed = max(now - last_time, 0.001)
        counters, latencies = metrics.snapshot(clear_latencies=True)

        def rate(name: str) -> float:
            return round((counters.get(name, 0) - previous.get(name, 0)) / elapsed, 2)

        output = {
            "event": "rate",
            "elapsed_s": round(now - (control.started_at or now), 1),
            "produce_msg_s": rate("produced"),
            "put_req_s": rate("put_requests"),
            "consume_msg_s": rate("consumed"),
            "get_req_s": rate("get_requests"),
            "put_p50_ms": percentile_ms(latencies["put"], 0.50),
            "put_p95_ms": percentile_ms(latencies["put"], 0.95),
            "get_p50_ms": percentile_ms(latencies["get"], 0.50),
            "get_p95_ms": percentile_ms(latencies["get"], 0.95),
            "put_errors_total": counters.get("put_errors", 0),
            "put_throttled_total": counters.get("put_throttled", 0),
            "get_errors_total": counters.get("get_errors", 0),
            "get_throttled_total": counters.get("get_throttled", 0),
            "produced_total": counters.get("produced", 0),
            "consumed_total": counters.get("consumed", 0),
        }
        print(json.dumps(output), flush=True)
        previous = counters
        last_time = now


def validate_args(args: argparse.Namespace) -> None:
    if args.message_rate <= 0:
        raise SystemExit("--message-rate must be greater than zero")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be greater than zero")
    if args.message_bytes <= 0:
        raise SystemExit("--message-bytes must be greater than zero")
    if args.producer_workers <= 0 or args.consumer_workers <= 0:
        raise SystemExit("worker counts must be greater than zero")
    if args.get_limit < 1 or args.get_limit > 10000:
        raise SystemExit("--get-limit must be between 1 and 10000")
    if not (0 < args.get_rps_per_worker <= 5):
        raise SystemExit("--get-rps-per-worker must be greater than 0 and at most 5")

    # OCI calculates the 1 MiB request limit from decoded keys + values. Leave
    # headroom for keys and serialization instead of operating at the hard edge.
    estimated_decoded_bytes = args.batch_size * (args.message_bytes + 64)
    if estimated_decoded_bytes > SAFE_REQUEST_BYTES:
        raise SystemExit(
            "Estimated decoded PutMessages batch is too large: "
            f"{estimated_decoded_bytes} bytes. Reduce --batch-size or --message-bytes."
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate and consume OCI Streaming load at a target message rate."
    )
    parser.add_argument("mode", choices=["producer", "consumer", "both"])
    parser.add_argument(
        "--stream-id",
        default=os.getenv("OCI_STREAM_ID"),
        required=os.getenv("OCI_STREAM_ID") is None,
        help="Stream OCID; or set OCI_STREAM_ID.",
    )
    parser.add_argument(
        "--endpoint",
        default=os.getenv("OCI_STREAM_ENDPOINT"),
        required=os.getenv("OCI_STREAM_ENDPOINT") is None,
        help="Messages endpoint; or set OCI_STREAM_ENDPOINT.",
    )
    parser.add_argument("--duration", type=int, default=600, help="Test duration in seconds.")
    parser.add_argument("--message-rate", type=int, default=3000, help="Producer messages/s.")
    parser.add_argument("--message-bytes", type=int, default=256, help="Decoded value bytes/message.")
    parser.add_argument("--batch-size", type=int, default=100, help="Messages per PutMessages request.")
    parser.add_argument("--producer-workers", type=int, default=4)
    parser.add_argument("--consumer-workers", type=int, default=2)
    parser.add_argument("--get-limit", type=int, default=10000)
    parser.add_argument("--get-rps-per-worker", type=float, default=5.0)
    parser.add_argument(
        "--group-name",
        default=f"loadtest-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
    )
    parser.add_argument("--start-position", choices=["latest", "trim_horizon"], default="latest")
    parser.add_argument("--startup-delay", type=float, default=3.0)
    parser.add_argument("--report-interval", type=float, default=1.0)
    parser.add_argument(
        "--auth",
        choices=["instance_principal", "config"],
        default="instance_principal",
    )
    parser.add_argument("--config-file", default=os.path.expanduser("~/.oci/config"))
    parser.add_argument("--profile", default="DEFAULT")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    validate_args(args)

    metrics = Metrics()
    control = RunControl(args.duration)
    factory = ClientFactory(args)
    threads: List[threading.Thread] = []

    def stop_handler(_signum, _frame) -> None:
        control.stop()

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    if args.mode in ("consumer", "both"):
        for worker_id in range(args.consumer_workers):
            thread = threading.Thread(
                target=consumer_worker,
                args=(worker_id, args, factory, metrics, control),
                name=f"consumer-{worker_id}",
                daemon=True,
            )
            thread.start()
            threads.append(thread)

    if args.mode in ("producer", "both"):
        for worker_id in range(args.producer_workers):
            thread = threading.Thread(
                target=producer_worker,
                args=(worker_id, args, factory, metrics, control),
                name=f"producer-{worker_id}",
                daemon=True,
            )
            thread.start()
            threads.append(thread)

    report_thread = threading.Thread(
        target=reporter,
        args=(metrics, control, args.report_interval),
        name="reporter",
        daemon=True,
    )
    report_thread.start()
    threads.append(report_thread)

    print(
        json.dumps(
            {
                "event": "starting",
                "mode": args.mode,
                "duration_s": args.duration,
                "message_rate": args.message_rate,
                "message_bytes": args.message_bytes,
                "batch_size": args.batch_size,
                "producer_workers": args.producer_workers,
                "consumer_workers": args.consumer_workers,
                "group_name": args.group_name,
                "estimated_payload_mb_s": round(args.message_rate * args.message_bytes / 1_000_000, 3),
            }
        ),
        flush=True,
    )

    # Give consumers time to create their group cursors before the producer starts.
    time.sleep(max(args.startup_delay, 0))
    control.start()

    try:
        while not control.should_stop():
            time.sleep(0.2)
    finally:
        control.stop()
        for thread in threads:
            thread.join(timeout=10)

    counters, latencies = metrics.snapshot()
    runtime = max((time.monotonic() - (control.started_at or time.monotonic())), 0.001)
    summary = {
        "event": "summary",
        "runtime_s": round(runtime, 2),
        "produced": counters.get("produced", 0),
        "producer_avg_msg_s": round(counters.get("produced", 0) / runtime, 2),
        "put_requests": counters.get("put_requests", 0),
        "put_errors": counters.get("put_errors", 0),
        "put_throttled": counters.get("put_throttled", 0),
        "consumed": counters.get("consumed", 0),
        "consumer_avg_msg_s": round(counters.get("consumed", 0) / runtime, 2),
        "get_requests": counters.get("get_requests", 0),
        "get_errors": counters.get("get_errors", 0),
        "get_throttled": counters.get("get_throttled", 0),
        "produce_failed_entries": counters.get("produce_failed", 0),
    }
    print(json.dumps(summary), flush=True)

    return 0 if not (summary["put_errors"] or summary["get_errors"] or summary["produce_failed_entries"]) else 2


if __name__ == "__main__":
    sys.exit(main())
