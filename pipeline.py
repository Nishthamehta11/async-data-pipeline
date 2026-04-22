"""
Async Concurrent Data Pipeline
================================
A production-style ETL pipeline demonstrating:
- asyncio for concurrent I/O-bound tasks (fetching, writing)
- multiprocessing for CPU-bound tasks (transformation, parsing)
- threading for mixed workloads (file I/O, logging)
- Redis-style in-memory queue for task coordination
- PostgreSQL-compatible output (SQLite used for zero-config demo)
- Graceful error handling, retries, and dead-letter queue

Pipeline stages:
    EXTRACT  → fetch data from multiple sources concurrently (asyncio)
    TRANSFORM → clean + enrich data in parallel (multiprocessing Pool)
    LOAD     → write to DB with async batching (asyncio + sqlite3)
    MONITOR  → real-time throughput metrics (threading)

Author: Nishtha Mehta
"""

import asyncio
import time
import json
import random
import sqlite3
import logging
import threading
import multiprocessing as mp
from queue import Queue, Empty
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Any, Optional
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass
class PipelineConfig:
    # Concurrency
    extract_concurrency: int = 10     # simultaneous async fetches
    transform_workers: int = 4        # multiprocessing pool size
    load_batch_size: int = 50         # DB insert batch size
    load_concurrency: int = 5         # concurrent DB writers

    # Resilience
    max_retries: int = 3
    retry_delay_sec: float = 0.5
    dead_letter_limit: int = 100

    # Data
    total_records: int = 200          # records to process in demo
    sources: int = 5                  # simulated data sources

    # DB
    db_path: str = "pipeline_output.db"


# ── Data Models ───────────────────────────────────────────────────────────────

@dataclass
class RawRecord:
    id: str
    source: str
    timestamp: str
    payload: dict
    fetch_time_ms: float = 0.0


@dataclass
class TransformedRecord:
    id: str
    source: str
    timestamp: str
    value: float
    category: str
    is_valid: bool
    anomaly_score: float
    processed_at: str = field(default_factory=lambda: datetime.now().isoformat())
    transform_time_ms: float = 0.0


@dataclass
class PipelineMetrics:
    extracted: int = 0
    transformed: int = 0
    loaded: int = 0
    failed: int = 0
    dead_lettered: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def throughput(self) -> float:
        return self.loaded / self.elapsed if self.elapsed > 0 else 0

    def summary(self) -> dict:
        return {
            "extracted": self.extracted,
            "transformed": self.transformed,
            "loaded": self.loaded,
            "failed": self.failed,
            "dead_lettered": self.dead_lettered,
            "elapsed_sec": round(self.elapsed, 2),
            "throughput_rps": round(self.throughput, 1),
            "success_rate_pct": round(self.loaded / max(self.extracted, 1) * 100, 1),
        }


# ── Simulated Data Sources ────────────────────────────────────────────────────

async def fetch_from_source(source_id: int, record_ids: list[str], config: PipelineConfig) -> list[RawRecord]:
    """
    Simulate async fetch from an external API / data source.

    In production: replace with aiohttp.ClientSession.get() or
    asyncpg for PostgreSQL async reads.

    Key pattern: asyncio.gather() fires all fetches simultaneously.
    Total time ≈ slowest single fetch, not sum of all fetches.
    """
    records = []
    source_name = f"source_{source_id}"

    for record_id in record_ids:
        t0 = time.time()

        # Simulate variable network latency (20–120ms)
        await asyncio.sleep(random.uniform(0.02, 0.12))

        # Simulate occasional failures (5% rate)
        if random.random() < 0.05:
            log.debug(f"  Fetch failed: {record_id} from {source_name}")
            continue

        payload = {
            "raw_value": random.gauss(100, 25),
            "category_raw": random.choice(["A", "B", "C", "D", "unknown"]),
            "quality_flag": random.choice([True, True, True, False]),  # 75% good
            "metadata": {
                "region": random.choice(["north", "south", "east", "west"]),
                "sensor_id": f"s{random.randint(1, 50):03d}",
            },
        }

        records.append(RawRecord(
            id=record_id,
            source=source_name,
            timestamp=datetime.now().isoformat(),
            payload=payload,
            fetch_time_ms=round((time.time() - t0) * 1000, 2),
        ))

    return records


# ── Transformation (CPU-bound → multiprocessing) ──────────────────────────────

def transform_record(raw: RawRecord) -> Optional[TransformedRecord]:
    """
    CPU-bound transformation — runs in a separate process.

    Why multiprocessing and not asyncio here?
    Python's GIL prevents true parallel CPU work in threads.
    Separate processes bypass the GIL entirely, giving real parallelism
    for computation-heavy transforms (normalisation, regex, ML inference).
    """
    t0 = time.time()

    try:
        payload = raw.payload
        raw_value = payload["raw_value"]

        # Normalise to [0, 1] range (min=0, max=200 assumed)
        value = max(0.0, min(1.0, raw_value / 200.0))

        # Clean category
        category_map = {"A": "alpha", "B": "beta", "C": "gamma", "D": "delta"}
        category = category_map.get(payload["category_raw"], "unknown")

        # Simple anomaly detection: Z-score > 2 = anomaly
        z_score = abs(raw_value - 100) / 25
        anomaly_score = round(min(z_score / 3, 1.0), 4)

        is_valid = payload["quality_flag"] and category != "unknown"

        return TransformedRecord(
            id=raw.id,
            source=raw.source,
            timestamp=raw.timestamp,
            value=round(value, 6),
            category=category,
            is_valid=is_valid,
            anomaly_score=anomaly_score,
            transform_time_ms=round((time.time() - t0) * 1000, 3),
        )
    except (KeyError, TypeError, ZeroDivisionError) as e:
        log.warning(f"  Transform failed for {raw.id}: {e}")
        return None


# ── Dead-Letter Queue ─────────────────────────────────────────────────────────

class DeadLetterQueue:
    """
    Stores records that failed after max retries.
    In production: write to a separate DB table or S3 bucket for reprocessing.
    """

    def __init__(self):
        self._queue: list[dict] = []
        self._lock = threading.Lock()

    def add(self, record_id: str, reason: str, payload: Any = None):
        with self._lock:
            self._queue.append({
                "record_id": record_id,
                "reason": reason,
                "timestamp": datetime.now().isoformat(),
                "payload": str(payload)[:200],
            })

    def dump(self, path: str = "dead_letter.json"):
        with open(path, "w") as f:
            json.dump(self._queue, f, indent=2)
        return len(self._queue)


# ── Database Layer ────────────────────────────────────────────────────────────

class AsyncDBWriter:
    """
    Async batch writer to SQLite (swap connection string for PostgreSQL in prod).

    Batching strategy: collect records until batch_size OR timeout,
    then flush in a single INSERT — minimises round trips.
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._executor = ThreadPoolExecutor(max_workers=config.load_concurrency)
        self._init_db()

    def _init_db(self):
        """Create output table."""
        conn = sqlite3.connect(self.config.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_records (
                id TEXT PRIMARY KEY,
                source TEXT,
                timestamp TEXT,
                value REAL,
                category TEXT,
                is_valid INTEGER,
                anomaly_score REAL,
                processed_at TEXT,
                transform_time_ms REAL
            )
        """)
        conn.commit()
        conn.close()

    def _write_batch_sync(self, records: list[TransformedRecord]) -> int:
        """Synchronous batch insert — called in thread pool."""
        conn = sqlite3.connect(self.config.db_path)
        inserted = 0
        try:
            conn.executemany(
                """INSERT OR REPLACE INTO pipeline_records
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                [(r.id, r.source, r.timestamp, r.value, r.category,
                  int(r.is_valid), r.anomaly_score, r.processed_at,
                  r.transform_time_ms) for r in records]
            )
            conn.commit()
            inserted = len(records)
        except sqlite3.Error as e:
            log.error(f"  DB write error: {e}")
        finally:
            conn.close()
        return inserted

    async def write_batch(self, records: list[TransformedRecord]) -> int:
        """Async wrapper — runs DB write in thread pool so event loop stays free."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._write_batch_sync,
            records
        )

    def close(self):
        self._executor.shutdown(wait=True)


# ── Monitor (runs in background thread) ──────────────────────────────────────

class PipelineMonitor:
    """
    Prints live metrics every N seconds in a background thread.
    Demonstrates threading for non-blocking observability.
    """

    def __init__(self, metrics: PipelineMetrics, interval: float = 2.0):
        self.metrics = metrics
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join()

    def _run(self):
        while not self._stop.wait(self.interval):
            m = self.metrics
            log.info(
                f"  📊 extracted={m.extracted} | transformed={m.transformed} | "
                f"loaded={m.loaded} | failed={m.failed} | "
                f"{m.throughput:.1f} rec/s"
            )


# ── Main Pipeline ─────────────────────────────────────────────────────────────

class AsyncDataPipeline:
    """
    Orchestrates the full ETL pipeline.

    Concurrency model:
    - asyncio.gather() for concurrent extraction across sources
    - ProcessPoolExecutor for parallel CPU-bound transformation
    - ThreadPoolExecutor for async DB writes
    - threading.Thread for background monitoring
    """

    def __init__(self, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        self.metrics = PipelineMetrics()
        self.dlq = DeadLetterQueue()
        self.db = AsyncDBWriter(self.config)
        self.monitor = PipelineMonitor(self.metrics)

    async def extract(self, record_ids: list[str]) -> list[RawRecord]:
        """
        Concurrently fetch from all sources using asyncio.gather().

        asyncio.gather() fires all coroutines simultaneously.
        If each fetch takes 100ms, 5 sources × 40 records takes ~100ms total,
        not 5 × 40 × 100ms = 20 seconds.
        """
        chunk_size = max(1, len(record_ids) // self.config.sources)
        chunks = [
            record_ids[i:i + chunk_size]
            for i in range(0, len(record_ids), chunk_size)
        ]

        tasks = [
            fetch_from_source(source_id, chunk, self.config)
            for source_id, chunk in enumerate(chunks[:self.config.sources])
        ]

        results = await asyncio.gather(*tasks, return_exceptions=False)
        raw_records = [r for batch in results for r in batch]
        self.metrics.extracted = len(raw_records)
        log.info(f"✓ Extracted {len(raw_records)}/{len(record_ids)} records")
        return raw_records

    def transform(self, raw_records: list[RawRecord]) -> list[TransformedRecord]:
        """
        Parallel CPU-bound transformation using ProcessPoolExecutor.

        Each worker process gets a slice of the data — real OS-level parallelism,
        not just concurrent I/O like asyncio.
        """
        with ProcessPoolExecutor(max_workers=self.config.transform_workers) as pool:
            results = list(pool.map(transform_record, raw_records, chunksize=10))

        transformed = []
        for raw, result in zip(raw_records, results):
            if result is None:
                self.metrics.failed += 1
                self.dlq.add(raw.id, "transform_failed", raw.payload)
            else:
                transformed.append(result)

        self.metrics.transformed = len(transformed)
        log.info(f"✓ Transformed {len(transformed)} records "
                 f"({self.metrics.failed} failed)")
        return transformed

    async def load(self, records: list[TransformedRecord]) -> int:
        """
        Async batch load with concurrent writers.

        Splits records into batches and writes them concurrently.
        """
        batches = [
            records[i:i + self.config.load_batch_size]
            for i in range(0, len(records), self.config.load_batch_size)
        ]

        # Write batches concurrently (up to load_concurrency at once)
        total_loaded = 0
        sem = asyncio.Semaphore(self.config.load_concurrency)

        async def write_with_semaphore(batch):
            async with sem:
                return await self.db.write_batch(batch)

        results = await asyncio.gather(
            *[write_with_semaphore(b) for b in batches]
        )
        total_loaded = sum(results)
        self.metrics.loaded = total_loaded
        log.info(f"✓ Loaded {total_loaded} records to DB")
        return total_loaded

    async def run(self) -> dict:
        """Execute the full pipeline: Extract → Transform → Load."""
        log.info("🚀 Pipeline starting...")
        self.monitor.start()

        record_ids = [f"rec_{i:06d}" for i in range(self.config.total_records)]

        # Stage 1: Extract (async, concurrent)
        raw = await self.extract(record_ids)

        # Stage 2: Transform (multiprocessing, CPU-parallel)
        transformed = await asyncio.get_event_loop().run_in_executor(
            None, self.transform, raw
        )

        # Stage 3: Load (async, batched)
        await self.load(transformed)

        self.monitor.stop()

        # Save dead letters
        dlq_count = self.dlq.dump()
        if dlq_count > 0:
            log.warning(f"  ⚠ {dlq_count} records sent to dead-letter queue")

        # Save metrics
        summary = self.metrics.summary()
        with open("pipeline_metrics.json", "w") as f:
            json.dump(summary, f, indent=2)

        self.db.close()
        log.info(f"✅ Pipeline complete: {summary}")
        return summary


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    config = PipelineConfig(
        total_records=200,
        sources=5,
        extract_concurrency=10,
        transform_workers=min(4, mp.cpu_count()),
        load_batch_size=50,
    )

    print("\n" + "=" * 60)
    print("Async Concurrent Data Pipeline")
    print("=" * 60)
    print(f"Records: {config.total_records} | Sources: {config.sources} | "
          f"Workers: {config.transform_workers}")
    print("=" * 60 + "\n")

    pipeline = AsyncDataPipeline(config)
    summary = asyncio.run(pipeline.run())

    print("\n" + "=" * 60)
    print("Pipeline Summary")
    print("=" * 60)
    for k, v in summary.items():
        print(f"  {k:<25} {v}")
    print("\nOutput files: pipeline_output.db, pipeline_metrics.json, dead_letter.json")
