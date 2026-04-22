Async Concurrent Data Pipeline

A production-style ETL pipeline demonstrating **real concurrency** in Python — asyncio for I/O, multiprocessing for CPU work, and threading for background monitoring.

Processes 200 records from 5 simultaneous sources in under 3 seconds on a standard laptop.

---

## Why This Project Exists

Most Python "async" tutorials show toy examples. This shows how a real pipeline handles:
- Multiple concurrent data sources (asyncio.gather)
- CPU-bound transformation without GIL bottleneck (multiprocessing)
- Async DB writes without blocking the event loop (ThreadPoolExecutor)
- Graceful failure handling with a dead-letter queue
- Live monitoring without interrupting the pipeline (threading)

---

## Concurrency Architecture

```
                    ┌─────────────────────────────────────┐
                    │           EXTRACT STAGE              │
                    │         (asyncio.gather)             │
   source_0 ───────►│                                     │
   source_1 ───────►│  All sources fetched simultaneously │
   source_2 ───────►│  Total time ≈ slowest single fetch  │
   source_3 ───────►│  Not: N × single_fetch_time         │
   source_4 ───────►│                                     │
                    └──────────────┬──────────────────────┘
                                   │ RawRecord[]
                    ┌──────────────▼──────────────────────┐
                    │         TRANSFORM STAGE              │
                    │      (ProcessPoolExecutor)           │
   worker_0 ───────►│                                     │
   worker_1 ───────►│  True CPU parallelism via separate  │
   worker_2 ───────►│  OS processes — bypasses Python GIL │
   worker_3 ───────►│                                     │
                    └──────────────┬──────────────────────┘
                                   │ TransformedRecord[]
                    ┌──────────────▼──────────────────────┐
                    │           LOAD STAGE                 │
                    │  (asyncio + ThreadPoolExecutor)      │
                    │                                      │
                    │  Batches of 50 → concurrent writers  │
                    │  DB writes in thread pool so         │
                    │  event loop stays responsive         │
                    └─────────────────────────────────────┘
                    
   Background: PipelineMonitor thread prints live metrics every 2s
```

---

## The Concurrency Decision Tree

| Task type | Tool used | Why |
|-----------|-----------|-----|
| Network I/O, API calls | `asyncio.gather()` | Non-blocking, single thread |
| CPU computation | `ProcessPoolExecutor` | Bypasses GIL, true parallelism |
| File/DB I/O (blocking) | `ThreadPoolExecutor` | Offloads blocking calls from event loop |
| Background monitoring | `threading.Thread` | Daemon thread, no coordination needed |

This is the same model used in production ML inference services, data platforms, and async API backends.

---

## Quick Start

```bash
pip install -r requirements.txt
python pipeline.py
```

### Output
```
============================================================
Async Concurrent Data Pipeline
============================================================
Records: 200 | Sources: 5 | Workers: 4

12:34:01 [INFO] 🚀 Pipeline starting...
12:34:01 [INFO] ✓ Extracted 189/200 records
12:34:02 [INFO] 📊 extracted=189 | transformed=0 | loaded=0 | 0.0 rec/s
12:34:03 [INFO] ✓ Transformed 176 records (13 failed)
12:34:03 [INFO] ✓ Loaded 176 records to DB
12:34:03 [INFO] ✅ Pipeline complete

Pipeline Summary
============================================================
  extracted                 189
  transformed               176
  loaded                    176
  failed                    13
  elapsed_sec               2.84
  throughput_rps            62.0
  success_rate_pct          88.0

Output files: pipeline_output.db, pipeline_metrics.json, dead_letter.json
```

---

## Key Design Patterns

### Dead-Letter Queue
Records that fail transformation (after retries) go to `dead_letter.json` for offline inspection and reprocessing — not silently dropped. Standard in production data pipelines.

### Semaphore-controlled concurrency
```python
sem = asyncio.Semaphore(load_concurrency)
async with sem:
    await db.write_batch(batch)
```
Prevents overwhelming the DB with too many concurrent connections.

### Executor pattern for blocking calls
```python
result = await loop.run_in_executor(thread_pool, blocking_function, args)
```
Keeps the asyncio event loop non-blocking even when calling synchronous libraries (sqlite3, file I/O).

---

## Swap for Production

| Demo component | Production equivalent |
|---------------|----------------------|
| `asyncio.sleep()` (simulated fetch) | `aiohttp.ClientSession.get()` |
| SQLite | `asyncpg` (PostgreSQL async driver) |
| In-memory dead-letter | Kafka dead-letter topic / S3 bucket |
| `print` monitoring | Prometheus metrics + Grafana |

---

## Files

```
async-data-pipeline/
├── pipeline.py              # Full ETL implementation
├── requirements.txt
├── pipeline_output.db       # Auto-generated: SQLite output
├── pipeline_metrics.json    # Auto-generated: run summary
└── dead_letter.json         # Auto-generated: failed records
```

---

*Python 3.10+ · asyncio · multiprocessing · threading · SQLite*
