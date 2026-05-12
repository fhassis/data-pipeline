# NATS JetStream Data Pipeline — Tutorial

A step-by-step guide to building a resilient sensor data pipeline with
NATS JetStream, Python asyncio workers, and PostgreSQL.

> This file lives in `docs/TUTORIAL.md`.

---

## What we are building

```
[ProducerWorker A] ──┐
                      ├──> RAW stream (raw.sensor.>)
[ProducerWorker B] ──┘         │
                                ▼
                       [RawStoreWorker]
                       /              \
               raw_sensor_data     RAW_STORED stream (raw_stored.sensor.>)
               (original bytes)           │
                                          ▼
                                  [ProcessorWorker]
                                          │
                                          ▼
                                  PARSED stream (parsed.sensor.>)
                                          │
                                          ▼
                                   [StoreWorker]
                                   /            \
                             sensor_data    NOTIFICATIONS stream
                             (FK: raw_id)         │
                                                  ▼
                                         [NotificationWorker]
                                          (simulates Telegram)
```

Two producers simulate independent temperature sensors (A and B). Every
message is stored to the database **before** any processing begins, giving
you a permanent audit log of the exact bytes the producer sent. The pipeline
can be fully replayed from the raw data if a processing bug is found.

---

## Project structure

```
docs/
    TUTORIAL.md

scripts/
    nats_setup.sh
    postgres_setup.sql

packages/
    shared/
        src/shared/
            __init__.py
            logging.py
            models.py
            py.typed

    workers/
        src/workers/
            core/
                __init__.py     ← re-exports BaseWorker, WorkerHealth
                base.py         ← BaseWorker
                health.py       ← WorkerHealth
            db/
                __init__.py
                database.py
                repository.py
            __init__.py
            main.py
            producer_worker.py
            raw_store_worker.py
            processor_worker.py
            store_worker.py
            notification_worker.py
            py.typed
        pyproject.toml

.vscode/
    launch.json
```

---

## Prerequisites

You are running inside the devcontainer defined in `.devcontainer/`. All tools
are already installed: Python 3.14, uv, nats CLI, psql.

Open the project in VS Code → **Reopen in Container**.

---

## Step 1 — Install dependencies

```bash
uv sync --all-packages --all-groups
```

Dependencies in `packages/workers/pyproject.toml`:

```toml
dependencies = [
    "shared",
    "nats-py>=2.9.0",    # NATS client with JetStream support
    "asyncpg>=0.30.0",   # PostgreSQL async driver
    "uvloop>=0.21.0",    # fast drop-in replacement for asyncio event loop
]
```

`py.typed` in both `shared/` and `workers/` is a PEP 561 marker telling type
checkers (mypy, pyright, Pylance) that these packages ship inline type
annotations and should be fully checked.

---

## Step 2 — Understand the NATS infrastructure

### Streams

A **stream** is a persistent ordered log of messages. It captures all
messages published to matching subjects, whether or not any consumer is
connected. Messages are durable — they survive worker restarts.

This pipeline uses four streams:

```
RAW           raw.sensor.>           producer publishes here
RAW_STORED    raw_stored.sensor.>    raw_store_worker republishes after DB insert
PARSED        parsed.sensor.>        processor_worker publishes normalised data
NOTIFICATIONS notifications.sensor.> store_worker publishes success/failure
```

### Consumers

A **consumer** is a cursor into a stream. It tracks what has been delivered
and acknowledged. The cursor lives **on the server**, not in the Python
process — this is why pull consumers survive TCP reconnections. After
reconnect, the worker calls `fetch()` and the server resumes from the last
acknowledged message.

```
raw_store  → RAW stream        (RawStoreWorker)
processor  → RAW_STORED stream (ProcessorWorker)
store      → PARSED stream     (StoreWorker)
notifier   → NOTIFICATIONS     (NotificationWorker)
```

### Why static consumers?

Consumers are created by a shell script rather than in Python code, keeping
infrastructure separate from application logic. In production this becomes
Kubernetes NATS CRDs — the script is a learning stand-in. The Python workers
bind to existing consumers with `pull_subscribe_bind()`, which raises
immediately at startup if the consumer does not exist.

---

## Step 3 — Set up NATS infrastructure

Inside the devcontainer, NATS is reachable via Docker Compose DNS at
`nats:4222` — not `localhost`.

```bash
NATS_URL=nats://nats:4222 ./scripts/nats_setup.sh
```

**To reset** (streams must be removed one at a time in this CLI version):

```bash
nats --server=nats://nats:4222 stream rm RAW --force
nats --server=nats://nats:4222 stream rm RAW_STORED --force
nats --server=nats://nats:4222 stream rm PARSED --force
nats --server=nats://nats:4222 stream rm NOTIFICATIONS --force
NATS_URL=nats://nats:4222 ./scripts/nats_setup.sh
```

Verify:
```bash
nats --server=nats://nats:4222 stream ls
nats --server=nats://nats:4222 consumer ls RAW
nats --server=nats://nats:4222 consumer ls RAW_STORED
nats --server=nats://nats:4222 consumer ls PARSED
nats --server=nats://nats:4222 consumer ls NOTIFICATIONS
```

You should see four streams and four consumers.

---

## Step 4 — Set up PostgreSQL

```bash
psql -h postgres -U postgres -d postgres -f scripts/postgres_setup.sql
```

Or open pgAdmin at http://localhost:5050 and paste the SQL into the query tool.

### Two tables

```
raw_sensor_data   original producer bytes, written by RawStoreWorker
sensor_data       parsed and typed, written by StoreWorker
```

`sensor_data.raw_id` is a FK to `raw_sensor_data.id`. The FK is satisfied
structurally by the pipeline order — `RawStoreWorker` always writes the raw
row and carries `raw_id` forward before any processing begins. `StoreWorker`
does a plain single INSERT with no transaction wrapper.

### Identity columns

Both tables use the SQL standard identity syntax:

```sql
id  BIGINT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY
```

`GENERATED ALWAYS` rejects manual inserts unless you explicitly use
`OVERRIDING SYSTEM VALUE` — the correct guard for a surrogate key.

---

## Step 5 — Shared models (`shared/models.py`)

Four structs define the message contracts for each pipeline stage:

```python
class RawSensorData(msgspec.Struct, frozen=True, rename=_raw_rename):
    timestamp: int    # "t" on the wire — Unix epoch, raw
    value: float      # "d" on the wire

class RawStoredEvent(msgspec.Struct, frozen=True):
    raw_id: int       # PK from raw_sensor_data
    subject: str      # original NATS subject ("raw.sensor.A")
    payload: str      # original producer JSON byte-for-byte

class SensorData(msgspec.Struct, frozen=True):
    raw_id: int       # FK to raw_sensor_data.id
    sensor_id: str
    timestamp: datetime  # UTC-aware, converted from RawSensorData.timestamp
    value: float

class Notification(msgspec.Struct, frozen=True):
    sensor_id: str
    success: bool
    message: str
```

### Field renaming in RawSensorData

The producer publishes `{"t": ..., "d": ...}` (short IoT keys). Rather than
using those opaque names throughout the codebase, `RawSensorData` declares a
`rename` function:

```python
def _raw_rename(name: str) -> str:
    return {"timestamp": "t", "value": "d"}.get(name, name)
```

msgspec applies this when encoding and decoding, so the Python struct always
uses readable names while the wire format stays compact.

### Why RawSensorData has no sensor_id

`sensor_id` is not in the payload — it lives in the NATS subject
(`raw.sensor.A`). Every worker that needs it extracts it from
`msg.subject.split(".")[-1]` or from `RawStoredEvent.subject`. A single
struct cannot decode the raw payload and carry the subject metadata at the
same time.

### RawStoredEvent — the pipeline glue

This struct is the key design piece. It travels on the `RAW_STORED` stream
and carries everything the downstream pipeline needs:
- `raw_id` — so `StoreWorker` can satisfy the FK without a lookup
- `subject` — so `ProcessorWorker` can extract `sensor_id` from the original subject
- `payload` — the original bytes, so `ProcessorWorker` can decode them as `RawSensorData`

---

## Step 6 — Core module (`workers/core/`)

Infrastructure shared across all workers lives in `core/`, separate from the
domain workers in the root of the package.

```
workers/core/
    __init__.py       ← re-exports BaseWorker and WorkerHealth
    base.py           ← BaseWorker
    health.py         ← WorkerHealth
```

All workers import from the package, not the individual files:

```python
from workers.core import BaseWorker, WorkerHealth
```

### WorkerHealth (`core/health.py`)

Tracks runtime state: `connected`, `last_message_at`, `last_error`.
The `is_healthy` property drives the liveness signal — the heartbeat loop
only touches the health file when `is_healthy` returns `True`. A disconnected
or stalled worker stops touching its file, the Kubernetes probe detects the
stale mtime, and the pod is restarted.

`as_dict()` is ready for a future `/health` HTTP endpoint — no changes needed
to the health logic when that is added.

### BaseWorker (`core/base.py`)

Handles everything shared across all workers: NATS connection, JetStream
context, structured logging, health tracking, and the heartbeat task.

**Single source of truth for worker identity:**

```python
worker_name = type(self).__name__
self._health_file = Path(f"/tmp/worker_health_{worker_name.lower()}")
self.health = WorkerHealth(worker_name=worker_name)
self.logger = structlog.get_logger(worker_name)
```

One variable drives the health file path, health state, and logger name —
no drift possible between them. Each worker gets its own health file:

```
/tmp/worker_health_rawstoreworker
/tmp/worker_health_processorworker
/tmp/worker_health_storeworker
...
```

**Kubernetes liveness probe** (per worker deployment, reference the specific path):

```yaml
livenessProbe:
  exec:
    command: ["find", "/tmp/worker_health_rawstoreworker", "-mmin", "-1"]
```

**Configuration validation at instantiation:**

```python
if bool(self.STREAM) ^ bool(self.CONSUMER):
    raise ValueError(...)  # catches partial config before any network call
```

XOR catches the case where exactly one of `STREAM`/`CONSUMER` is set —
a misconfiguration that would otherwise fail silently or with a confusing
error later at runtime.

**Infinite reconnect:**

```python
max_reconnect_attempts=-1  # default 60 silently kills long-running workers
```

**Three usage patterns — all from one class:**

```
STREAM + CONSUMER set, on_message() implemented   → pure consumer
STREAM + CONSUMER None, run() overridden           → pure producer
STREAM + CONSUMER set, on_message() calls publish  → hybrid
```

---

## Step 7 — Database layer (`workers/db/`)

### `database.py`

```python
server_settings={"timezone": "UTC"}
```

Forces UTC on every connection in the pool. `TIMESTAMPTZ` stores in UTC
internally but PostgreSQL displays in the session timezone, which defaults
to the server's setting. This ensures asyncpg always returns UTC-aware
`datetime` objects regardless of `postgresql.conf`.

### `repository.py` — SensorRepository

Two methods, used by different workers:

```python
async def insert_raw(conn, subject: str, payload: str) -> int:
    # raw_sensor_data — called by RawStoreWorker

async def insert(conn, reading: SensorData) -> int:
    # sensor_data — called by StoreWorker, raw_id already in reading
```

Both receive a `conn` rather than acquiring one — the caller controls the
connection lifecycle.

---

## Step 8 — ProducerWorker (`producer_worker.py`)

Pure producer — `STREAM` and `CONSUMER` left as `None`, `run()` overridden
with a timer loop. `publish()` is inherited from `BaseWorker`.

Publishes `{"t": <epoch>, "d": <float>}` every 2 seconds to `raw.sensor.<id>`.
The sensor ID is in the subject, not the payload.

The optional `stream` parameter on `publish()` confirms routing:

```python
await self.publish(self._subject, json_encode(reading), stream="RAW")
```

If the subject does not match the RAW stream's filter, nats-py raises
immediately — useful for catching misconfiguration early.

In development, `main.py` creates two instances directly with `sensor_id="A"` and `sensor_id="B"`. In production, `sensor_id` comes from the `SENSOR_ID` environment variable — one Kubernetes deployment per sensor, all using `WORKER=producer` with a different `SENSOR_ID`.

---

## Step 9 — RawStoreWorker (`raw_store_worker.py`)

Hybrid worker — set `STREAM="RAW"` and `CONSUMER="raw_store"` to consume,
calls `publish()` inside `on_message()` to republish.

The most important worker for data integrity.

**No parsing.** The raw bytes go straight to `raw_sensor_data` with no
interpretation. A parsing bug in `ProcessorWorker` cannot corrupt this table
because `RawStoreWorker` never calls msgspec decode on the payload content.

**Sequence:**
1. `on_message()` receives message from `RAW` stream
2. `INSERT INTO raw_sensor_data (subject, payload)` → get `raw_id`
3. Publish `RawStoredEvent(raw_id, subject, payload)` to `raw_stored.sensor.<id>`
4. Ack original message **only after both DB write and republish succeed**

**Ack boundary.** If the DB insert succeeds but the republish fails, we nack
the original message. On retry, the raw insert produces a duplicate
`raw_sensor_data` row. This orphan is acceptable — the retry will eventually
succeed and create a properly linked `sensor_data` row.

---

## Step 10 — ProcessorWorker (`processor_worker.py`)

Hybrid worker — consumes from `RAW_STORED`, publishes to `PARSED`.

Decodes in two stages inside `on_message()`:

```python
# Stage 1: decode the RawStoredEvent envelope
event: RawStoredEvent = json_decode(msg.data, type=RawStoredEvent)

# Stage 2: decode the original producer payload inside the envelope
raw: RawSensorData = json_decode(event.payload, type=RawSensorData)
```

Extract `sensor_id` from the **original** subject, not the current one:
```python
sensor_id = event.subject.split(".")[-1]
# event.subject  = "raw.sensor.A"        ← original, correct
# msg.subject    = "raw_stored.sensor.A" ← current stream subject
```

Timestamp conversion happens here — once, at the normalisation boundary:
```python
timestamp=datetime.fromtimestamp(raw.timestamp, tz=timezone.utc)
```

`SensorData` carries `raw_id` from the event — no lookup needed downstream.

---

## Step 11 — StoreWorker (`store_worker.py`)

Hybrid worker — consumes from `PARSED`, publishes to `NOTIFICATIONS`.

Single INSERT inside `on_message()`, no transaction:

```python
async with self._db.acquire() as conn:
    row_id = await self._repo.insert(conn, reading)
# reading.raw_id satisfies the FK — guaranteed by pipeline order
```

Acks even on DB failure — persistent errors would loop forever on nack.
The `Notification` message carries the failure to the notifier.

---

## Step 12 — NotificationWorker (`notification_worker.py`)

Pure consumer — `STREAM` and `CONSUMER` set, no publishing. The simplest
worker in the pipeline: decode, log, ack inside `on_message()`.

```python
icon = "✅" if notif.success else "❌"
self.logger.info("notifier.alert", icon=icon, ...)
```

Replace the `logger.info` call with an `httpx` POST to the Telegram Bot API
when ready. No other changes needed.

---

## Step 13 — main.py

`main.py` serves two modes depending on the `WORKER` environment variable.

**Development mode** (`WORKER` unset) — all workers in one process:

```python
async def _run_all(nats_url, db_url) -> None:
    db = Database(db_url)       # one shared pool for the whole process
    workers = [
        ProducerWorker(nats_url, sensor_id="A"),
        ProducerWorker(nats_url, sensor_id="B"),
        RawStoreWorker(nats_url, db),
        ProcessorWorker(nats_url),
        StoreWorker(nats_url, db),
        NotificationWorker(nats_url),
    ]
    await asyncio.gather(*[w.start() for w in workers])
```

**Production mode** (`WORKER` set) — one worker per container:

```python
async def _run_one(worker: BaseWorker) -> None:
    # signal handling + start + stop for a single worker
```

```python
def _build_worker(name: str, nats_url: str, db_url: str) -> BaseWorker:
    match name:
        case "producer":
            sensor_id = os.environ["SENSOR_ID"]  # required for producer
            return ProducerWorker(nats_url, sensor_id=sensor_id)
        case "raw_store":
            return RawStoreWorker(nats_url, Database(db_url))
        case "processor":
            return ProcessorWorker(nats_url)
        case "store":
            return StoreWorker(nats_url, Database(db_url))
        case "notifier":
            return NotificationWorker(nats_url)
        case _:
            raise ValueError(f"Unknown worker {name!r}")
```

Each production container instantiates its own `Database` — no pool sharing
across containers. The `_` case raises `ValueError` immediately if `WORKER`
is misspelled in a deployment manifest.

**`run()` — the script entrypoint:**

```python
def run() -> None:
    configure_logging()
    uvloop.run(main())
```

`run()` is the symbol registered as `data-pipeline` in `pyproject.toml`:

```toml
[project.scripts]
data-pipeline = "workers.main:run"
```

`uv sync` during the Docker build stage installs this as
`.venv/bin/data-pipeline` — a plain Python shim with no uv dependency.
The runner image uses it as:

```dockerfile
CMD ["data-pipeline"]
```

`uvloop.run()` is scoped to this coroutine and does not mutate the global
event loop policy, unlike `uvloop.install()`.

---

## Step 14 — Run the pipeline

```bash
uv sync --all-packages --all-groups
```

**Development — all workers in one process:**

```bash
# Pretty logs (requires kelora — see docs/useful_commands.md)
uv run --package workers python -u -m workers.main | kelora

# Raw JSON
uv run --package workers python -m workers.main
```

**Production — one worker per container (simulated locally):**

```bash
# Each command simulates one container
WORKER=producer SENSOR_ID=A uv run --package workers data-pipeline
WORKER=producer SENSOR_ID=B uv run --package workers data-pipeline
WORKER=raw_store             uv run --package workers data-pipeline
WORKER=processor             uv run --package workers data-pipeline
WORKER=store                 uv run --package workers data-pipeline
WORKER=notifier              uv run --package workers data-pipeline
```

Every 2 seconds you should see (development mode):
1. `producer.published` × 2 (A and B)
2. `raw_store.received` + `raw_store.persisted` + `raw_store.published` × 2
3. `processor.received` + `processor.published` × 2
4. `store.received` + `store.persisted` × 2
5. `notifier.alert` with ✅ × 2

---

## Step 15 — Observe in NATS

```bash
# Watch each stream in real time
nats --server=nats://nats:4222 sub "raw.>"
nats --server=nats://nats:4222 sub "raw_stored.>"
nats --server=nats://nats:4222 sub "parsed.>"
nats --server=nats://nats:4222 sub "notifications.>"

# Consumer lag — how many messages are pending acknowledgement
nats --server=nats://nats:4222 consumer info RAW raw_store
nats --server=nats://nats:4222 consumer info RAW_STORED processor

# Retrieve a specific message by sequence number
nats --server=nats://nats:4222 stream get RAW 1
nats --server=nats://nats:4222 stream get RAW_STORED 1
```

---

## Step 16 — Observe in PostgreSQL (pgAdmin)

Open http://localhost:5050.

```sql
-- Full picture: original bytes alongside parsed data and pipeline latency
SELECT
    sd.id,
    sd.sensor_id,
    sd.timestamp,
    sd.value,
    rsd.subject             AS raw_subject,
    rsd.payload             AS raw_payload,
    rsd.received_at         AS raw_received_at,
    sd.received_at          AS parsed_received_at,
    sd.received_at - rsd.received_at AS pipeline_latency
FROM sensor_data sd
JOIN raw_sensor_data rsd ON rsd.id = sd.raw_id
ORDER BY sd.received_at DESC
LIMIT 20;

-- Verify raw payload contains original short keys
SELECT payload FROM raw_sensor_data LIMIT 5;
-- Should show: {"t": 1718000000, "d": 23.47}
-- NOT the parsed form with "sensor_id", "timestamp", etc.

-- Count per sensor
SELECT sensor_id, COUNT(*) FROM sensor_data GROUP BY sensor_id;
```

---

## Step 17 — Debug in VS Code

1. Open Run & Debug panel (`Ctrl+Shift+D`) → select **Debug Pipeline**
2. Set a breakpoint in `RawStoreWorker.on_message()`:
   ```python
   raw_id = await self._repo.insert_raw(conn, subject=msg.subject, payload=msg.data.decode())
   ```
   Inspect `msg.subject` and `msg.data` — you will see the raw `{"t":...,"d":...}` bytes.
3. Set a breakpoint in `ProcessorWorker.on_message()`:
   ```python
   raw: RawSensorData = json_decode(event.payload, type=RawSensorData)
   ```
   Inspect `event.raw_id`, `event.subject`, `event.payload`, and `raw` after decode.
4. Step through to inspect `parsed` after the timestamp conversion.

Standard breakpoints work because uvloop runs on the main thread, same as asyncio.

---

## Code quality

Format and lint all files with ruff:

```bash
uv run ruff check --fix .
uv run ruff format .
```

Run `check --fix` before `format` — linting fixes can change code that
formatting then needs to clean up.

---

## Exercises

1. **Verify the audit log is untouched** — query `raw_sensor_data.payload`
   directly. Confirm it contains `{"t":..., "d":...}` (short keys, original
   format) — not the parsed SensorData form. This proves `RawStoreWorker`
   stored the bytes before any processing occurred.

2. **Replay the pipeline** — stop `ProcessorWorker` and `StoreWorker`. Let
   messages accumulate. Check consumer pending counts:
   ```bash
   nats --server=nats://nats:4222 consumer info RAW_STORED processor
   ```
   Restart both workers — they resume from the last acknowledged message.
   Every raw row gets a matching `sensor_data` row.

3. **Introduce a bad message** — publish invalid JSON:
   ```bash
   nats --server=nats://nats:4222 pub raw.sensor.A '{"t": "not-a-number", "d": 25.0}'
   ```
   `RawStoreWorker` stores it without error (no parsing). `ProcessorWorker`
   logs `decode_error.payload` and nacks. The raw row exists in
   `raw_sensor_data` — fix the processor and replay it.

4. **Stop NATS mid-run** — `docker compose stop nats`. Watch
   `nats.disconnected` logs. Restart — `nats.reconnected` appears and the
   pipeline resumes. This is `max_reconnect_attempts=-1` in action.

5. **Add a third sensor** — in development, add `ProducerWorker(nats_url, sensor_id="C")`
   to `_run_all()`. In production, deploy another instance with `WORKER=producer SENSOR_ID=C`.
   No other changes needed — wildcard consumers and subjects handle it automatically.

6. **Verify UTC** — check that all `timestamp` and `received_at` values
   in pgAdmin show `+00` offset. Change the postgres container timezone and
   restart — values remain UTC because of `server_settings={"timezone": "UTC"}`.

7. **Trigger the misconfiguration guard** — create a worker with only
   `STREAM` set and no `CONSUMER`. Observe the `ValueError` raised at
   instantiation before any network call is made.

8. **Test the WORKER dispatcher** — run each worker individually using the
   production mode commands from Step 14. Confirm each starts, subscribes
   or publishes to the correct stream, and that an unknown value:
   ```bash
   WORKER=unknown uv run --package workers data-pipeline
   ```
   raises a `ValueError` immediately with a clear message listing valid values.

9. **Verify SENSOR_ID is required** — run a producer without it:
   ```bash
   WORKER=producer uv run --package workers data-pipeline
   ```
   Confirm it raises `ValueError` before connecting to NATS.
