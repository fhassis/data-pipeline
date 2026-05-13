# Useful Commands

## uv setup and useful commands

Initialize the project:

```bash
uv init --lib packages/shared
uv init --app packages/workers

```

Add the following to the `pyproject.toml` to create a workspace:

```toml
[tool.uv.workspace]
members = ["packages/*"]
```

Create the sub-projects:

> **NOTE**: using `--lib` as it creates the project with a _src_ folder layout.

```bash
uv init --lib packages/shared
uv init --lib packages/workers
```

Add dependencies in the sub-projects:

```bash
cd packages/shared
uv add msgspec
```

Bind the projects (_shared_ into _workers_):

```bash
cd packages/workers
uv add --workspace shared
```

## Pretty printing logs in development

As we are using structured logging in json, to see them nicely formatted in the terminal during development we can use [kelora](https://www.kelora.dev/).

Install it in your machine:

```bash
curl -LO https://github.com/dloss/kelora/releases/latest/download/kelora-x86_64-unknown-linux-musl.tar.gz
tar xzf kelora-x86_64-unknown-linux-musl.tar.gz
sudo mv kelora /usr/local/bin/
```

Now you can do this anywhere:

```bash
uv run --package workers python -u -m workers.main | kelora
```

## NATS — streams, consumers, and messages

> All commands use `NATS_URL=nats://nats:4222` (the Docker service name).
> From outside the container replace `nats` with `localhost`.

```bash
# Shorthand used throughout this section
alias nats='nats --server=nats://nats:4222'
```

### Setup and teardown

```bash
# Create all streams and consumers from scratch
NATS_URL=nats://nats:4222 ./scripts/nats_setup.sh

# Nuke everything and recreate (useful when changing config)
nats stream rm RAW --force
nats stream rm RAW_STORED --force
nats stream rm PARSED --force
nats stream rm NOTIFICATIONS --force
NATS_URL=nats://nats:4222 ./scripts/nats_setup.sh
```

### Inspect streams

```bash
# List all streams with message counts and sizes
nats stream ls

# Full info for one stream (retention policy, subjects, limits…)
nats stream info RAW
nats stream info RAW_STORED
nats stream info PARSED
nats stream info NOTIFICATIONS

# Watch message counts update live (refreshes every second)
nats stream report --watch
```

### Inspect consumers

```bash
# List consumers for each stream (shows pending / ack-pending counts)
nats consumer ls RAW
nats consumer ls RAW_STORED
nats consumer ls PARSED
nats consumer ls NOTIFICATIONS

# Full info for one consumer
nats consumer info RAW raw_store
nats consumer info RAW_STORED processor
nats consumer info PARSED store
nats consumer info NOTIFICATIONS notifier
```

### Subscribe and watch messages live

```bash
# Watch everything published to any subject
nats sub ">"

# Watch only raw sensor messages (both sensors)
nats sub "raw.sensor.>"

# Watch a specific sensor
nats sub "raw.sensor.A"

# Watch the full pipeline in order
nats sub "raw.sensor.>"          # stage 1 — producer output
nats sub "raw_stored.sensor.>"   # stage 2 — after DB insert
nats sub "parsed.sensor.>"       # stage 3 — normalised data
nats sub "notifications.sensor.>" # stage 4 — success/failure

# Replay all messages already in a stream from the beginning
nats sub --stream=RAW --all "raw.sensor.>"
```

### Peek at messages without consuming them

```bash
# Get the first (oldest) undelivered message from a consumer
nats consumer next RAW raw_store --count=1

# Get the last message published on a subject
nats stream get RAW --last-for "raw.sensor.A"

# Get message by sequence number
nats stream get RAW 1
```

### Publish test messages manually

```bash
# Inject one raw reading for sensor A (mirrors the producer format)
nats pub raw.sensor.A '{"t":1718000000,"d":23.47}'

# Inject multiple readings in a loop (10 messages, 1 s apart)
for i in $(seq 1 10); do
  nats pub raw.sensor.A "{\"t\":$(date +%s),\"d\":$((RANDOM % 5000 / 100))}";
  sleep 1;
done
```

### Purge and reset

```bash
# Remove all messages from a stream (consumers keep their positions)
nats stream purge RAW --force

# Purge only messages on a specific subject
nats stream purge RAW --subject "raw.sensor.A" --force

# Delete a single message by sequence number
nats stream rm-msg RAW 42
```

### Server health

> `nats server ping/info/report` require a system account — not configured here.
> Open these URLs directly in the browser (`localhost:8222` is forwarded from the container):

| URL | What it shows |
|---|---|
| http://localhost:8222/healthz | Liveness — `{"status":"ok"}` |
| http://localhost:8222/jsz | JetStream stats (streams, consumers, bytes) |
| http://localhost:8222/jsz?streams=true | Per-stream detail |
| http://localhost:8222/varz | Version, uptime, message counters |
| http://localhost:8222/connz | Active client connections |
