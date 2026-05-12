#!/usr/bin/env bash
# =============================================================================
# nats_setup.sh
#
# Creates all JetStream streams and durable consumers for the pipeline.
#
# Run once before starting workers:
#   chmod +x scripts/nats_setup.sh
#   NATS_URL=nats://nats:4222 ./scripts/nats_setup.sh
#
# To reset everything and start fresh (remove one stream at a time):
#   nats --server=nats://nats:4222 stream rm RAW --force
#   nats --server=nats://nats:4222 stream rm RAW_STORED --force
#   nats --server=nats://nats:4222 stream rm PARSED --force
#   nats --server=nats://nats:4222 stream rm NOTIFICATIONS --force
#   NATS_URL=nats://nats:4222 ./scripts/nats_setup.sh
# =============================================================================

set -euo pipefail

NATS_URL="${NATS_URL:-nats://nats:4222}"
NATS="nats --server=$NATS_URL"

echo ">>> Connecting to NATS at $NATS_URL"

# =============================================================================
# STREAMS
#
# Pipeline flow:
#   raw.sensor.>       — producer publishes here
#   raw_stored.sensor.> — raw_store_worker republishes after DB insert (with raw_id)
#   parsed.sensor.>    — processor_worker publishes normalised SensorData
#   notifications.sensor.> — store_worker publishes success/failure outcome
# =============================================================================

echo ""
echo ">>> Creating stream: RAW"
$NATS stream add RAW \
  --subjects="raw.>" \
  --storage=file \
  --replicas=1 \
  --retention=limits \
  --discard=old \
  --max-age=1h \
  --dupe-window=2m \
  --defaults 2>/dev/null || echo "    (already exists, skipping)"

echo ">>> Creating stream: RAW_STORED"
$NATS stream add RAW_STORED \
  --subjects="raw_stored.>" \
  --storage=file \
  --replicas=1 \
  --retention=limits \
  --discard=old \
  --max-age=1h \
  --dupe-window=2m \
  --defaults 2>/dev/null || echo "    (already exists, skipping)"

echo ">>> Creating stream: PARSED"
$NATS stream add PARSED \
  --subjects="parsed.>" \
  --storage=file \
  --replicas=1 \
  --retention=limits \
  --discard=old \
  --max-age=1h \
  --dupe-window=2m \
  --defaults 2>/dev/null || echo "    (already exists, skipping)"

echo ">>> Creating stream: NOTIFICATIONS"
$NATS stream add NOTIFICATIONS \
  --subjects="notifications.>" \
  --storage=file \
  --replicas=1 \
  --retention=limits \
  --discard=old \
  --max-age=1h \
  --dupe-window=2m \
  --defaults 2>/dev/null || echo "    (already exists, skipping)"

# =============================================================================
# CONSUMERS
#
# One durable pull consumer per worker type.
# The consumer name IS the durable name (second positional argument).
#
# raw_store  → RAW stream        (RawStoreWorker)
# processor  → RAW_STORED stream (ProcessorWorker)
# store      → PARSED stream     (StoreWorker)
# notifier   → NOTIFICATIONS     (NotificationWorker)
# =============================================================================

echo ""
echo ">>> Creating consumer: raw_store on stream RAW"
$NATS consumer add RAW raw_store \
  --pull \
  --deliver=all \
  --ack=explicit \
  --replay=instant \
  --max-deliver=5 \
  --defaults 2>/dev/null || echo "    (already exists, skipping)"

echo ">>> Creating consumer: processor on stream RAW_STORED"
$NATS consumer add RAW_STORED processor \
  --pull \
  --deliver=all \
  --ack=explicit \
  --replay=instant \
  --max-deliver=5 \
  --defaults 2>/dev/null || echo "    (already exists, skipping)"

echo ">>> Creating consumer: store on stream PARSED"
$NATS consumer add PARSED store \
  --pull \
  --deliver=all \
  --ack=explicit \
  --replay=instant \
  --max-deliver=5 \
  --defaults 2>/dev/null || echo "    (already exists, skipping)"

echo ">>> Creating consumer: notifier on stream NOTIFICATIONS"
$NATS consumer add NOTIFICATIONS notifier \
  --pull \
  --deliver=all \
  --ack=explicit \
  --replay=instant \
  --max-deliver=5 \
  --defaults 2>/dev/null || echo "    (already exists, skipping)"

# =============================================================================
# Verify
# =============================================================================

echo ""
echo ">>> Stream summary:"
$NATS stream ls

echo ""
echo ">>> Consumer summary:"
$NATS consumer ls RAW
$NATS consumer ls RAW_STORED
$NATS consumer ls PARSED
$NATS consumer ls NOTIFICATIONS

echo ""
echo ">>> Done. Infrastructure is ready."
