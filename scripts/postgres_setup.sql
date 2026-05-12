-- =============================================================================
-- postgres_setup.sql
--
-- Run once to create the schema. In the devcontainer:
--   psql -h postgres -U postgres -d postgres -f scripts/postgres_setup.sql
--
-- Or paste into pgAdmin query tool (http://localhost:5050).
--
-- Table naming mirrors the Python structs:
--   raw_sensor_data  ← RawSensorData (immutable audit log, JSONB)
--   sensor_data      ← SensorData    (typed, queryable columns)
-- =============================================================================


-- raw_sensor_data
-- Immutable audit log. Stores the original producer message byte-for-byte,
-- before any parsing or normalisation. Written by RawStoreWorker.
--
-- Because this table is written before any processing happens, it can be
-- used to fully replay the pipeline from scratch if a bug is found in the
-- processing logic. The data here is exactly what the producer published.
CREATE TABLE IF NOT EXISTS raw_sensor_data (
    id          BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subject     VARCHAR(100)    NOT NULL,       -- NATS subject (e.g. "raw.sensor.A")
    payload     JSONB           NOT NULL,       -- original message, e.g. {"t":...,"d":...}
    received_at TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);


-- sensor_data
-- Parsed and validated sensor readings, tied to their raw source via FK.
-- Written by StoreWorker. raw_id references raw_sensor_data.id, which is
-- resolved upstream by RawStoreWorker before any processing occurs.
-- No transaction needed between the two tables — the FK is already satisfied
-- by the time StoreWorker runs.
CREATE TABLE IF NOT EXISTS sensor_data (
    id          BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_id      BIGINT          NOT NULL REFERENCES raw_sensor_data(id),
    sensor_id   VARCHAR(50)     NOT NULL,
    timestamp   TIMESTAMPTZ     NOT NULL,       -- when the sensor took the reading (UTC)
    value       NUMERIC(10, 4)  NOT NULL,
    received_at TIMESTAMPTZ     NOT NULL DEFAULT NOW()  -- when we stored this row (UTC)
);

CREATE INDEX IF NOT EXISTS idx_sensor_data_sensor_ts
    ON sensor_data (sensor_id, timestamp DESC);

-- Verify
SELECT 'raw_sensor_data and sensor_data tables ready' AS status;
