-- ============================================================================
-- EXERCISE: sequence-gap (loss) detection.  Write it yourself.
-- ============================================================================
-- Input: a view named `telemetry` (the silver table). Useful columns:
--     aircraft_id STRING, msg_type STRING, seq BIGINT, event_ts TIMESTAMP, event_ts_ms BIGINT
--   One row per message, no duplicates. seq counts 0,1,2,... per (aircraft_id, msg_type).
--
-- Output: ONE ROW PER GAP (a hole between two messages that did arrive), columns:
--     aircraft_id     STRING
--     msg_type        STRING
--     prev_seq        BIGINT     last seq received before the hole
--     next_seq        BIGINT     first seq received after the hole
--     missing_count   BIGINT     how many seqs are missing in this hole
--     gap_start_ts    TIMESTAMP  event_ts of prev_seq
--     gap_end_ts      TIMESTAMP  event_ts of next_seq
--
-- Example: seqs 7, 8, 12 -> one row: prev_seq 8, next_seq 12, missing_count 3
--
-- Check it:   pytest tests/silver/test_gaps.py
-- The test compares your total missing_count with the generator's ground truth.
--
-- Hints, only if you are stuck (read one at a time):
--   1. A window function can look at the previous row in the same stream.
--   2. Which columns go in PARTITION BY, and which in ORDER BY?
--   3. You cannot filter on a window function in the same SELECT's WHERE.
--      Wrap it: SELECT ... FROM (SELECT ..., LAG(...) OVER (...) AS ...) WHERE ...
--   4. What is missing_count when next_seq - prev_seq = 1?
-- ============================================================================

-- Write your query below this line (lines starting with -- are ignored).
SELECT aircraft_id,
       msg_type,
       prev_seq,
       seq                 AS next_seq,
       seq - prev_seq - 1  AS missing_count,
       prev_event_ts       AS gap_start_ts,
       event_ts            AS gap_end_ts
FROM (
    SELECT aircraft_id, msg_type, seq, event_ts,
           LAG(seq)      OVER (PARTITION BY aircraft_id, msg_type ORDER BY seq) AS prev_seq,
           LAG(event_ts) OVER (PARTITION BY aircraft_id, msg_type ORDER BY seq) AS prev_event_ts
    FROM telemetry
)
WHERE seq - prev_seq > 1
