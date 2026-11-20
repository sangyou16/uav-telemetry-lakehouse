-- ============================================================================
-- EXERCISE: loss % per stream.  Write it yourself.
-- ============================================================================
-- Inputs (two views):
--   telemetry   one row per received message: aircraft_id, msg_type, seq, ...
--   gaps        your silver gap table: aircraft_id, msg_type, missing_count, ...
--               (one row per gap; a stream with no loss has NO rows here)
--
-- Output: ONE ROW PER STREAM (aircraft_id, msg_type), columns:
--     aircraft_id   STRING
--     msg_type      STRING
--     received      BIGINT   messages received in this stream
--     missing       BIGINT   total missing_count of its gaps, 0 if it has none
--     loss_pct      number   100 * missing / (received + missing), rounded to 3 decimals
--
-- Example: received 990, missing 10  ->  loss_pct = 100 * 10 / 1000 = 1.0
--
-- Check it:   pytest tests/gold/test_loss.py --tb=line -q
--
-- Hints (one at a time):
--   1. Build two small results first: received per stream, missing per stream.
--      Then join them. Each is a GROUP BY.
--   2. Some streams have no gaps. Which JOIN keeps them?
--   3. A stream with no gaps gets NULL for missing after that join.
--      Which function turns NULL into 0?
--   4. Why aggregate BEFORE joining? Think what happens to COUNT(*) if you
--      join telemetry (thousands of rows) to gaps (several rows) first.
--   5. Structure:  WITH r AS (...), m AS (...) SELECT ... FROM r LEFT JOIN m ...
-- ============================================================================

-- Write your query below this line (lines starting with -- are ignored).
WITH r AS (
    SELECT aircraft_id, msg_type, COUNT(*) AS received
    FROM telemetry
    GROUP BY aircraft_id, msg_type
),
m AS (
    SELECT aircraft_id, msg_type, SUM(missing_count) AS missing
    FROM gaps
    GROUP BY aircraft_id, msg_type
)
SELECT r.aircraft_id,
       r.msg_type,
       r.received,
       COALESCE(m.missing, 0)                                             AS missing,
       ROUND(100.0 * COALESCE(m.missing, 0)
             / (r.received + COALESCE(m.missing, 0)), 3)                  AS loss_pct
FROM r
LEFT JOIN m
  ON r.aircraft_id = m.aircraft_id AND r.msg_type = m.msg_type
