-- Per-stream rate and latency, from the silver `telemetry` view.
-- (Written for you. Read it: every line is a likely interview question.)
SELECT
    aircraft_id,
    msg_type,
    COUNT(*)                         AS received,
    MIN(event_ts)                    AS first_event_ts,
    MAX(event_ts)                    AS last_event_ts,

    -- N messages span N-1 intervals. NULLIF avoids dividing by zero for a
    -- one-message stream (Spark 4 runs in ANSI mode, where x/0 is an error).
    ROUND(
        (COUNT(*) - 1) / NULLIF((MAX(event_ts_ms) - MIN(event_ts_ms)) / 1000.0, 0),
        3
    )                                AS received_rate_hz,

    -- Percentiles, not averages: one 8-second store-and-forward burst barely
    -- moves the mean but is exactly what p99 is for.
    -- percentile_approx is what scales on big data; on this size it is exact.
    percentile_approx(latency_ms, 0.50) AS latency_p50_ms,
    percentile_approx(latency_ms, 0.95) AS latency_p95_ms,
    percentile_approx(latency_ms, 0.99) AS latency_p99_ms,
    MAX(latency_ms)                     AS latency_max_ms
FROM telemetry
GROUP BY aircraft_id, msg_type
