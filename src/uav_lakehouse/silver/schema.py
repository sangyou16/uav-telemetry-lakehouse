"""The contract silver enforces on every bronze line.

Written as DDL strings: easy to read, easy to rewrite from memory.
"""

SUPPORTED_SCHEMA_VERSION = 1

KNOWN_MSG_TYPES = ("heartbeat", "position", "attitude", "battery")

# One message is identified by these three columns.
MESSAGE_KEY = ("aircraft_id", "msg_type", "seq")

# Fields every message must have.
REQUIRED_FIELDS = ("aircraft_id", "msg_type", "seq", "event_ts_ms", "recv_ts_ms")

# Union of all payload fields. Each msg_type fills its own and leaves the rest NULL.
PAYLOAD_DDL = """
    lat_deg DOUBLE, lon_deg DOUBLE, alt_m DOUBLE, ground_speed_mps DOUBLE, heading_deg DOUBLE,
    roll_deg DOUBLE, pitch_deg DOUBLE, yaw_deg DOUBLE,
    voltage_v DOUBLE, current_a DOUBLE, remaining_pct DOUBLE,
    mode STRING, armed BOOLEAN, health STRING
"""

# _corrupt_record: Spark puts the whole line here when it is not valid JSON.
# Checking it explicitly beats guessing from NULL fields.
RAW_DDL = f"""
    schema_version INT, aircraft_id STRING, msg_type STRING, seq BIGINT,
    event_ts_ms BIGINT, recv_ts_ms BIGINT,
    payload STRUCT<{PAYLOAD_DDL}>,
    _corrupt_record STRING
"""
