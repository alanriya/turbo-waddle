import pyarrow as pa

schema = pa.schema([
    ('symbol', pa.string()),
    ('last_px', pa.float64()),
    ('timestamp', pa.string()),
    ('volume', pa.float64()),
    ('condition', pa.string()),
    ('message_type', pa.string()),
    ('sys_timestamp', pa.string())
])