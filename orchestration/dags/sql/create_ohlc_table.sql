CREATE TABLE IF NOT EXISTS stock_px (
    ts timestamp not null,
    symbol TEXT not null,
    volume DOUBLE PRECISION NOT NULL, 
    open_px DOUBLE PRECISION NOT NULL, 
    high_px DOUBLE PRECISION NOT NULL, 
    low_px DOUBLE PRECISION NOT NULL,
    close_px DOUBLE PRECISION NOT NULL
);

-- SELECT create_hypertable('stock_px', by_range('ts', INTERVAL '1 day'));
SELECT create_hypertable('stock_px', 'ts');
