-- Metadata control table driving the ingestion framework.
-- Lives in the Warehouse (wh_freight), schema etl.
-- The master pipeline reads this with a Lookup activity; adding a new source
-- table to the platform means inserting a row here, not editing the pipeline.

CREATE SCHEMA etl;
GO

CREATE TABLE etl.ingestion_control (
    table_name        VARCHAR(100)  NOT NULL,   -- target table in bronze
    source_folder     VARCHAR(200)  NOT NULL,   -- folder under Files/landing/
    source_format     VARCHAR(20)   NOT NULL,   -- csv | parquet | json
    load_type         VARCHAR(20)   NOT NULL,   -- full | incremental
    watermark_column  VARCHAR(100)  NULL,       -- for incremental loads
    watermark_value   VARCHAR(50)   NULL,       -- last successfully loaded value
    is_active         BIT           NOT NULL,
    priority          INT           NOT NULL,   -- lower loads first (dims before facts)
    notebook_name     VARCHAR(100)  NOT NULL    -- which silver notebook to invoke
);
GO

INSERT INTO etl.ingestion_control VALUES
('dim_carriers',   'carriers',   'csv', 'full',        NULL,        NULL,                  1, 10, 'nb_silver_dims'),
('dim_lanes',      'lanes',      'csv', 'full',        NULL,        NULL,                  1, 10, 'nb_silver_dims'),
('fuel_purchases', 'fuel',       'csv', 'incremental', 'purchase_date', '2025-01-01',      1, 20, 'nb_silver_fuel'),
('shipments',      'shipments',  'csv', 'incremental', 'pickup_ts', '2025-01-01T00:00:00', 1, 30, 'nb_silver_shipments');
GO

-- Called by the pipeline after each successful incremental load
CREATE PROCEDURE etl.update_watermark
    @table_name VARCHAR(100),
    @new_value  VARCHAR(50)
AS
BEGIN
    UPDATE etl.ingestion_control
    SET watermark_value = @new_value
    WHERE table_name = @table_name;
END;
GO

-- Run log written by the pipeline for observability
CREATE TABLE etl.pipeline_run_log (
    run_id        VARCHAR(100),
    pipeline_name VARCHAR(100),
    table_name    VARCHAR(100),
    load_date     DATE,
    rows_loaded   INT,
    status        VARCHAR(20),      -- Succeeded | Failed | DQFailed
    started_utc   DATETIME2(6),
    ended_utc     DATETIME2(6),
    error_message VARCHAR(4000)
);
