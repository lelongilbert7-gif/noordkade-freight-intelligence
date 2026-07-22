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