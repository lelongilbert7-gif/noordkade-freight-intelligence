CREATE TABLE [etl].[pipeline_run_log] (

	[run_id] varchar(100) NULL, 
	[pipeline_name] varchar(100) NULL, 
	[table_name] varchar(100) NULL, 
	[load_date] date NULL, 
	[rows_loaded] int NULL, 
	[status] varchar(20) NULL, 
	[started_utc] datetime2(6) NULL, 
	[ended_utc] datetime2(6) NULL, 
	[error_message] varchar(4000) NULL
);