CREATE TABLE [etl].[ingestion_control] (

	[table_name] varchar(100) NOT NULL, 
	[source_folder] varchar(200) NOT NULL, 
	[source_format] varchar(20) NOT NULL, 
	[load_type] varchar(20) NOT NULL, 
	[watermark_column] varchar(100) NULL, 
	[watermark_value] varchar(50) NULL, 
	[is_active] bit NOT NULL, 
	[priority] int NOT NULL, 
	[notebook_name] varchar(100) NOT NULL
);