CREATE TABLE [mart].[carrier_scorecard_monthly] (

	[year_month] char(7) NOT NULL, 
	[carrier_id] varchar(10) NOT NULL, 
	[carrier_name] varchar(100) NOT NULL, 
	[shipments] int NULL, 
	[total_km] bigint NULL, 
	[total_cost_eur] decimal(14,2) NULL, 
	[total_revenue_eur] decimal(14,2) NULL, 
	[margin_eur] decimal(14,2) NULL, 
	[margin_pct] decimal(6,3) NULL, 
	[cost_per_km_eur] decimal(8,3) NULL, 
	[otd_pct] decimal(6,3) NULL, 
	[snapshot_utc] datetime2(6) NOT NULL
);