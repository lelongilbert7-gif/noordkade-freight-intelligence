CREATE PROCEDURE mart.build_carrier_scorecard
    @year_month CHAR(7)
AS
BEGIN
    DELETE FROM mart.carrier_scorecard_monthly WHERE year_month = @year_month;

    INSERT INTO mart.carrier_scorecard_monthly
        (year_month, carrier_id, carrier_name, shipments, total_km,
         total_cost_eur, total_revenue_eur, margin_eur, margin_pct,
         cost_per_km_eur, otd_pct, snapshot_utc)
    SELECT
        @year_month,
        f.carrier_id,
        c.carrier_name,
        COUNT(*),
        SUM(CAST(f.distance_km AS BIGINT)),
        SUM(f.total_cost_eur),
        SUM(f.customer_revenue_eur),
        SUM(f.margin_eur),
        ROUND(SUM(f.margin_eur) / NULLIF(SUM(f.customer_revenue_eur), 0), 3),
        ROUND(SUM(f.total_cost_eur) / NULLIF(SUM(CAST(f.distance_km AS BIGINT)), 0), 3),
        ROUND(AVG(CAST(f.on_time_flag AS DECIMAL(3,1))), 3),
        SYSUTCDATETIME()
    FROM lh_freight.dbo.gold_fact_shipments f
    JOIN lh_freight.dbo.gold_dim_carriers c ON c.carrier_id = f.carrier_id
    WHERE FORMAT(f.pickup_ts, 'yyyy-MM') = @year_month
    GROUP BY f.carrier_id, c.carrier_name;
END;