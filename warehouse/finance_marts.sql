-- Warehouse: wh_freight
-- Finance marts built with T-SQL on top of gold lakehouse tables via
-- cross-database three-part naming (lh_freight.dbo.gold_*).
-- Rationale in DECISIONS.md: the finance persona works in T-SQL, and the
-- monthly close needs stable snapshot tables, not live Direct Lake views.

CREATE SCHEMA mart;
GO

-- Monthly carrier scorecard: the table procurement uses in rate negotiations
CREATE TABLE mart.carrier_scorecard_monthly (
    year_month        CHAR(7)      NOT NULL,
    carrier_id        VARCHAR(10)  NOT NULL,
    carrier_name      VARCHAR(100) NOT NULL,
    shipments         INT,
    total_km          BIGINT,
    total_cost_eur    DECIMAL(14,2),
    total_revenue_eur DECIMAL(14,2),
    margin_eur        DECIMAL(14,2),
    margin_pct        DECIMAL(6,3),
    cost_per_km_eur   DECIMAL(8,3),
    otd_pct           DECIMAL(6,3),
    snapshot_utc      DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

CREATE PROCEDURE mart.build_carrier_scorecard
    @year_month CHAR(7)          -- e.g. '2026-06'
AS
BEGIN
    DELETE FROM mart.carrier_scorecard_monthly WHERE year_month = @year_month;

    INSERT INTO mart.carrier_scorecard_monthly
        (year_month, carrier_id, carrier_name, shipments, total_km,
         total_cost_eur, total_revenue_eur, margin_eur, margin_pct,
         cost_per_km_eur, otd_pct)
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
        ROUND(AVG(CAST(f.on_time_flag AS DECIMAL(3,1))), 3)
    FROM lh_freight.dbo.gold_fact_shipments f
    JOIN lh_freight.dbo.gold_dim_carriers c ON c.carrier_id = f.carrier_id
    WHERE FORMAT(f.pickup_ts, 'yyyy-MM') = @year_month
    GROUP BY f.carrier_id, c.carrier_name;
END;
GO

-- Lane profitability with window functions: rank lanes and flag the
-- bottom decile for the commercial team to reprice
CREATE VIEW mart.v_lane_profitability AS
WITH lane_agg AS (
    SELECT
        f.lane_id,
        l.origin_city, l.dest_city,
        COUNT(*)                    AS shipments,
        SUM(f.margin_eur)           AS margin_eur,
        SUM(f.margin_eur) / NULLIF(SUM(f.customer_revenue_eur), 0) AS margin_pct,
        AVG(f.cost_per_km_eur)      AS avg_cost_per_km
    FROM lh_freight.dbo.gold_fact_shipments f
    JOIN lh_freight.dbo.gold_dim_lanes l ON l.lane_id = f.lane_id
    GROUP BY f.lane_id, l.origin_city, l.dest_city
)
SELECT *,
    RANK()  OVER (ORDER BY margin_pct DESC)              AS margin_rank,
    NTILE(10) OVER (ORDER BY margin_pct DESC)            AS margin_decile,
    CASE WHEN NTILE(10) OVER (ORDER BY margin_pct DESC) = 10
         THEN 1 ELSE 0 END                               AS reprice_flag
FROM lane_agg;
GO
