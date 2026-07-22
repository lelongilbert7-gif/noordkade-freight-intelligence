-- Auto Generated (Do not modify) 776C95CFD4A1606C71342B27B67E1BB6C1186F902E3418B0C0E877B0042F026D


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