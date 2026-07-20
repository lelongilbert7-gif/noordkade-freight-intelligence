# Semantic model: sm_freight_ops

Direct Lake on `gold_*` tables. Star schema: `gold_fact_shipments` to `gold_dim_date` (date_key), `gold_dim_carriers`, `gold_dim_lanes`. Mark `gold_dim_date` as date table.

## Base measures

```dax
Shipments = COUNTROWS ( gold_fact_shipments )

Total Revenue = SUM ( gold_fact_shipments[customer_revenue_eur] )

Total Cost = SUM ( gold_fact_shipments[total_cost_eur] )

Margin = [Total Revenue] - [Total Cost]

Margin % = DIVIDE ( [Margin], [Total Revenue] )

Total KM = SUM ( gold_fact_shipments[distance_km] )

Cost per KM =
DIVIDE ( [Total Cost], [Total KM] )

OTD % =
DIVIDE (
    CALCULATE ( [Shipments], gold_fact_shipments[on_time_flag] = TRUE () ),
    [Shipments]
)

Avg Transit Hours = AVERAGE ( gold_fact_shipments[transit_hours] )

Revenue per Tonne =
DIVIDE ( [Total Revenue], SUM ( gold_fact_shipments[weight_kg] ) / 1000 )
```

## Calculation group: Time Intelligence

One calculation group replaces what would otherwise be 40+ duplicated measures (8 base measures x 5 time flavors). Create in Tabular Editor or the TMDL view.

```
calculationGroup 'Time Intelligence'
    precedence: 10

    calculationItem 'Current' = SELECTEDMEASURE ()

    calculationItem 'MTD' =
        CALCULATE ( SELECTEDMEASURE (), DATESMTD ( gold_dim_date[date] ) )

    calculationItem 'YTD' =
        CALCULATE ( SELECTEDMEASURE (), DATESYTD ( gold_dim_date[date] ) )

    calculationItem 'PY' =
        CALCULATE ( SELECTEDMEASURE (), SAMEPERIODLASTYEAR ( gold_dim_date[date] ) )

    calculationItem 'YoY %' =
        VAR Cur = SELECTEDMEASURE ()
        VAR Prev =
            CALCULATE ( SELECTEDMEASURE (), SAMEPERIODLASTYEAR ( gold_dim_date[date] ) )
        RETURN DIVIDE ( Cur - Prev, Prev )
        formatString: "0.0%"
```

## Row-level security

Two roles, demonstrating both static and dynamic patterns:

```dax
-- Role: Regional Manager (dynamic, via a bridge table of user->origin cities)
'gold_dim_lanes'[origin_city] IN
    CALCULATETABLE (
        VALUES ( security_user_region[origin_city] ),
        security_user_region[upn] = USERPRINCIPALNAME ()
    )

-- Role: Carrier Portal (static per-carrier, for external sharing demos)
'gold_dim_carriers'[carrier_id] = "CAR001"
```

Test with "View as role" and document the OLS consideration: the carrier role must not see `mart.carrier_scorecard_monthly` margin columns, handled with object-level security on the margin measures.

## Field parameters

One field parameter `Analyze By` over {carrier_name, origin_city, dest_city, service_type} so a single visual serves four analysis paths, and one `Metric Selector` over {Margin %, OTD %, Cost per KM} for the exec summary tile.

## Report pages (executive report)

1. Network overview: KPI band (Revenue, Margin %, OTD %, Cost per KM with YoY from the calc group), monthly trend, map of lane volumes.
2. Carrier scorecard: matrix vs mart.carrier_scorecard_monthly, conditional formatting on OTD and margin decile, drill-through to shipment detail.
3. Lane profitability: scatter of margin % vs volume, reprice_flag highlighted, decomposition tree on Margin.
4. Ops health: quarantine trend from silver_quarantine_shipments, DQ pass history, pipeline run log table. A report page about the platform itself signals engineering maturity.
