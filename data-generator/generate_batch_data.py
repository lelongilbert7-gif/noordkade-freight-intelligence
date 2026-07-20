"""
Freight Operations Intelligence Platform
Noordkade Freight synthetic data generator: carriers, lanes, shipments, fuel purchases.

Run locally, then upload the CSVs to the Lakehouse Files/landing/ area,
partitioned by date folder (yyyy/MM/dd) to match the parameterized pipeline paths.

Volumes are tuned for a Fabric trial capacity:
  ~1.2M shipment legs across 18 months, ~40 carriers, ~120 lanes.
"""

import numpy as np
import pandas as pd
from datetime import date
from pathlib import Path

RNG = np.random.default_rng(42)
OUT = Path("output")
START = pd.Timestamp("2025-01-01")
END = pd.Timestamp("2026-06-30")
N_SHIPMENTS = 1_200_000

CITIES = {
    "Rotterdam": (51.92, 4.48), "Amsterdam": (52.37, 4.90), "Antwerp": (51.22, 4.40),
    "Brussels": (50.85, 4.35), "Duisburg": (51.43, 6.76), "Cologne": (50.94, 6.96),
    "Hamburg": (53.55, 9.99), "Paris": (48.86, 2.35), "Lyon": (45.76, 4.84),
    "Frankfurt": (50.11, 8.68), "Munich": (48.14, 11.58), "Milan": (45.46, 9.19),
    "Venlo": (51.37, 6.17), "Eindhoven": (51.44, 5.47), "Liege": (50.63, 5.57),
    "Utrecht": (52.09, 5.12),
}

SERVICE_TYPES = ["FTL", "LTL", "Reefer", "Container drayage"]
SERVICE_WEIGHTS = [0.45, 0.30, 0.15, 0.10]


def haversine_km(a, b):
    lat1, lon1, lat2, lon2 = map(np.radians, [a[0], a[1], b[0], b[1]])
    h = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371 * 2 * np.arcsin(np.sqrt(h))


def make_carriers(n=40):
    prefixes = ["Trans", "Euro", "Delta", "Nova", "Rhine", "Polder", "Alpine", "Coast", "Vector", "Meridian"]
    suffixes = ["Logistics", "Freight", "Cargo", "Haulage", "Transport", "Lines"]
    names = []
    while len(names) < n:
        nm = f"{RNG.choice(prefixes)} {RNG.choice(suffixes)} {RNG.integers(1, 99)}"
        if nm not in names:
            names.append(nm)
    return pd.DataFrame({
        "carrier_id": [f"CAR{str(i+1).zfill(3)}" for i in range(n)],
        "carrier_name": names,
        "home_country": RNG.choice(["NL", "BE", "DE", "FR", "PL"], n, p=[0.35, 0.15, 0.25, 0.10, 0.15]),
        "fleet_size": RNG.integers(5, 220, n),
        "safety_rating": np.round(RNG.uniform(2.8, 5.0, n), 1),
        "contracted_rate_per_km_eur": np.round(RNG.uniform(1.05, 1.75, n), 2),
    })


def make_lanes():
    rows = []
    city_names = list(CITIES)
    lane_id = 1
    for o in city_names:
        for d in city_names:
            if o == d:
                continue
            km = haversine_km(CITIES[o], CITIES[d]) * 1.28  # road factor
            if km < 60:
                continue
            rows.append({
                "lane_id": f"LN{str(lane_id).zfill(4)}",
                "origin_city": o, "dest_city": d,
                "distance_km": round(km),
                "std_transit_hours": round(km / 62 + RNG.uniform(1, 4), 1),
            })
            lane_id += 1
    lanes = pd.DataFrame(rows)
    # Keep the ~120 busiest plausible lanes, biased toward port origins
    port_bias = lanes["origin_city"].isin(["Rotterdam", "Antwerp", "Hamburg"]).astype(int)
    lanes["demand_weight"] = RNG.uniform(0.2, 1.0, len(lanes)) + port_bias * 0.9
    return lanes.nlargest(120, "demand_weight").reset_index(drop=True)


def make_shipments(carriers, lanes):
    n = N_SHIPMENTS
    lane_idx = RNG.choice(len(lanes), n, p=lanes["demand_weight"] / lanes["demand_weight"].sum())
    carrier_idx = RNG.choice(len(carriers), n)
    lane = lanes.iloc[lane_idx].reset_index(drop=True)
    car = carriers.iloc[carrier_idx].reset_index(drop=True)

    days = (END - START).days
    pickup_offset = RNG.integers(0, days, n)
    # Seasonality: Q4 peak, August dip
    pickup = START + pd.to_timedelta(pickup_offset, unit="D") + pd.to_timedelta(RNG.integers(5, 21, n), unit="h")
    month = pickup.month
    keep = RNG.uniform(0, 1, n) < np.where(np.isin(month, [10, 11, 12]), 1.0, np.where(month == 8, 0.62, 0.85))

    service = RNG.choice(SERVICE_TYPES, n, p=SERVICE_WEIGHTS)
    # Delay model: carrier safety rating and reefer complexity drive lateness
    base_delay = RNG.gamma(2.0, 1.6, n)  # hours
    delay = base_delay * (5.2 - car["safety_rating"].to_numpy()) / 1.6
    delay = delay + np.where(service == "Reefer", RNG.gamma(1.5, 1.2, n), 0)
    on_time = delay <= 2.0

    transit_actual = lane["std_transit_hours"].to_numpy() + delay - RNG.uniform(0, 1.5, n)
    weight_kg = np.where(service == "LTL", RNG.integers(200, 8000, n), RNG.integers(8000, 24000, n))
    fuel_surcharge = np.round(RNG.uniform(0.06, 0.14, n), 3)
    linehaul = lane["distance_km"].to_numpy() * car["contracted_rate_per_km_eur"].to_numpy()
    revenue = np.round(linehaul * (1 + fuel_surcharge) * RNG.uniform(1.12, 1.30, n), 2)

    df = pd.DataFrame({
        "shipment_id": [f"SHP{str(i+1).zfill(8)}" for i in range(n)],
        "lane_id": lane["lane_id"], "carrier_id": car["carrier_id"],
        "service_type": service,
        "pickup_ts": pickup, "delivery_ts": pickup + pd.to_timedelta(transit_actual, unit="h"),
        "weight_kg": weight_kg,
        "linehaul_cost_eur": np.round(linehaul, 2),
        "fuel_surcharge_pct": fuel_surcharge,
        "customer_revenue_eur": revenue,
        "on_time_flag": on_time,
        "status": RNG.choice(["Delivered", "Delivered", "Delivered", "Cancelled"], n, p=[0.55, 0.25, 0.17, 0.03]),
    })[keep]
    # Deliberate data quality issues for the DQ notebook to catch (~0.4%)
    dirty = RNG.choice(df.index, int(len(df) * 0.004), replace=False)
    df.loc[dirty[: len(dirty) // 2], "weight_kg"] = -1
    df.loc[dirty[len(dirty) // 2:], "carrier_id"] = "CAR999"
    return df.reset_index(drop=True)


def make_fuel(carriers):
    n = 90_000
    ts = START + pd.to_timedelta(RNG.integers(0, (END - START).days, n), unit="D")
    return pd.DataFrame({
        "purchase_id": [f"FUE{str(i+1).zfill(7)}" for i in range(n)],
        "carrier_id": RNG.choice(carriers["carrier_id"], n),
        "purchase_date": ts.date,
        "litres": RNG.integers(120, 900, n),
        "price_per_litre_eur": np.round(1.52 + 0.22 * np.sin(2 * np.pi * pd.Series(ts).dt.dayofyear / 365) + RNG.normal(0, 0.05, n), 3),
        "country": RNG.choice(["NL", "BE", "DE", "FR"], n),
    })


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    carriers, lanes = make_carriers(), make_lanes()
    shipments = make_shipments(carriers, lanes)
    fuel = make_fuel(carriers)

    carriers.to_csv(OUT / "dim_carriers.csv", index=False)
    lanes.drop(columns="demand_weight").to_csv(OUT / "dim_lanes.csv", index=False)
    fuel.to_csv(OUT / "fuel_purchases.csv", index=False)

    # Partition shipments by pickup date to match the parameterized landing path
    shipments["pdate"] = shipments["pickup_ts"].dt.date
    for d, grp in shipments.groupby("pdate"):
        p = OUT / "shipments" / f"{d:%Y/%m/%d}"
        p.mkdir(parents=True, exist_ok=True)
        grp.drop(columns="pdate").to_csv(p / "shipments.csv", index=False)

    print(f"Shipments: {len(shipments):,} | Carriers: {len(carriers)} | Lanes: {len(lanes)} | Fuel: {len(fuel):,}")
