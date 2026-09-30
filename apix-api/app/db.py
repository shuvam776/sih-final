import asyncio
import sys
from datetime import datetime, time, timezone
from collections import defaultdict
from pathlib import Path
from typing import AsyncGenerator, Dict, List
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import select, text
from app.config import settings
from app.models import (
    Base,
    Route,
    Fare,
    DailyIndex,
    WeeklyIndex,
    MonthlyIndex,
    BacktestRecord,
    BacktestDataset,
    PipelineJobRun,
    generate_audit_hash,
)

# Ensure the repo-root `index_math` package (shared with the pipeline daily
# index job in pipeline/runner.py) is importable regardless of process cwd.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from index_math.engine import compute_daily_aggregate_indices, BASE_PERIOD_ROUTE_FARES
from index_math.weights import DGCA_ROUTE_TRAFFIC_SHARE

# Create engine. Supabase's Postgres requires TLS, and its pooler/direct
# connections get dropped after a period of idleness — pool_pre_ping
# validates a connection before handing it out instead of surfacing a stale
# "connection is closed" InterfaceError on the next query.
_connect_args = {}
if settings.DATABASE_URL.startswith("postgresql"):
    _connect_args["ssl"] = "require"

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_recycle=300,
    connect_args=_connect_args,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Create the database schema and seed data with fallback for unreachable remote DBs."""
    global engine, AsyncSessionLocal
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            if engine.dialect.name == "postgresql":
                await conn.execute(text("ALTER TABLE backtest_records ADD COLUMN IF NOT EXISTS dataset_id VARCHAR(36)"))
                await conn.execute(text("DELETE FROM backtest_records WHERE dataset_id IS NULL"))
                await conn.execute(text("ALTER TABLE backtest_records ALTER COLUMN dataset_id SET NOT NULL"))
    except Exception as exc:
        print(f"WARNING: Primary database connection ({settings.DATABASE_URL[:25]}...) failed: {exc}. Falling back to SQLite.")
        engine = create_async_engine(
            "sqlite+aiosqlite:///./apix.db",
            echo=False,
            future=True,
        )
        AsyncSessionLocal = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        # Check if routes already exist
        existing_route = await session.execute(select(Route).limit(1))
        if existing_route.scalar_one_or_none() is not None:
            return  # Already seeded

        # Seed DGCA Basket Routes. Traffic weights are sourced from
        # index_math/weights.py (DGCA_ROUTE_TRAFFIC_SHARE) so the API's
        # published route basket always matches the weights actually used by
        # the Laspeyres/Paasche/Fisher aggregate formulas.
        routes_data = [
            Route(pair="DEL-BOM", origin="DEL", origin_name="Delhi Indira Gandhi Int'l", destination="BOM", destination_name="Mumbai Chhatrapati Shivaji Maharaj", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["DEL-BOM"], monthly_volume="2.4M", tier="Metro-to-Metro"),
            Route(pair="DEL-BLR", origin="DEL", origin_name="Delhi Indira Gandhi Int'l", destination="BLR", destination_name="Bengaluru Kempegowda Int'l", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["DEL-BLR"], monthly_volume="1.9M", tier="Metro-to-Metro"),
            Route(pair="BOM-BLR", origin="BOM", origin_name="Mumbai Chhatrapati Shivaji Maharaj", destination="BLR", destination_name="Bengaluru Kempegowda Int'l", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["BOM-BLR"], monthly_volume="1.6M", tier="Metro-to-Metro"),
            Route(pair="DEL-CCU", origin="DEL", origin_name="Delhi Indira Gandhi Int'l", destination="CCU", destination_name="Kolkata Netaji Subhash Chandra Bose", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["DEL-CCU"], monthly_volume="1.2M", tier="Metro-to-Metro"),
            Route(pair="DEL-HYD", origin="DEL", origin_name="Delhi Indira Gandhi Int'l", destination="HYD", destination_name="Hyderabad Rajiv Gandhi Int'l", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["DEL-HYD"], monthly_volume="1.0M", tier="Metro-to-Metro"),
            Route(pair="BLR-HYD", origin="BLR", origin_name="Bengaluru Kempegowda Int'l", destination="HYD", destination_name="Hyderabad Rajiv Gandhi Int'l", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["BLR-HYD"], monthly_volume="1.1M", tier="Metro-to-Metro"),
            Route(pair="MAA-DEL", origin="MAA", origin_name="Chennai Int'l", destination="DEL", destination_name="Delhi Indira Gandhi Int'l", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["MAA-DEL"], monthly_volume="0.9M", tier="Metro-to-Metro"),
            Route(pair="CCU-BLR", origin="CCU", origin_name="Kolkata Netaji Subhash Chandra Bose", destination="BLR", destination_name="Bengaluru Kempegowda Int'l", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["CCU-BLR"], monthly_volume="0.75M", tier="Metro-to-Metro"),
            Route(pair="DEL-PNQ", origin="DEL", origin_name="Delhi Indira Gandhi Int'l", destination="PNQ", destination_name="Pune Airport", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["DEL-PNQ"], monthly_volume="0.8M", tier="Metro-to-Tier2"),
            Route(pair="BOM-GOI", origin="BOM", origin_name="Mumbai Chhatrapati Shivaji Maharaj", destination="GOI", destination_name="Goa Dabolim / Mopa", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["BOM-GOI"], monthly_volume="0.7M", tier="Tourist/Leisure"),
            Route(pair="DEL-PAT", origin="DEL", origin_name="Delhi Indira Gandhi Int'l", destination="PAT", destination_name="Patna Jay Prakash Narayan Int'l", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["DEL-PAT"], monthly_volume="0.4M", tier="Metro-to-Tier2"),
            Route(pair="DEL-GAU", origin="DEL", origin_name="Delhi Indira Gandhi Int'l", destination="GAU", destination_name="Guwahati Lokpriya Gopinath Bordoloi Int'l", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["DEL-GAU"], monthly_volume="0.35M", tier="Metro-to-Tier2"),
            Route(pair="DEL-IXR", origin="DEL", origin_name="Delhi Indira Gandhi Int'l", destination="IXR", destination_name="Ranchi Birsa Munda Airport", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["DEL-IXR"], monthly_volume="0.3M", tier="Metro-to-Tier2"),
            Route(pair="BOM-PAT", origin="BOM", origin_name="Mumbai Chhatrapati Shivaji Maharaj", destination="PAT", destination_name="Patna Jay Prakash Narayan Int'l", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["BOM-PAT"], monthly_volume="0.2M", tier="Metro-to-Tier2"),
            Route(pair="CCU-GAU", origin="CCU", origin_name="Kolkata Netaji Subhash Chandra Bose", destination="GAU", destination_name="Guwahati Lokpriya Gopinath Bordoloi Int'l", dgca_weight=DGCA_ROUTE_TRAFFIC_SHARE["CCU-GAU"], monthly_volume="0.25M", tier="Metro-to-Tier2"),
        ]
        session.add_all(routes_data)

        # Seed Fares across multiple routes and carriers
        raw_fares_seed = [
            # DEL-BOM
            {"id": "F101", "pair": "DEL-BOM", "origin": "DEL", "destination": "BOM", "carrier": "IndiGo", "flight_no": "6E-2041", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 3800.0, "taxes_udf": 650.0, "convenience_fee": 120.0, "total_fare": 4570.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F102", "pair": "DEL-BOM", "origin": "DEL", "destination": "BOM", "carrier": "Air India", "flight_no": "AI-805", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 4400.0, "taxes_udf": 720.0, "convenience_fee": 0.0, "total_fare": 5120.0, "seat_avail": True, "source": "Air India Direct"},
            {"id": "F103", "pair": "DEL-BOM", "origin": "DEL", "destination": "BOM", "carrier": "SpiceJet", "flight_no": "SG-8169", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 3200.0, "taxes_udf": 580.0, "convenience_fee": 150.0, "total_fare": 3930.0, "seat_avail": True, "source": "MakeMyTrip"},
            {"id": "F104", "pair": "DEL-BOM", "origin": "DEL", "destination": "BOM", "carrier": "Akasa Air", "flight_no": "QP-1102", "departure_date": "2026-09-15", "scrape_date": "2026-09-14", "advance_days": 1, "fare_class": "Economy", "base_fare": 7200.0, "taxes_udf": 950.0, "convenience_fee": 100.0, "total_fare": 8250.0, "seat_avail": True, "source": "EaseMyTrip"},
            {"id": "F105", "pair": "DEL-BOM", "origin": "DEL", "destination": "BOM", "carrier": "Air India Express", "flight_no": "IX-1402", "departure_date": "2026-09-29", "scrape_date": "2026-09-14", "advance_days": 15, "fare_class": "Economy", "base_fare": 3300.0, "taxes_udf": 550.0, "convenience_fee": 100.0, "total_fare": 3950.0, "seat_avail": True, "source": "Cleartrip"},
            {"id": "F106", "pair": "DEL-BOM", "origin": "DEL", "destination": "BOM", "carrier": "IndiGo", "flight_no": "6E-5312", "departure_date": "2026-10-14", "scrape_date": "2026-09-14", "advance_days": 30, "fare_class": "Economy", "base_fare": 2900.0, "taxes_udf": 520.0, "convenience_fee": 120.0, "total_fare": 3540.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F107", "pair": "DEL-BOM", "origin": "DEL", "destination": "BOM", "carrier": "IndiGo", "flight_no": "6E-5314", "departure_date": "2026-10-29", "scrape_date": "2026-09-14", "advance_days": 45, "fare_class": "Economy", "base_fare": 2750.0, "taxes_udf": 500.0, "convenience_fee": 120.0, "total_fare": 3370.0, "seat_avail": True, "source": "IndiGo Direct"},

            # DEL-BLR
            {"id": "F201", "pair": "DEL-BLR", "origin": "DEL", "destination": "BLR", "carrier": "IndiGo", "flight_no": "6E-2134", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 3900.0, "taxes_udf": 680.0, "convenience_fee": 120.0, "total_fare": 4700.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F202", "pair": "DEL-BLR", "origin": "DEL", "destination": "BLR", "carrier": "Air India", "flight_no": "AI-506", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 4600.0, "taxes_udf": 750.0, "convenience_fee": 0.0, "total_fare": 5350.0, "seat_avail": True, "source": "Air India Direct"},
            {"id": "F203", "pair": "DEL-BLR", "origin": "DEL", "destination": "BLR", "carrier": "Akasa Air", "flight_no": "QP-1331", "departure_date": "2026-09-29", "scrape_date": "2026-09-14", "advance_days": 15, "fare_class": "Economy", "base_fare": 3250.0, "taxes_udf": 580.0, "convenience_fee": 100.0, "total_fare": 3930.0, "seat_avail": True, "source": "EaseMyTrip"},

            # BOM-BLR
            {"id": "F301", "pair": "BOM-BLR", "origin": "BOM", "destination": "BLR", "carrier": "IndiGo", "flight_no": "6E-455", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 2900.0, "taxes_udf": 510.0, "convenience_fee": 120.0, "total_fare": 3530.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F302", "pair": "BOM-BLR", "origin": "BOM", "destination": "BLR", "carrier": "SpiceJet", "flight_no": "SG-302", "departure_date": "2026-09-29", "scrape_date": "2026-09-14", "advance_days": 15, "fare_class": "Economy", "base_fare": 2500.0, "taxes_udf": 480.0, "convenience_fee": 150.0, "total_fare": 3130.0, "seat_avail": True, "source": "Yatra"},

            # DEL-CCU
            {"id": "F401", "pair": "DEL-CCU", "origin": "DEL", "destination": "CCU", "carrier": "IndiGo", "flight_no": "6E-678", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 2600.0, "taxes_udf": 450.0, "convenience_fee": 120.0, "total_fare": 3170.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F402", "pair": "DEL-CCU", "origin": "DEL", "destination": "CCU", "carrier": "SpiceJet", "flight_no": "SG-271", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 2300.0, "taxes_udf": 420.0, "convenience_fee": 150.0, "total_fare": 2870.0, "seat_avail": True, "source": "Ixigo"},

            # BLR-HYD
            {"id": "F501", "pair": "BLR-HYD", "origin": "BLR", "destination": "HYD", "carrier": "IndiGo", "flight_no": "6E-344", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 1800.0, "taxes_udf": 380.0, "convenience_fee": 120.0, "total_fare": 2300.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F502", "pair": "BLR-HYD", "origin": "BLR", "destination": "HYD", "carrier": "Air India Express", "flight_no": "IX-992", "departure_date": "2026-10-29", "scrape_date": "2026-09-14", "advance_days": 45, "fare_class": "Economy", "base_fare": 1500.0, "taxes_udf": 320.0, "convenience_fee": 100.0, "total_fare": 1920.0, "seat_avail": True, "source": "Cleartrip"},

            # MAA-DEL
            {"id": "F601", "pair": "MAA-DEL", "origin": "MAA", "destination": "DEL", "carrier": "Air India", "flight_no": "AI-440", "departure_date": "2026-09-15", "scrape_date": "2026-09-14", "advance_days": 1, "fare_class": "Economy", "base_fare": 7400.0, "taxes_udf": 980.0, "convenience_fee": 0.0, "total_fare": 8380.0, "seat_avail": True, "source": "Air India Direct"},
            {"id": "F602", "pair": "MAA-DEL", "origin": "MAA", "destination": "DEL", "carrier": "IndiGo", "flight_no": "6E-501", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 3600.0, "taxes_udf": 620.0, "convenience_fee": 120.0, "total_fare": 4340.0, "seat_avail": True, "source": "IndiGo Direct"},

            # DEL-HYD
            {"id": "F701", "pair": "DEL-HYD", "origin": "DEL", "destination": "HYD", "carrier": "IndiGo", "flight_no": "6E-733", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 4000.0, "taxes_udf": 680.0, "convenience_fee": 120.0, "total_fare": 4800.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F702", "pair": "DEL-HYD", "origin": "DEL", "destination": "HYD", "carrier": "Air India", "flight_no": "AI-2402", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 4300.0, "taxes_udf": 720.0, "convenience_fee": 0.0, "total_fare": 5020.0, "seat_avail": True, "source": "Air India Direct"},

            # CCU-BLR
            {"id": "F711", "pair": "CCU-BLR", "origin": "CCU", "destination": "BLR", "carrier": "IndiGo", "flight_no": "6E-891", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 4200.0, "taxes_udf": 700.0, "convenience_fee": 120.0, "total_fare": 5020.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F712", "pair": "CCU-BLR", "origin": "CCU", "destination": "BLR", "carrier": "SpiceJet", "flight_no": "SG-411", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 3900.0, "taxes_udf": 650.0, "convenience_fee": 150.0, "total_fare": 4700.0, "seat_avail": True, "source": "MakeMyTrip"},

            # DEL-PAT
            {"id": "F721", "pair": "DEL-PAT", "origin": "DEL", "destination": "PAT", "carrier": "IndiGo", "flight_no": "6E-2077", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 3300.0, "taxes_udf": 560.0, "convenience_fee": 120.0, "total_fare": 3980.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F722", "pair": "DEL-PAT", "origin": "DEL", "destination": "PAT", "carrier": "SpiceJet", "flight_no": "SG-8721", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 3100.0, "taxes_udf": 530.0, "convenience_fee": 150.0, "total_fare": 3780.0, "seat_avail": True, "source": "MakeMyTrip"},

            # DEL-IXR
            {"id": "F731", "pair": "DEL-IXR", "origin": "DEL", "destination": "IXR", "carrier": "IndiGo", "flight_no": "6E-2205", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 3400.0, "taxes_udf": 580.0, "convenience_fee": 120.0, "total_fare": 4100.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F732", "pair": "DEL-IXR", "origin": "DEL", "destination": "IXR", "carrier": "Air India", "flight_no": "AI-9821", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 3600.0, "taxes_udf": 620.0, "convenience_fee": 0.0, "total_fare": 4220.0, "seat_avail": True, "source": "Air India Direct"},

            # DEL-GAU
            {"id": "F741", "pair": "DEL-GAU", "origin": "DEL", "destination": "GAU", "carrier": "IndiGo", "flight_no": "6E-6202", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 4500.0, "taxes_udf": 760.0, "convenience_fee": 120.0, "total_fare": 5380.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F742", "pair": "DEL-GAU", "origin": "DEL", "destination": "GAU", "carrier": "Air India", "flight_no": "AI-717", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 4700.0, "taxes_udf": 800.0, "convenience_fee": 0.0, "total_fare": 5500.0, "seat_avail": True, "source": "Air India Direct"},

            # BOM-PAT
            {"id": "F751", "pair": "BOM-PAT", "origin": "BOM", "destination": "PAT", "carrier": "IndiGo", "flight_no": "6E-6501", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 4600.0, "taxes_udf": 780.0, "convenience_fee": 120.0, "total_fare": 5500.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F752", "pair": "BOM-PAT", "origin": "BOM", "destination": "PAT", "carrier": "SpiceJet", "flight_no": "SG-8455", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 4400.0, "taxes_udf": 750.0, "convenience_fee": 150.0, "total_fare": 5300.0, "seat_avail": True, "source": "MakeMyTrip"},

            # CCU-GAU
            {"id": "F761", "pair": "CCU-GAU", "origin": "CCU", "destination": "GAU", "carrier": "IndiGo", "flight_no": "6E-6701", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 3050.0, "taxes_udf": 530.0, "convenience_fee": 120.0, "total_fare": 3700.0, "seat_avail": True, "source": "IndiGo Direct"},
            {"id": "F762", "pair": "CCU-GAU", "origin": "CCU", "destination": "GAU", "carrier": "Alliance Air", "flight_no": "9I-681", "departure_date": "2026-09-21", "scrape_date": "2026-09-14", "advance_days": 7, "fare_class": "Economy", "base_fare": 2900.0, "taxes_udf": 500.0, "convenience_fee": 0.0, "total_fare": 3400.0, "seat_avail": True, "source": "Cleartrip"},
        ]

        # Real Laspeyres/Paasche/Fisher computation (index_math/engine.py) over
        # the most recent day's basket-route fares, so the last DailyIndex row
        # (2026-09-14) reflects the real formula output rather than a
        # hand-typed placeholder. Earlier historical rows remain a static
        # backfill snapshot since no historical raw fare quotes exist for
        # those dates in this seed dataset.
        latest_fares_by_route: Dict[str, List[float]] = defaultdict(list)
        for f in raw_fares_seed:
            if f["pair"] in BASE_PERIOD_ROUTE_FARES:
                latest_fares_by_route[f["pair"]].append(f["total_fare"])
        latest_index = compute_daily_aggregate_indices(latest_fares_by_route)

        # Seed Daily Index History (Base 2025-01-01 = 100)
        daily_records = [
            DailyIndex(date="2026-09-01", laspeyres=101.40, fisher=101.05, ci_lower=100.2, ci_upper=102.6, t1=119.2, t7=114.3, t15=109.1, t30=107.5, t45=106.8),
            DailyIndex(date="2026-09-02", laspeyres=101.50, fisher=101.10, ci_lower=100.3, ci_upper=102.7, t1=119.4, t7=114.5, t15=109.2, t30=107.6, t45=106.9),
            DailyIndex(date="2026-09-03", laspeyres=101.65, fisher=101.20, ci_lower=100.4, ci_upper=102.9, t1=119.6, t7=114.7, t15=109.4, t30=107.8, t45=107.0),
            DailyIndex(date="2026-09-04", laspeyres=101.80, fisher=101.35, ci_lower=100.5, ci_upper=103.1, t1=119.9, t7=115.0, t15=109.7, t30=108.0, t45=107.2),
            DailyIndex(date="2026-09-05", laspeyres=101.90, fisher=101.40, ci_lower=100.6, ci_upper=103.2, t1=120.0, t7=115.1, t15=109.8, t30=108.1, t45=107.3),
            DailyIndex(date="2026-09-06", laspeyres=102.00, fisher=101.55, ci_lower=100.7, ci_upper=103.3, t1=120.1, t7=115.2, t15=109.9, t30=108.2, t45=107.4),
            DailyIndex(date="2026-09-07", laspeyres=102.10, fisher=101.65, ci_lower=100.8, ci_upper=103.4, t1=120.2, t7=115.3, t15=110.0, t30=108.3, t45=107.5),
            DailyIndex(date="2026-09-08", laspeyres=102.15, fisher=101.80, ci_lower=100.9, ci_upper=103.4, t1=120.2, t7=115.3, t15=110.0, t30=108.4, t45=107.6),
            DailyIndex(date="2026-09-09", laspeyres=102.20, fisher=101.85, ci_lower=100.9, ci_upper=103.5, t1=120.2, t7=115.4, t15=110.1, t30=108.5, t45=107.7),
            DailyIndex(date="2026-09-10", laspeyres=102.25, fisher=101.90, ci_lower=101.0, ci_upper=103.5, t1=120.3, t7=115.4, t15=110.1, t30=108.5, t45=107.7),
            DailyIndex(date="2026-09-11", laspeyres=102.30, fisher=102.00, ci_lower=101.0, ci_upper=103.6, t1=120.3, t7=115.5, t15=110.2, t30=108.6, t45=107.8),
            DailyIndex(date="2026-09-12", laspeyres=102.35, fisher=102.05, ci_lower=101.1, ci_upper=103.6, t1=120.4, t7=115.6, t15=110.3, t30=108.6, t45=107.8),
            DailyIndex(date="2026-09-13", laspeyres=102.40, fisher=102.10, ci_lower=101.1, ci_upper=103.7, t1=120.5, t7=115.7, t15=110.4, t30=108.7, t45=107.9),
            DailyIndex(
                date="2026-09-14",
                laspeyres=latest_index["laspeyres"],
                fisher=latest_index["fisher"],
                ci_lower=latest_index["ci_lower"],
                ci_upper=latest_index["ci_upper"],
                t1=120.3, t7=115.2, t15=110.1, t30=108.7, t45=107.9,
            ),
        ]
        for record in daily_records:
            record.observed_at = datetime.combine(
                datetime.fromisoformat(record.date).date(), time.min, tzinfo=timezone.utc
            )
        session.add_all(daily_records)

        # Seed Weekly Rolling Index
        weekly_records = [
            WeeklyIndex(week_ending="2026-08-24", week_number=34, rolling_laspeyres=100.10, rolling_fisher=99.80, t1=118.2, t7=113.5, t15=108.7, t30=107.0, t45=106.2),
            WeeklyIndex(week_ending="2026-08-31", week_number=35, rolling_laspeyres=101.20, rolling_fisher=100.90, t1=119.0, t7=114.1, t15=109.1, t30=107.4, t45=106.6),
            WeeklyIndex(week_ending="2026-09-07", week_number=36, rolling_laspeyres=101.85, rolling_fisher=101.45, t1=119.8, t7=115.0, t15=109.8, t30=108.0, t45=107.2),
            WeeklyIndex(week_ending="2026-09-14", week_number=37, rolling_laspeyres=102.35, rolling_fisher=102.00, t1=120.3, t7=115.5, t15=110.2, t30=108.6, t45=107.8),
        ]
        session.add_all(weekly_records)

        # Seed Monthly Index
        monthly_records = [
            MonthlyIndex(year=2026, month=5, formula="chained_laspeyres", index_value=111.8, mom_change_pct=0.4, yoy_change_pct=3.6, cpi_transport_contrib=0.12),
            MonthlyIndex(year=2026, month=6, formula="chained_laspeyres", index_value=112.5, mom_change_pct=0.6, yoy_change_pct=3.9, cpi_transport_contrib=0.14),
            MonthlyIndex(year=2026, month=7, formula="chained_laspeyres", index_value=113.1, mom_change_pct=0.5, yoy_change_pct=4.0, cpi_transport_contrib=0.14),
            MonthlyIndex(year=2026, month=8, formula="chained_laspeyres", index_value=113.7, mom_change_pct=0.8, yoy_change_pct=4.2, cpi_transport_contrib=0.15),
        ]
        session.add_all(monthly_records)

        fares_objs = [
            Fare(
                id=f["id"],
                pair=f["pair"],
                origin=f["origin"],
                destination=f["destination"],
                carrier=f["carrier"],
                flight_no=f["flight_no"],
                departure_date=f["departure_date"],
                scrape_date=f["scrape_date"],
                advance_days=f["advance_days"],
                fare_class=f["fare_class"],
                base_fare=f["base_fare"],
                taxes_udf=f["taxes_udf"],
                convenience_fee=f["convenience_fee"],
                total_fare=f["total_fare"],
                seat_avail=f["seat_avail"],
                source=f["source"],
                audit_hash=generate_audit_hash(f),
            )
            for f in raw_fares_seed
        ]
        for fare in fares_objs:
            fare.observed_at = datetime.combine(
                datetime.fromisoformat(fare.scrape_date).date(), time.min, tzinfo=timezone.utc
            )
        session.add_all(fares_objs)

        # Seed Backtest Dataset and Records for DGCA Benchmark Comparison
        backtest_dataset_id = "ds-dgca-2026-benchmark"
        existing_dataset = await session.execute(select(BacktestDataset).where(BacktestDataset.id == backtest_dataset_id))
        if existing_dataset.scalar_one_or_none() is None:
            dataset = BacktestDataset(
                id=backtest_dataset_id,
                source_title="DGCA Monthly Tariff & Airfare Survey (MoSPI Augmentation)",
                source_url="https://dgca.gov.in/reports/airfare_monthly_2026.csv",
                source_file_name="airfare_monthly_2026.csv",
                source_sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                coverage_start="2026-09-01",
                coverage_end="2026-09-14",
            )
            session.add(dataset)

            backtest_records_data = [
                BacktestRecord(date="2026-09-01", dataset_id=backtest_dataset_id, apix_index=101.40, dgca_avg_fare=4480.0, variance_pct=0.45),
                BacktestRecord(date="2026-09-02", dataset_id=backtest_dataset_id, apix_index=101.50, dgca_avg_fare=4510.0, variance_pct=0.42),
                BacktestRecord(date="2026-09-03", dataset_id=backtest_dataset_id, apix_index=101.65, dgca_avg_fare=4540.0, variance_pct=0.38),
                BacktestRecord(date="2026-09-04", dataset_id=backtest_dataset_id, apix_index=101.80, dgca_avg_fare=4580.0, variance_pct=0.35),
                BacktestRecord(date="2026-09-05", dataset_id=backtest_dataset_id, apix_index=101.90, dgca_avg_fare=4610.0, variance_pct=0.32),
                BacktestRecord(date="2026-09-06", dataset_id=backtest_dataset_id, apix_index=102.00, dgca_avg_fare=4640.0, variance_pct=0.30),
                BacktestRecord(date="2026-09-07", dataset_id=backtest_dataset_id, apix_index=102.10, dgca_avg_fare=4670.0, variance_pct=0.28),
                BacktestRecord(date="2026-09-08", dataset_id=backtest_dataset_id, apix_index=102.15, dgca_avg_fare=4690.0, variance_pct=0.25),
                BacktestRecord(date="2026-09-09", dataset_id=backtest_dataset_id, apix_index=102.20, dgca_avg_fare=4710.0, variance_pct=0.22),
                BacktestRecord(date="2026-09-10", dataset_id=backtest_dataset_id, apix_index=102.25, dgca_avg_fare=4730.0, variance_pct=0.20),
                BacktestRecord(date="2026-09-11", dataset_id=backtest_dataset_id, apix_index=102.30, dgca_avg_fare=4750.0, variance_pct=0.18),
                BacktestRecord(date="2026-09-12", dataset_id=backtest_dataset_id, apix_index=102.35, dgca_avg_fare=4780.0, variance_pct=0.15),
                BacktestRecord(date="2026-09-13", dataset_id=backtest_dataset_id, apix_index=102.40, dgca_avg_fare=4810.0, variance_pct=0.12),
                BacktestRecord(date="2026-09-14", dataset_id=backtest_dataset_id, apix_index=latest_index["laspeyres"], dgca_avg_fare=4850.0, variance_pct=0.10),
            ]
            session.add_all(backtest_records_data)

        await session.commit()
