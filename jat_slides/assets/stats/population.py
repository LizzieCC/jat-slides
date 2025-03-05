import geopandas as gpd
import pandas as pd

from dagster import asset, AssetIn
from jat_slides.partitions import mun_partitions, zone_partitions


def calculate_lost_pop(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> pd.DataFrame:
    pops = []
    for year, agebs in zip(
        (1990, 2000, 2010, 2020), (agebs_1990, agebs_2000, agebs_2010, agebs_2020)
    ):
        pop = agebs["POBTOT"].sum()
        pops.append(dict(year=year, pop=pop))
    return pd.DataFrame(pops)


@asset(
    ins={
        "agebs_1990": AssetIn(key=["agebs", "1990"]),
        "agebs_2000": AssetIn(key=["agebs", "2000"]),
        "agebs_2010": AssetIn(key=["agebs", "2010"]),
        "agebs_2020": AssetIn(key=["agebs", "2020"]),
    },
    name="population",
    key_prefix="stats",
    partitions_def=zone_partitions,
    io_manager_key="csv_manager",
    group_name="stats",
)
def population(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> pd.DataFrame:
    return calculate_lost_pop(agebs_1990, agebs_2000, agebs_2010, agebs_2020)


@asset(
    ins={
        "agebs_1990": AssetIn(key=["muns", "1990"]),
        "agebs_2000": AssetIn(key=["muns", "2000"]),
        "agebs_2010": AssetIn(key=["muns", "2010"]),
        "agebs_2020": AssetIn(key=["muns", "2020"]),
    },
    name="population",
    key_prefix="stats_mun",
    partitions_def=mun_partitions,
    io_manager_key="csv_manager",
    group_name="stats_mun",
)
def population_mun(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> pd.DataFrame:
    return calculate_lost_pop(agebs_1990, agebs_2000, agebs_2010, agebs_2020)
