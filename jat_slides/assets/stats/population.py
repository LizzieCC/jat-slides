import geopandas as gpd
import pandas as pd

from dagster import asset, AssetIn
from jat_slides.partitions import zone_partitions


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
    io_manager_key="csv_manager"
)
def population(agebs_1990: gpd.GeoDataFrame, agebs_2000: gpd.GeoDataFrame, agebs_2010: gpd.GeoDataFrame, agebs_2020: gpd.GeoDataFrame) -> pd.DataFrame:
    pops = []
    for year, agebs in zip((1990, 2000, 2010, 2020), (agebs_1990, agebs_2000, agebs_2010, agebs_2020)):
        pop = agebs["POBTOT"].sum()
        pops.append(dict(
            year=year,
            pop=pop
        ))
    return pd.DataFrame(pops)