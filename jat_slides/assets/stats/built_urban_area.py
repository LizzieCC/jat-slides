import geopandas as gpd
import pandas as pd

from dagster import asset, AssetIn
from jat_slides.partitions import zone_partitions


@asset(
    name="built_urban_area", 
    key_prefix="stats", 
    ins={
        "agebs_1990": AssetIn(key=["agebs", "1990"]),
        "agebs_2000": AssetIn(key=["agebs", "2000"]),
        "agebs_2010": AssetIn(key=["agebs", "2010"]),
        "agebs_2020": AssetIn(key=["agebs", "2020"]),
    },
    partitions_def=zone_partitions, 
    io_manager_key="csv_manager"
)
def built_urban_area(agebs_1990: gpd.GeoDataFrame, agebs_2000: gpd.GeoDataFrame, agebs_2010: gpd.GeoDataFrame, agebs_2020: gpd.GeoDataFrame) -> pd.DataFrame:
    out = []
    for year, agebs in zip((1990, 2000, 2010, 2020), (agebs_1990, agebs_2000, agebs_2010, agebs_2020)):
        area = agebs.to_crs("EPSG:6372").area.sum()
        out.append(dict(
            year=year,
            area=area
        ))
    out = pd.DataFrame(out)
    return out