import geopandas as gpd

import dagster as dg
from jat_slides.partitions import zone_partitions


@dg.asset(
    name="total_jobs",
    key_prefix="stats",
    ins={"df_jobs": dg.AssetIn(["jobs", "reprojected"])},
    partitions_def=zone_partitions,
    io_manager_key="text_manager",
    group_name="stats",
)
def total_jobs(df_jobs: gpd.GeoDataFrame) -> float:
    return df_jobs["jobs"].sum()
