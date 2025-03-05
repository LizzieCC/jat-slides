import geopandas as gpd

from dagster import asset, AssetExecutionContext, AssetIn
from jat_slides.assets.common import get_mun_cells
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import PathResource
from pathlib import Path


@asset(
    name="lost_pop_after_2000",
    key_prefix="stats",
    partitions_def=zone_partitions,
    io_manager_key="text_manager",
    group_name="stats",
)
def lost_pop_after_2000(
    context: AssetExecutionContext, path_resource: PathResource
) -> float:
    fpath = (
        Path(path_resource.pg_path)
        / f"differences/2000_2020/{context.partition_key}.gpkg"
    )
    df = gpd.read_file(fpath)
    diff = (df["difference"] < 0).sum() / len(df)
    return diff


@asset(
    name="lost_pop_after_2000",
    key_prefix="stats_mun",
    ins={"agebs": AssetIn(["muns", "2020"])},
    partitions_def=mun_partitions,
    io_manager_key="text_manager",
    group_name="stats_mun",
)
def lost_pop_after_2000_mun(
    context: AssetExecutionContext, path_resource: PathResource, agebs: gpd.GeoDataFrame
) -> float:
    df = get_mun_cells(context.partition_key, path_resource, agebs)
    diff = (df["difference"] < 0).sum() / len(df)
    return diff
