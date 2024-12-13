import geopandas as gpd
import pandas as pd

from dagster import asset
from jat_slides.partitions import zone_partitions
from jat_slides.resources import PathResource
from pathlib import Path


@asset(name="lost_pop_after_2000", key_prefix="stats", io_manager_key="csv_manager")
def lost_pop_after_2000(path_resource: PathResource) -> pd.DataFrame:
    differences = []
    for partition_key in zone_partitions.get_partition_keys():
        fpath = (
            Path(path_resource.pg_path) / f"differences/2000_2020/{partition_key}.gpkg"
        )
        df = gpd.read_file(fpath)
        diff = (df["difference"] < 0).sum() / len(df)
        differences.append(dict(zone=partition_key, lost=diff))
    differences = pd.DataFrame(differences)
    return differences
