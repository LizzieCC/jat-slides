import geopandas as gpd
import pandas as pd

from dagster import asset, AssetExecutionContext
from jat_slides.partitions import mun_partitions
from jat_slides.resources import PathResource
from pathlib import Path
from typing import assert_never


def muns_factory(year: int):
    if year in (1990, 2000):
        infix = "translated"
    elif year in (2010, 2020):
        infix = "shaped"
    else:
        assert_never(year)

    @asset(
        name=str(year),
        key_prefix="muns",
        partitions_def=mun_partitions,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        context: AssetExecutionContext, path_resource: PathResource
    ) -> gpd.GeoDataFrame:
        total_chars = len(context.partition_key)
        if total_chars == 4:
            ent = context.partition_key[0].rjust(2, "0")
        else:
            ent = context.partition_key[:2]

        agebs_dir_path = Path(path_resource.pg_path) / f"zone_agebs/{infix}/{year}"

        df = []
        for path in agebs_dir_path.glob(f"{ent}.*.gpkg"):
            df.append(gpd.read_file(path).to_crs("ESRI:54009"))
        
        df = pd.concat(df)
        df["geometry"] = df["geometry"].make_valid()

        code_prefix = context.partition_key.rjust(5, "0")
        df = df.loc[df["CVEGEO"].str.startswith(code_prefix), ["POBTOT", "geometry"]]
        return df

    return _asset


agebs = [muns_factory(year) for year in (1990, 2000, 2010, 2020)]
