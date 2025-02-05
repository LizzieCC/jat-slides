import geopandas as gpd

from dagster import asset, AssetExecutionContext
from jat_slides.partitions import zone_partitions
from jat_slides.resources import PathResource
from pathlib import Path
from typing import assert_never


def agebs_factory(year: int):
    if year in (1990, 2000):
        infix = "translated"
    elif year in (2010, 2020):
        infix = "shaped"
    else:
        assert_never(year)

    @asset(
        name=str(year),
        key_prefix="agebs",
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        context: AssetExecutionContext, path_resource: PathResource
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key

        agebs_path = Path(path_resource.pg_path) / f"zone_agebs/{infix}/{year}/{zone}.gpkg"
        df = gpd.read_file(agebs_path).to_crs("ESRI:54009")
        df["geometry"] = df["geometry"].make_valid()
        return df

    return _asset


agebs = [agebs_factory(year) for year in (1990, 2000, 2010, 2020)]
