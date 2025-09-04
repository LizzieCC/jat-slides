from upath import UPath as Path


import geopandas as gpd

import dagster as dg
from jat_slides.partitions import zone_partitions
from jat_slides.resources import PathResource
from cfc_core_utils import gdal_azure_session

def agebs_factory(year: int) -> dg.AssetsDefinition:
    infix = "shaped"

    @dg.asset(
        name=str(year),
        key_prefix="agebs",
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        path_resource: PathResource,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key

        agebs_path = (
            Path(path_resource.pg_path) / f"zone_agebs/{infix}/{year}/{zone}.gpkg"
        )
        print(f"[DEBUG agebs__{year}] Using agebs_path={agebs_path}")
        with gdal_azure_session(path=agebs_path):
            df = gpd.read_file(agebs_path).to_crs("ESRI:54009")
        df["geometry"] = df["geometry"].make_valid()
        return df

    return _asset


agebs = [agebs_factory(year) for year in (1990, 2000, 2010, 2020)]
