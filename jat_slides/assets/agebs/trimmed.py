from upath import UPath as Path

import geopandas as gpd

import dagster as dg
from jat_slides.partitions import zone_partitions
from jat_slides.resources import PathResource

from cfc_core_utils import gdal_azure_session

def agebs_trimmed_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        name=str(year),
        ins={"ageb_df": dg.AssetIn(["agebs", str(year)])},
        key_prefix="agebs_trimmed",
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        path_resource: PathResource,
        ageb_df: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        trimmed_path = Path(path_resource.data_path) / "trimmed"

        path_list = list(trimmed_path.glob(f"{context.partition_key}.gpkg"))
        if len(path_list) == 0:
            return ageb_df

        if ageb_df.crs is None:
            err = "ageb_df has no CRS. Please set the CRS before trimming."
            raise ValueError(err)
        
        with gdal_azure_session(path=path_list[0]):
            trim_bounds = gpd.read_file(path_list[0]).to_crs(ageb_df.crs)["geometry"].item()
        intersection_area_frac = ageb_df.intersection(trim_bounds).area / ageb_df.area
        return ageb_df.loc[intersection_area_frac > 0.8]

    return _asset


agebs_trimmed = [agebs_trimmed_factory(year) for year in (1990, 2000, 2010, 2020)]
