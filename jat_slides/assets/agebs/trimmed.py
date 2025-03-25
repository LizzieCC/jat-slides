import dagster as dg
import geopandas as gpd

from jat_slides.partitions import zone_partitions
from jat_slides.resources import PathResource
from pathlib import Path


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
        trimmed_path = Path(path_resource.trimmed_path)

        path_list = list(trimmed_path.glob(f"{context.partition_key}.gpkg"))
        if len(path_list) == 0:
            return ageb_df

        trim_bounds = gpd.read_file(path_list[0]).to_crs(ageb_df.crs)["geometry"].item()
        intersection_area_frac = ageb_df.intersection(trim_bounds).area / ageb_df.area
        ageb_df = ageb_df[intersection_area_frac > 0.8]
        return ageb_df

    return _asset


agebs_trimmed = [agebs_trimmed_factory(year) for year in (1990, 2000, 2010, 2020)]
