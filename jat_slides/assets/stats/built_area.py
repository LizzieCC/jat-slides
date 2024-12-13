import rasterio.mask

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio

from affine import Affine
from dagster import graph_asset, op, AssetIn, Out
from jat_slides.partitions import zone_partitions
from jat_slides.resources import PathResource
from pathlib import Path


YEARS = (1990, 2000, 2010, 2020)


def load_built_area_rasters_factory(year: int):
    @op(name=f"load_built_area_rasters_{year}", out={"data": Out(), "transform": Out()})
    def _op(
        path_resource: PathResource, bounds: dict[int, list]
    ) -> tuple[np.ndarray, Affine]:
        fpath = Path(path_resource.ghsl_path) / f"BUILT_100/{year}.tif"
        with rio.open(fpath, nodata=65535) as ds:
            data, transform = rio.mask.mask(ds, bounds[year], crop=True, nodata=0)

        data[data == 65535] = 0
        return data, transform

    return _op


load_built_area_rasters_ops = {
    year: load_built_area_rasters_factory(year) for year in range(1975, 2021, 5)
}


# pylint: disable=unused-argument
@op(out=Out(io_manager_key="csv_manager"))
def reduce_area_rasters(
    rasters: list[np.ndarray], transforms: list[Affine]
) -> pd.DataFrame:
    out = []
    for year, arr in zip(YEARS, rasters):
        out.append(dict(year=year, area=arr.sum()))
    return pd.DataFrame(out)


@op
def get_bounds(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> dict[int, list]:
    bounds = {}
    for year, agebs in zip(
        (1990, 2000, 2010, 2020), (agebs_1990, agebs_2000, agebs_2010, agebs_2020)
    ):
        bounds[year] = agebs["geometry"].to_numpy().tolist()
    return bounds


@graph_asset(
    ins={
        "agebs_1990": AssetIn(key=["agebs", "1990"]),
        "agebs_2000": AssetIn(key=["agebs", "2000"]),
        "agebs_2010": AssetIn(key=["agebs", "2010"]),
        "agebs_2020": AssetIn(key=["agebs", "2020"]),
    },
    name="built_area",
    key_prefix="stats",
    partitions_def=zone_partitions,
)
def built_area(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> pd.DataFrame:
    rasters, transforms = [], []
    bounds = get_bounds(agebs_1990, agebs_2000, agebs_2010, agebs_2020)
    for year in YEARS:
        f = load_built_area_rasters_ops[year]
        data, transform = f(bounds)
        rasters.append(data)
        transforms.append(transform)
    return reduce_area_rasters(rasters, transforms)
