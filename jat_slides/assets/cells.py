from upath import UPath as Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import PathResource

from cfc_core_utils import gdal_azure_session
@dg.asset(
    name="base",
    key_prefix="cells",
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
    group_name="cells_base",
)
def cells_base(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
) -> gpd.GeoDataFrame:
    fpath = (
        Path(path_resource.pg_path)
        / f"differences/2000_2020/{context.partition_key}.gpkg"
    )
    with gdal_azure_session(path=fpath):
        df_return = gpd.read_file(fpath)
    return df_return


@dg.asset(
    name="trimmed",
    key_prefix="cells",
    ins={
        "agebs": dg.AssetIn(["agebs_trimmed", "2020"]),
        "cells": dg.AssetIn(["cells", "base"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
    group_name="cells_trimmed",
)
def cells_trimmed(agebs: gpd.GeoDataFrame, cells: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if cells.crs is None:
        err = "Cells GeoDataFrame has no CRS"
        raise ValueError(err)

    agebs = agebs.to_crs(cells.crs)
    idx = cells.sjoin(agebs[["geometry"]]).index.unique()
    return cells.loc[idx]


@dg.asset(
    name="mun",
    key_prefix="cells",
    ins={"agebs": dg.AssetIn(["muns", "2020"])},
    partitions_def=mun_partitions,
    io_manager_key="gpkg_manager",
    group_name="cells_mun",
)
def cells_mun(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
    agebs: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    if len(context.partition_key) == 4:
        ent = context.partition_key[0].rjust(2, "0")
    else:
        ent = context.partition_key[:2]

    diff_path = Path(path_resource.pg_path) / "differences/2000_2020"
    df = []

    with gdal_azure_session(path=diff_path):
        for path in diff_path.glob(f"{ent}.*.gpkg"):
            temp = gpd.read_file(path)
            df.append(temp)
    df = pd.concat(df)

    joined = df.sjoin(agebs.to_crs("EPSG:6372"), how="inner", predicate="intersects")[
        "codigo"
    ].unique()

    return df.loc[df["codigo"].isin(joined)]
