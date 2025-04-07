import json

import dagster as dg
import geopandas as gpd
import matplotlib as mpl
import pandas as pd

from jat_slides.assets.maps.common import (
    generate_figure,
    get_bounds_base,
    get_bounds_mun,
    get_bounds_trimmed,
    get_linewidth,
    intersect_geometries,
    update_categorical_legend,
)
from jat_slides.partitions import mun_partitions, zone_partitions
from jat_slides.resources import (
    PathResource,
)
from matplotlib.figure import Figure
from pathlib import Path
from typing import assert_never


@dg.op
def load_income_df(
    context: dg.OpExecutionContext, path_resource: PathResource
) -> gpd.GeoDataFrame:
    segregation_path = Path(path_resource.segregation_path)

    with open(segregation_path / "short_to_long_map.json", "r", encoding="utf8") as f:
        long_to_short_map = {value: key for key, value in json.load(f).items()}

    return (
        gpd.read_file(
            segregation_path
            / f"incomes/{long_to_short_map[context.partition_key]}.gpkg"
        )
        .dropna(subset=["income_pc"])
        .to_crs("EPSG:4326")
    )


@dg.op
def load_state_income_df(
    context: dg.OpExecutionContext, path_resource: PathResource
) -> gpd.GeoDataFrame:
    if len(context.partition_key) == 4:
        ent = f"0{context.partition_key[0]}"
    elif len(context.partition_key) == 5:
        ent = context.partition_key[:2]
    else:
        assert_never(len(context.partition_key))

    income_path = Path(path_resource.segregation_path) / "incomes"

    df = []
    for path in income_path.glob(f"M{ent}*.gpkg"):
        df.append(gpd.read_file(path).dropna(subset=["income_pc"]))

    return gpd.GeoDataFrame(pd.concat(df, ignore_index=True)).to_crs("EPSG:4326")


@dg.op(out=dg.Out(io_manager_key="plot_manager"))
def plot_income(
    df: gpd.GeoDataFrame, bounds: tuple[float, float, float, float], lw: float
) -> Figure:
    cmap = mpl.colormaps["RdBu"]

    fig, ax = generate_figure(*bounds)
    df.plot(
        column="income_pc",
        scheme="natural_breaks",
        k=6,
        cmap=cmap,
        legend=True,
        ax=ax,
        edgecolor="k",
        lw=lw,
        autolim=False,
        aspect=None,
    )

    update_categorical_legend(
        ax, title="Ingreso anual per cápita\n(miles de USD)", fmt=".2f", cmap=cmap
    )
    return fig


# pylint: disable=no-value-for-parameter
@dg.graph_asset(
    name="income",
    key_prefix="plot",
    partitions_def=zone_partitions,
    group_name="plot",
)
def income_plot() -> Figure:
    df = load_income_df()
    lw = get_linewidth()
    bounds = get_bounds_base()
    return plot_income(df, bounds, lw)


# pylint: disable=no-value-for-parameter
@dg.graph_asset(
    name="income",
    key_prefix="plot_mun",
    ins={"agebs_mun": dg.AssetIn(key=["muns", "2020"])},
    partitions_def=mun_partitions,
    group_name="plot_mun",
)
def income_plot_mun(agebs_mun: gpd.GeoDataFrame) -> Figure:
    state_df = load_state_income_df()
    df = intersect_geometries(state_df, agebs_mun)
    lw = get_linewidth()
    bounds = get_bounds_mun()
    return plot_income(df, bounds, lw)


# pylint: disable=no-value-for-parameter
@dg.graph_asset(
    name="income",
    key_prefix="plot_trimmed",
    ins={"agebs": dg.AssetIn(key=["agebs_trimmed", "2020"])},
    partitions_def=zone_partitions,
    group_name="plot_trimmed",
)
def income_plot_trimmed(agebs: gpd.GeoDataFrame) -> Figure:
    df = load_income_df()
    lw = get_linewidth()
    bounds = get_bounds_trimmed()
    df = intersect_geometries(df, agebs)
    return plot_income(df, bounds, lw)
