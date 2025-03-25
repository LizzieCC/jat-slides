import jenkspy

import dagster as dg
import geopandas as gpd
import matplotlib as mpl
import numpy as np
import pandas as pd

from jat_slides.assets.maps.common import (
    generate_figure,
    get_linewidth,
    get_bounds_base,
    get_bounds_trimmed,
    intersect_geometries,
    update_categorical_legend,
)
from jat_slides.partitions import zone_partitions
from jat_slides.resources import (
    PathResource,
)
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.legend import Legend
from pathlib import Path


@dg.op
def load_jobs_df(
    context: dg.OpExecutionContext, path_resource: PathResource
) -> gpd.GeoDataFrame:
    jobs_path = Path(path_resource.jobs_path)
    return (
        gpd.read_file(jobs_path / f"{context.partition_key}.geojson")
        .dropna(subset=["num_empleos"])
        .to_crs("EPSG:4326")
    )


def add_categorical_column(
    df: gpd.GeoDataFrame, column: str, bins: int
) -> tuple[gpd.GeoDataFrame, dict[int, str]]:
    breaks_orig = jenkspy.jenks_breaks(df[column], bins)
    breaks_middle = np.round(np.array(breaks_orig[1:-1]) / 100) * 100

    start = np.floor(breaks_orig[0] / 100) * 100
    start = np.max([1, start])

    breaks = np.insert(breaks_middle, 0, start)
    breaks = np.append(breaks, np.ceil(breaks_orig[-1] / 100) * 100)

    mask = pd.Series([0] * len(df), index=df.index, dtype=int)
    label_map = {}
    for i, (start, end) in enumerate(zip(breaks, breaks[1:])):
        mask = mask + ((df[column] >= start) & (df[column] < end)) * (i + 1)
        label_map[i + 1] = f"{start:,.0f} - {end:,.0f}"

    df = df.assign(category=mask).sort_values("category")
    return df, label_map


def replace_categorical_legend(ax: Axes, label_map: dict[int, str]):
    legend = ax.get_legend()

    labels = [label_map[int(text.get_text())] for text in legend.texts]
    handles = legend.legend_handles

    ax.legend(
        labels=reversed(labels), handles=reversed(handles), title="Número de empleos"
    )


@dg.op(out=dg.Out(io_manager_key="plot_manager"))
def plot_jobs(
    df: gpd.GeoDataFrame, bounds: tuple[float, float, float, float], lw: float
) -> Figure:
    cmap = mpl.colormaps["YlGn"]

    df, label_map = add_categorical_column(df, "num_empleos", 6)

    fig, ax = generate_figure(*bounds)
    df.plot(
        column="category",
        legend=True,
        categorical=True,
        cmap=cmap,
        ax=ax,
        edgecolor="k",
        lw=lw,
        autolim=False,
        aspect=None,
    )
    replace_categorical_legend(ax, label_map)
    # update_categorical_legend(ax, title="Número de empleos", fmt=",.0f", cmap=cmap)
    return fig


# pylint: disable=no-value-for-parameter
@dg.graph_asset(
    name="jobs",
    key_prefix="plot",
    partitions_def=zone_partitions,
    group_name="plot",
)
def jobs_plot() -> Figure:
    df = load_jobs_df()
    lw = get_linewidth()
    bounds = get_bounds_base()
    return plot_jobs(df, bounds, lw)


# pylint: disable=no-value-for-parameter
@dg.graph_asset(
    name="jobs",
    key_prefix="plot_trimmed",
    ins={"agebs": dg.AssetIn(["agebs_trimmed", "2020"])},
    partitions_def=zone_partitions,
    group_name="plot_trimmed",
)
def jobs_trimmed_plot(agebs: gpd.GeoDataFrame) -> Figure:
    df = load_jobs_df()
    df = intersect_geometries(df, agebs)
    lw = get_linewidth()
    bounds = get_bounds_trimmed()
    return plot_jobs(df, bounds, lw)
