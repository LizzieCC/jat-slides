import rasterio.plot

import contextily as cx
import geopandas as gpd
import matplotlib as mpl
import matplotlib.colors as mcol
import matplotlib.pyplot as plt
import numpy as np
import rasterio as rio

from affine import Affine
from dagster import asset, AssetExecutionContext, AssetIn
from jat_slides.partitions import zone_partitions
from jat_slides.resources import (
    PathResource,
    ZonesMapListResource,
    ZonesMapFloatResource,
)
from matplotlib.figure import Figure
from matplotlib.patches import Patch
from pathlib import Path


def get_cmap_bounds(differences, n_steps):
    pos_step = differences.max() / n_steps
    neg_step = differences.min() / n_steps

    bounds = np.array(
        [neg_step * i for i in range(1, n_steps + 1)][::-1]
        + [-0.001, 0.001]
        + [pos_step * i for i in range(1, n_steps + 1)]
    )
    return bounds


def add_pop_legend(bounds, *, ax):
    cmap = mpl.colormaps["RdBu"].resampled(7)

    patches = []
    for i, (lower, upper) in enumerate(zip(bounds, bounds[1:])):
        if np.round(lower) == 0 and np.round(upper) == 0:
            label = "Sin cambio"
        else:
            label = f"{lower:.0f} - {upper:.0f}"
        patches.append(Patch(color=cmap(i), label=label))
    patches = patches[::-1]

    ax.legend(handles=patches, loc="lower right", title="Cambio de población\n(2020 - 2000)", alignment="left")


def generate_figure(xmin: float, ymin: float, xmax: float, ymax: float):
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.axis("off")

    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)

    fig.subplots_adjust(bottom=0)
    fig.subplots_adjust(top=1)
    fig.subplots_adjust(right=1)
    fig.subplots_adjust(left=0)

    cx.add_basemap(ax, source=cx.providers.CartoDB.Positron, crs="EPSG:4326")
    return fig, ax


@asset(
    name="population_grid",
    key_prefix="plot",
    partitions_def=zone_partitions,
    io_manager_key="plot_manager",
)
def population_grid_plot(
    context: AssetExecutionContext,
    path_resource: PathResource,
    zone_bounds_resource: ZonesMapListResource,
    zone_linewidths_resource: ZonesMapFloatResource,
) -> Figure:
    fpath = (
        Path(path_resource.pg_path)
        / f"differences/2000_2020/{context.partition_key}.gpkg"
    )
    df: gpd.GeoDataFrame = gpd.read_file(fpath)
    
    fig, ax = generate_figure(*zone_bounds_resource.zones[context.partition_key])

    cmap_bounds = get_cmap_bounds(df["difference"], 3)
    norm = mcol.BoundaryNorm(cmap_bounds, 256)

    if context.partition_key in zone_linewidths_resource.zones:
        lw = zone_linewidths_resource.zones[context.partition_key]
    else:
        lw = 0.2
    
    df.to_crs("EPSG:4326").plot(
        column="difference",
        ax=ax,
        cmap="RdBu",
        ec="k",
        lw=lw,
        autolim=False,
        norm=norm,
        aspect=None
    )
    
    add_pop_legend(cmap_bounds, ax=ax)
    
    return fig


def add_built_legend(cmap, *, ax):
    patches = []
    for i, year in enumerate(range(1975, 2021, 5)):
        if year == 1975:
            label = "1975 o antes"
        else:
            label = str(year)
        patches.append(Patch(color=cmap(i), label=label))

    ax.legend(handles=patches, loc="lower right", title="Año de construcción", alignment="left")


@asset(
    name="built",
    key_prefix="plot",
    ins={"data_and_transform": AssetIn(key="built", input_manager_key="reprojected_raster_manager")},
    partitions_def=zone_partitions,
    io_manager_key="plot_manager",
)
def built_plot(
    context: AssetExecutionContext,
    data_and_transform: tuple[np.ndarray, Affine],
    zone_bounds_resource: ZonesMapListResource,
) -> Figure:
    data, transform = data_and_transform
    data = data.astype(float)
    data[data == 0] = np.nan

    cmap = mpl.colormaps["magma_r"].resampled(10)
    fig, ax = generate_figure(*zone_bounds_resource.zones[context.partition_key])
    rio.plot.show(data, transform=transform, ax=ax, cmap=cmap)
    add_built_legend(cmap, ax=ax)
    
    return fig