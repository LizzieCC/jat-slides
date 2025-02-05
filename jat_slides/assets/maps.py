import contextily as cx
import geopandas as gpd
import matplotlib.colors as mcol
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np

from dagster import asset, AssetExecutionContext
from jat_slides.partitions import zone_partitions
from jat_slides.resources import (
    PathResource,
    ZonesMapListResource,
    ZonesMapFloatResource,
)
from matplotlib.figure import Figure
from pathlib import Path


def get_cmap_bounds(differences, n_steps):
    pos_step = differences.max() / n_steps
    neg_step = differences.min() / n_steps

    bounds = np.array(
        [neg_step * i for i in range(1, n_steps + 1)][::-1]
        + [0]
        + [pos_step * i for i in range(1, n_steps + 1)]
    )
    return bounds


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

    bounds = get_cmap_bounds(df["difference"], 3)
    norm = mcol.BoundaryNorm(bounds, 256)

    if context.partition_key in zone_linewidths_resource.zones:
        lw = zone_linewidths_resource.zones[context.partition_key]
    else:
        lw = 0.2

    fig, ax = plt.subplots(figsize=(8, 4.5))
    df.to_crs("EPSG:4326").plot(
        column="difference",
        ax=ax,
        cmap="RdBu",
        ec="k",
        lw=lw,
        autolim=False,
        legend=True,
        legend_kwds=dict(
            shrink=0.8,
            pad=0.01,
            format=mtick.FuncFormatter(lambda x, p: f"{x:,.0f}"),
            label="Cambio de población (2020 - 2000)",
        ),
        norm=norm,
    )

    xmin, ymin, xmax, ymax = zone_bounds_resource.zones[context.partition_key]
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)

    ax.axis("off")

    cx.add_basemap(ax, source=cx.providers.CartoDB.Positron, crs="EPSG:4326")

    return fig
