import rasterio.plot  # pylint: disable=unused-import

import dagster as dg
import matplotlib as mpl
import rasterio as rio
import numpy as np

from affine import Affine
from jat_slides.assets.maps.common import (
    generate_figure,
    get_bounds_base,
    get_bounds_mun,
    get_bounds_trimmed,
)
from jat_slides.partitions import mun_partitions, zone_partitions
from matplotlib.figure import Figure
from matplotlib.patches import Patch


def add_built_legend(cmap, *, ax):
    patches = []
    for i, year in enumerate(range(1975, 2021, 5)):
        if year == 1975:
            label = "1975 o antes"
        else:
            label = str(year)
        patches.append(Patch(color=cmap(i), label=label))

    ax.legend(
        handles=patches,
        title="Año de construcción",
        alignment="left",
    )


@dg.op(
    ins={"data_and_transform": dg.In(input_manager_key="reprojected_raster_manager")},
    out=dg.Out(io_manager_key="plot_manager"),
)
def plot_raster(
    bounds: tuple[float, float, float, float],
    data_and_transform: tuple[np.ndarray, Affine],
) -> Figure:
    fig, ax = generate_figure(*bounds)

    data, transform = data_and_transform

    data = data.astype(float)
    data[data == 0] = np.nan

    cmap = mpl.colormaps["magma_r"].resampled(10)
    rio.plot.show(data, transform=transform, ax=ax, cmap=cmap)
    add_built_legend(cmap, ax=ax)
    return fig


# pylint: disable=no-value-for-parameter
@dg.graph_asset(
    name="built",
    key_prefix="plot",
    ins={
        "data_and_transform": dg.AssetIn(
            key="built", input_manager_key="reprojected_raster_manager"
        )
    },
    partitions_def=zone_partitions,
    group_name="plot",
)
def built_plot(data_and_transform: tuple[np.ndarray, Affine]) -> Figure:
    bounds = get_bounds_base()
    return plot_raster(bounds, data_and_transform)


# pylint: disable=no-value-for-parameter
@dg.graph_asset(
    name="built",
    key_prefix="plot_mun",
    ins={"data_and_transform": dg.AssetIn(key="built_mun")},
    partitions_def=mun_partitions,
    group_name="plot_mun",
)
def built_plot_mun(data_and_transform: tuple[np.ndarray, Affine]) -> Figure:
    bounds = get_bounds_mun()
    return plot_raster(bounds, data_and_transform)


# pylint: disable=no-value-for-parameter
@dg.graph_asset(
    name="built",
    key_prefix="plot_trimmed",
    ins={"data_and_transform": dg.AssetIn(key="built_trimmed")},
    partitions_def=zone_partitions,
    group_name="plot_trimmed",
)
def built_plot_trimmed(data_and_transform: tuple[np.ndarray, Affine]) -> Figure:
    bounds = get_bounds_trimmed()
    return plot_raster(bounds, data_and_transform)
