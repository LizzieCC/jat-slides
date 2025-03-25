import dagster as dg
import geopandas as gpd
import matplotlib.colors as mcol

from jat_slides.assets.maps.common import (
    add_pop_legend,
    cmap_rdbu,
    generate_figure,
    get_bounds_base,
    get_bounds_mun,
    get_bounds_trimmed,
    get_cmap_bounds,
    get_linewidth,
)
from jat_slides.partitions import mun_partitions, zone_partitions
from matplotlib.figure import Figure
from typing import assert_never


@dg.op(out=dg.Out(io_manager_key="plot_manager"))
def plot_dataframe(
    bounds: tuple[float, float, float, float], df: gpd.GeoDataFrame, lw: float
) -> Figure:
    fig, ax = generate_figure(*bounds)

    cmap_bounds = get_cmap_bounds(df["difference"], 3)
    norm = mcol.BoundaryNorm(cmap_bounds, 256)

    df.to_crs("EPSG:4326").plot(
        column="difference",
        ax=ax,
        cmap=cmap_rdbu,
        ec="k",
        lw=lw,
        autolim=False,
        norm=norm,
        aspect=None,
    )

    add_pop_legend(cmap_bounds, ax=ax, cmap=cmap_rdbu)
    return fig


# pylint: disable=no-value-for-parameter
def population_grid_plot_factory(suffix: str) -> dg.AssetsDefinition:
    if suffix == "":
        op = get_bounds_base
        prefix = "base"
        partitions_def = zone_partitions
    elif suffix == "_mun":
        op = get_bounds_mun
        prefix = "mun"
        partitions_def = mun_partitions
    elif suffix == "_trimmed":
        op = get_bounds_trimmed
        prefix = "trimmed"
        partitions_def = zone_partitions
    else:
        assert_never(suffix)

    @dg.graph_asset(
        name="population_grid",
        key_prefix=f"plot{suffix}",
        ins={"df": dg.AssetIn(key=["cells", prefix])},
        partitions_def=partitions_def,
        group_name=f"plot{suffix}",
    )
    def _asset(df: gpd.GeoDataFrame) -> Figure:
        bounds = op()
        lw = get_linewidth()
        return plot_dataframe(bounds, df, lw)

    return _asset


dassets = [population_grid_plot_factory(suffix) for suffix in ("", "_mun", "_trimmed")]
