import contextily as cx
import dagster as dg
import geopandas as gpd
import matplotlib.colors as mcol
import matplotlib.pyplot as plt
import numpy as np

from jat_slides.resources import ZonesMapListResource, ZonesMapFloatResource
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import Patch


cmap_rdbu = mcol.LinearSegmentedColormap.from_list(
    "RdBu2",
    ["#67001f", "#c94741", "#f7b799", "#f6f7f7", "#5991e1", "#3340e2", "#090393"],
    N=255,
)


def get_cmap_bounds(differences, n_steps):
    pos_step = differences.max() / n_steps
    neg_step = differences.min() / n_steps

    bounds = np.array(
        [neg_step * i for i in range(1, n_steps + 1)][::-1]
        + [-0.001, 0.001]
        + [pos_step * i for i in range(1, n_steps + 1)]
    )
    return bounds


def add_pop_legend(bounds, *, ax, cmap: mcol.Colormap):
    cmap = cmap.resampled(7)

    patches = []
    for i, (lower, upper) in enumerate(zip(bounds, bounds[1:])):
        if np.round(lower) == 0 and np.round(upper) == 0:
            label = "Sin cambio"
        else:
            label = f"{lower:.0f} - {upper:.0f}"
        patches.append(Patch(color=cmap(i), label=label))
    patches = patches[::-1]

    ax.legend(
        handles=patches,
        loc="lower right",
        title="Cambio de población\n(2020 - 2000)",
        alignment="left",
    )


def generate_figure(
    xmin: float, ymin: float, xmax: float, ymax: float
) -> tuple[Figure, Axes]:
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


@dg.op
def get_bounds_base(
    context: dg.OpExecutionContext, zone_bounds_resource: ZonesMapListResource
) -> tuple[float, float, float, float]:
    return tuple(zone_bounds_resource.zones[context.partition_key])


@dg.op
def get_bounds_mun(
    context: dg.OpExecutionContext, mun_bounds_resource: ZonesMapListResource
) -> tuple[float, float, float, float]:
    return tuple(mun_bounds_resource.zones[context.partition_key])


@dg.op
def get_bounds_trimmed(
    context: dg.OpExecutionContext, trimmed_bounds_resource: ZonesMapListResource
) -> tuple[float, float, float, float]:
    return tuple(trimmed_bounds_resource.zones[context.partition_key])


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
        loc="lower right",
        title="Año de construcción",
        alignment="left",
    )


def update_categorical_legend(
    ax: Axes, title: str, fmt: str, cmap: mcol.Colormap
) -> None:
    leg = ax.get_legend()
    leg.set_title(title)
    leg.set_alignment("left")

    texts = []
    for text_obj in leg.get_texts():
        start, end = text_obj.get_text().split(",")
        start = float(start.strip())
        end = float(end.strip())
        texts.append(f"{start:{fmt}} - {end:{fmt}}")

    steps = [i / (len(texts) - 1) for i in range(len(texts))]
    steps[-1] = 0.9999
    handles = [Patch(facecolor=cmap(x)) for x in reversed(steps)]

    ax.legend(handles=handles, labels=reversed(texts), title=title, alignment="left")


@dg.op
def get_linewidth(
    context: dg.OpExecutionContext, zone_linewidths_resource: ZonesMapFloatResource
) -> float:
    if context.partition_key in zone_linewidths_resource.zones:
        return zone_linewidths_resource.zones[context.partition_key]
    else:
        return 0.2


@dg.op
def intersect_geometries(
    sources: gpd.GeoDataFrame, targets: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    overlay = sources.sjoin(targets[["geometry"]].to_crs(sources.crs))
    idx = overlay.index.unique()
    return sources.loc[idx]
