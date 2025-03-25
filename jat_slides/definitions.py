import dagster as dg

from jat_slides.assets import agebs, built, cells, maps, muns, slides, stats
from jat_slides.managers import (
    DataFrameIOManager,
    PathIOManager,
    PlotFigIOManager,
    PresentationIOManager,
    RasterIOManager,
    ReprojectedRasterIOManager,
    TextIOManager,
)
from jat_slides.resources import (
    path_resource,
    wanted_zones_resource,
    zone_names_resource,
    zone_bounds_resource,
    zone_linewidths_resource,
    wanted_muns_resource,
    mun_names_resource,
    mun_bounds_resource,
    trimmed_bounds_resource,
    wanted_trimmed_resource,
)


# Managers
csv_manager = DataFrameIOManager(path_resource=path_resource, extension=".csv")
gpkg_manager = DataFrameIOManager(path_resource=path_resource, extension=".gpkg")
memory_manager = dg.InMemoryIOManager()
raster_manager = RasterIOManager(path_resource=path_resource, extension=".tif")
reprojected_raster_manager = ReprojectedRasterIOManager(
    path_resource=path_resource, extension=".tif", crs="EPSG:4326"
)
presentation_manger = PresentationIOManager(
    path_resource=path_resource, extension=".pptx"
)
plot_manager = PlotFigIOManager(path_resource=path_resource, extension=".jpg")
path_manager = PathIOManager(path_resource=path_resource, extension=".jpg")
text_manager = TextIOManager(path_resource=path_resource, extension=".txt")


# Definitions
defs = dg.Definitions.merge(
    dg.Definitions(
        assets=(
            dg.load_assets_from_modules([agebs], group_name="agebs")
            + dg.load_assets_from_modules([built], group_name="built_rasters")
            + dg.load_assets_from_modules([muns], group_name="muns")
            + dg.load_assets_from_modules([cells], group_name="cells")
        ),
        resources={
            "path_resource": path_resource,
            "wanted_zones_resource": wanted_zones_resource,
            "zone_names_resource": zone_names_resource,
            "zone_bounds_resource": zone_bounds_resource,
            "zone_linewidths_resource": zone_linewidths_resource,
            "csv_manager": csv_manager,
            "gpkg_manager": gpkg_manager,
            "memory_manager": memory_manager,
            "presentation_manager": presentation_manger,
            "raster_manager": raster_manager,
            "reprojected_raster_manager": reprojected_raster_manager,
            "plot_manager": plot_manager,
            "path_manager": path_manager,
            "text_manager": text_manager,
            "wanted_muns_resource": wanted_muns_resource,
            "mun_names_resource": mun_names_resource,
            "mun_bounds_resource": mun_bounds_resource,
            "trimmed_bounds_resource": trimmed_bounds_resource,
            "wanted_trimmed_resource": wanted_trimmed_resource,
        },
    ),
    agebs.defs,
    maps.defs,
    slides.defs,
    stats.defs,
)
