import toml

from dagster import (
    Definitions,
    EnvVar,
    load_assets_from_modules,
    load_assets_from_package_module,
)
from jat_slides.assets import agebs, built, maps, slides, stats
from jat_slides.managers import (
    DataFrameIOManager,
    PathIOManager,
    PlotFigIOManager,
    PresentationIOManager,
    RasterIOManager,
)
from jat_slides.resources import (
    PathResource,
    ZonesMapListResource,
    ZonesMapStrResource,
    ZonesListResource,
    ZonesMapFloatResource,
)


# Assets
ageb_assets = load_assets_from_modules([agebs], group_name="agebs")
built_assets = load_assets_from_modules([built], group_name="built")
map_assets = load_assets_from_modules([maps], group_name="maps")
stat_assets = load_assets_from_package_module(stats, group_name="stats")


# Resources
path_resource = PathResource(
    ghsl_path=EnvVar("GHSL_PATH"),
    pg_path=EnvVar("POPULATION_GRIDS_PATH"),
    out_path=EnvVar("OUT_PATH"),
    figure_path=EnvVar("FIGURE_PATH"),
)

with open("./config.toml", "r", encoding="utf8") as f:
    config = toml.load(f)

wanted_zones_resource = ZonesListResource(zones=config["wanted_zones"])
zone_names_resource = ZonesMapStrResource(zones=config["zone_names"])
zone_bounds_resource = ZonesMapListResource(zones=config["bounds"])
zone_linewidths_resource = ZonesMapFloatResource(zones=config["linewidths"])


# Managers
csv_manager = DataFrameIOManager(path_resource=path_resource, extension=".csv")
gpkg_manager = DataFrameIOManager(path_resource=path_resource, extension=".gpkg")
raster_manager = RasterIOManager(path_resource=path_resource, extension=".tif")
presentation_manger = PresentationIOManager(
    path_resource=path_resource, extension=".pptx"
)
plot_manager = PlotFigIOManager(path_resource=path_resource, extension=".png")
path_manager = PathIOManager(path_resource=path_resource, extension=".png")


# Definitions
defs = Definitions.merge(
    Definitions(
        assets=ageb_assets + built_assets + stat_assets + map_assets,
        resources={
            "path_resource": path_resource,
            "wanted_zones_resource": wanted_zones_resource,
            "zone_names_resource": zone_names_resource,
            "zone_bounds_resource": zone_bounds_resource,
            "zone_linewidths_resource": zone_linewidths_resource,
            "csv_manager": csv_manager,
            "gpkg_manager": gpkg_manager,
            "presentation_manager": presentation_manger,
            "raster_manager": raster_manager,
            "plot_manager": plot_manager,
            "path_manager": path_manager,
        },
    ),
    slides.defs,
)
