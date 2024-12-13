import toml

from dagster import Definitions, EnvVar, load_assets_from_modules, load_assets_from_package_module
from jat_slides.assets import agebs, built, slides, stats
from jat_slides.managers import DataFrameIOManager, PresentationIOManager, RasterIOManager
from jat_slides.resources import PathResource, ZonesResource


# Assets
ageb_assets = load_assets_from_modules([agebs], group_name="agebs")
built_assets = load_assets_from_modules([built], group_name="built")
stat_assets = load_assets_from_package_module(stats, group_name="stats")


# Resources
path_resource = PathResource(
    ghsl_path=EnvVar("GHSL_PATH"),
    pg_path=EnvVar("POPULATION_GRIDS_PATH"),
    out_path=EnvVar("OUT_PATH"),
    figure_path=EnvVar("FIGURE_PATH")
)

with open("./config.toml", "r", encoding="utf8") as f:
    config = toml.load(f)

zones_resource = ZonesResource(
    wanted_zones=config["wanted_zones"],
    zone_names=config["zone_names"]
)


# Managers
csv_manager = DataFrameIOManager(path_resource=path_resource, extension=".csv")
gpkg_manager = DataFrameIOManager(path_resource=path_resource, extension=".gpkg")
raster_manager = RasterIOManager(path_resource=path_resource, extension=".tif")
presentation_manger = PresentationIOManager(path_resource=path_resource, extension=".pptx")


# Definitions
defs = Definitions.merge(
    Definitions(
        assets=ageb_assets+built_assets+stat_assets,
        resources={
            "path_resource": path_resource,
            "zones_resource": zones_resource,
            "csv_manager": csv_manager,
            "gpkg_manager": gpkg_manager,
            "presentation_manager": presentation_manger,
            "raster_manager": raster_manager
        }
    ),
    slides.defs
)