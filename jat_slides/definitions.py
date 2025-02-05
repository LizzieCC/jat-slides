import toml

from dagster import (
    Definitions,
    EnvVar,
    load_assets_from_modules,
)
from jat_slides.assets import agebs, built, maps, muns, slides, stats
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
    PathResource,
    ZonesMapListResource,
    ZonesMapStrResource,
    ZonesListResource,
    ZonesMapFloatResource,
)


# Assets
ageb_assets = load_assets_from_modules([agebs], group_name="agebs")
built_assets = load_assets_from_modules([built], group_name="built_rasters")
map_assets = load_assets_from_modules([maps], group_name="maps")
muns_assets = load_assets_from_modules([muns], group_name="muns")

built_area_assets = load_assets_from_modules([stats.built_area], group_name="built_area")
built_urban_area_assets = load_assets_from_modules([stats.built_urban_area], group_name="built_urban_area")
built_after_2000_assets = load_assets_from_modules([stats.built_after_2000], group_name="built_after_2000")
lost_pop_after_2000_assets = load_assets_from_modules([stats.lost_pop_after_2000], group_name="lost_pop_after_2000")
population_assets = load_assets_from_modules([stats.population], group_name="population")

# Resources
path_resource = PathResource(
    ghsl_path=EnvVar("GHSL_PATH"),
    pg_path=EnvVar("POPULATION_GRIDS_PATH"),
    out_path=EnvVar("OUT_PATH"),
    figure_path=EnvVar("FIGURE_PATH"),
    framework_path=EnvVar("FRAMEWORK_PATH"),
)

with open("./config.toml", "r", encoding="utf8") as f:
    config = toml.load(f)

wanted_zones_resource = ZonesListResource(zones=config["wanted_zones"])
zone_names_resource = ZonesMapStrResource(zones=config["zone_names"])
zone_bounds_resource = ZonesMapListResource(zones=config["bounds"])
zone_linewidths_resource = ZonesMapFloatResource(zones=config["linewidths"])

wanted_muns_resource = ZonesListResource(zones=config["wanted_muns"])
mun_names_resource = ZonesMapStrResource(zones=config["mun_names"])
mun_bounds_resource = ZonesMapListResource(zones=config["bounds_mun"])


# Managers
csv_manager = DataFrameIOManager(path_resource=path_resource, extension=".csv")
gpkg_manager = DataFrameIOManager(path_resource=path_resource, extension=".gpkg")
raster_manager = RasterIOManager(path_resource=path_resource, extension=".tif")
reprojected_raster_manager = ReprojectedRasterIOManager(path_resource=path_resource, extension=".tif", crs="EPSG:4326")
presentation_manger = PresentationIOManager(
    path_resource=path_resource, extension=".pptx"
)
plot_manager = PlotFigIOManager(path_resource=path_resource, extension=".jpg")
path_manager = PathIOManager(path_resource=path_resource, extension=".jpg")
text_manager = TextIOManager(path_resource=path_resource, extension=".txt")


# Definitions
defs = Definitions.merge(
    Definitions(
        assets=(
            ageb_assets 
            + built_assets 
            + built_after_2000_assets
            + built_area_assets
            + built_urban_area_assets
            + lost_pop_after_2000_assets
            + population_assets
            + map_assets 
            + muns_assets
        ),
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
            "reprojected_raster_manager": reprojected_raster_manager,
            "plot_manager": plot_manager,
            "path_manager": path_manager,
            "text_manager": text_manager,
            "wanted_muns_resource": wanted_muns_resource,
            "mun_names_resource": mun_names_resource,
            "mun_bounds_resource": mun_bounds_resource,
        },
    ),
    slides.defs,
)
