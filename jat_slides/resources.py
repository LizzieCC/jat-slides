import toml

import dagster as dg

from dagster import ConfigurableResource


class PathResource(ConfigurableResource):
    pg_path: str
    out_path: str
    ghsl_path: str
    figure_path: str
    segregation_path: str
    jobs_path: str
    trimmed_path: str


class ZonesListResource(ConfigurableResource):
    zones: list[str]


class ZonesMapListResource(ConfigurableResource):
    zones: dict[str, list[float, float, float, float]]


class ZonesMapStrResource(ConfigurableResource):
    zones: dict[str, str]


class ZonesMapFloatResource(ConfigurableResource):
    zones: dict[str, float]


with open("./config.toml", "r", encoding="utf8") as f:
    config = toml.load(f)

wanted_zones_resource = ZonesListResource(zones=config["wanted_zones"])
zone_names_resource = ZonesMapStrResource(zones=config["zone_names"])
zone_bounds_resource = ZonesMapListResource(zones=config["bounds"])
zone_linewidths_resource = ZonesMapFloatResource(zones=config["linewidths"])

wanted_muns_resource = ZonesListResource(zones=config["wanted_muns"])
mun_names_resource = ZonesMapStrResource(zones=config["mun_names"])
mun_bounds_resource = ZonesMapListResource(zones=config["bounds_mun"])

wanted_trimmed_resource = ZonesListResource(zones=config["wanted_trimmed"])
trimmed_bounds_resource = ZonesMapListResource(zones=config["bounds_trimmed"])


path_resource = PathResource(
    ghsl_path=dg.EnvVar("GHSL_PATH"),
    pg_path=dg.EnvVar("POPULATION_GRIDS_PATH"),
    out_path=dg.EnvVar("OUT_PATH"),
    figure_path=dg.EnvVar("FIGURE_PATH"),
    segregation_path=dg.EnvVar("SEGREGATION_PATH"),
    jobs_path=dg.EnvVar("JOBS_PATH"),
    trimmed_path=dg.EnvVar("TRIMMED_PATH"),
)
