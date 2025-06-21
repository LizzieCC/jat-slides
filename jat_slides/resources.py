import toml

import dagster as dg
from dagster import ConfigurableResource


class PathResource(ConfigurableResource):
    pg_path: str
    ghsl_path: str
    segregation_path: str
    jobs_path: str
    data_path: str


class ZonesListResource(ConfigurableResource):
    zones: list[str]


class ZonesMapListResource(ConfigurableResource):
    zones: dict[str, list[float]]


class ZonesMapStrResource(ConfigurableResource):
    zones: dict[str, str]


class ZonesMapFloatResource(ConfigurableResource):
    zones: dict[str, float]


class ConfigResource(ConfigurableResource):
    bounds: dict[str, list[float]]
    names: dict[str, str] | None = None
    linewidths: dict[str, float] | None = None
    legend_pos: dict[str, str] | None = None
    add_labels: dict[str, list[str]] | None = None


with open("./config/zone.toml", encoding="utf8") as f:
    config = toml.load(f)

zone_config = ConfigResource(
    names=config.get("names"),
    bounds=config.get("bounds"),
    linewidths=config.get("linewidths"),
    legend_pos=config.get("legend_pos"),
    add_labels=config.get("add_labels"),
)


with open("./config/mun.toml", encoding="utf8") as f:
    config = toml.load(f)

mun_config = ConfigResource(
    names=config.get("names"),
    bounds=config.get("bounds"),
    linewidths=config.get("linewidths"),
    legend_pos=config.get("legend_pos"),
    add_labels=config.get("add_labels"),
)


with open("./config/trimmed.toml", encoding="utf8") as f:
    config = toml.load(f)
trimmed_config = ConfigResource(
    names=config.get("names"),
    bounds=config.get("bounds"),
    linewidths=config.get("linewidths"),
    legend_pos=config.get("legend_pos"),
    add_labels=config.get("add_labels"),
)


path_resource = PathResource(
    ghsl_path=dg.EnvVar("GHSL_PATH"),
    pg_path=dg.EnvVar("POPULATION_GRIDS_PATH"),
    segregation_path=dg.EnvVar("SEGREGATION_PATH"),
    jobs_path=dg.EnvVar("JOBS_PATH"),
    data_path=dg.EnvVar("DATA_PATH"),
)


defs = dg.Definitions(
    resources={
        "path_resource": path_resource,
        "zone_config_resource": zone_config,
        "mun_config_resource": mun_config,
        "trimmed_config_resource": trimmed_config,
    },
)
