from dagster import ConfigurableResource


class PathResource(ConfigurableResource):
    pg_path: str
    out_path: str
    ghsl_path: str
    figure_path: str


class ZonesResource(ConfigurableResource):
    wanted_zones: list[str]
    zone_names: dict[str, str]