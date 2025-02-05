from dagster import ConfigurableResource


class PathResource(ConfigurableResource):
    pg_path: str
    out_path: str
    ghsl_path: str
    figure_path: str
    framework_path: str


class ZonesListResource(ConfigurableResource):
    zones: list[str]


class ZonesMapListResource(ConfigurableResource):
    zones: dict[str, list[float, float, float, float]]


class ZonesMapStrResource(ConfigurableResource):
    zones: dict[str, str]


class ZonesMapFloatResource(ConfigurableResource):
    zones: dict[str, float]
