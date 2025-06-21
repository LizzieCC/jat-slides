import geopandas as gpd

import dagster as dg
from dagster._core.definitions.assets import AssetsDefinition
from jat_slides.partitions import mun_partitions, zone_partitions


def lost_pop_after_2000_factory(suffix: str) -> AssetsDefinition:
    if suffix == "":
        prefix = "base"
        partitions_def = zone_partitions
    elif suffix == "_mun":
        prefix = "mun"
        partitions_def = mun_partitions
    elif suffix == "_trimmed":
        prefix = "trimmed"
        partitions_def = zone_partitions
    else:
        err = f"Suffix {suffix} is not supported. Supported suffixes are '', '_mun', and '_trimmed'."  # noqa: E501
        raise ValueError(err)

    @dg.asset(
        name="lost_pop_after_2000",
        key_prefix=f"stats{suffix}",
        ins={"df": dg.AssetIn(["cells", prefix])},
        partitions_def=partitions_def,
        group_name=f"stats{suffix}",
        io_manager_key="text_manager",
    )
    def _asset(df: gpd.GeoDataFrame) -> float:
        return (df["difference"] < 0).sum() / len(df)

    return _asset


dassets = [lost_pop_after_2000_factory(suffix) for suffix in ("", "_mun", "_trimmed")]
