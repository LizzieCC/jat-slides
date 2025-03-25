import dagster as dg
import geopandas as gpd

from jat_slides.partitions import mun_partitions, zone_partitions
from typing import assert_never


def lost_pop_after_2000_factory(suffix: str):
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
        assert_never(suffix)

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
