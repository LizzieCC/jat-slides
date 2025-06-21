import numpy as np
from affine import Affine

from dagster import AssetIn, asset
from jat_slides.partitions import mun_partitions, zone_partitions


def calculate_frac_built(built_data: tuple[np.ndarray, Affine]) -> float:
    arr, _ = built_data
    frac_built = (arr >= 2000).sum() / (arr > 0).sum()
    return frac_built


def built_after_2000_factory(suffix: str):
    if suffix == "_mun":
        partitions_def = mun_partitions
    else:
        partitions_def = zone_partitions

    @asset(
        name="built_after_2000",
        key_prefix=f"stats{suffix}",
        ins={"built_data": AssetIn(f"built{suffix}")},
        partitions_def=partitions_def,
        io_manager_key="text_manager",
        group_name=f"stats{suffix}",
    )
    def _asset(built_data: tuple[np.ndarray, Affine]) -> float:
        return calculate_frac_built(built_data)

    return _asset


dassets = [built_after_2000_factory(suffix) for suffix in ("", "_mun", "_trimmed")]
