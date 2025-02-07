import numpy as np

from affine import Affine
from dagster import asset, AssetIn, AssetsDefinition
from jat_slides.partitions import mun_partitions, zone_partitions
from typing import Optional


def calculate_frac_built(built_data: tuple[np.ndarray, Affine]) -> float:
    arr, _ = built_data
    frac_built = (arr >= 2000).sum() / (arr > 0).sum()
    return frac_built


@asset(
    name="built_after_2000",
    key_prefix="stats",
    ins={"built_data": AssetIn("built")},
    partitions_def=zone_partitions,
    io_manager_key="text_manager",
    group_name="stats"
)
def built_after_2000(built_data: tuple[np.ndarray, Affine]) -> float:
    return calculate_frac_built(built_data)


@asset(
    name="built_after_2000",
    key_prefix="stats_mun",
    ins={"built_data": AssetIn("built_mun")},
    partitions_def=mun_partitions,
    io_manager_key="text_manager",
    group_name="stats_mun"
)
def built_after_2000_mun(built_data: tuple[np.ndarray, Affine]) -> float:
    return calculate_frac_built(built_data)