import numpy as np
import pandas as pd

from affine import Affine
from dagster import asset, AssetIn
from jat_slides.partitions import mun_partitions, zone_partitions


@asset(
    name="built_after_2000",
    key_prefix="stats",
    ins={"built_data": AssetIn("built")},
    partitions_def=zone_partitions,
    io_manager_key="text_manager",
)
def built_after_2000(built_data: tuple[np.ndarray, Affine]):
    arr, _ = built_data
    frac_built = (arr >= 2000).sum() / (arr > 0).sum()
    return frac_built


@asset(
    name="built_after_2000_mun",
    key_prefix="stats",
    ins={"built_data": AssetIn("built_mun")},
    partitions_def=mun_partitions,
    io_manager_key="text_manager",
)
def built_after_2000_mun(built_data: tuple[np.ndarray, Affine]):
    arr, _ = built_data
    frac_built = (arr >= 2000).sum() / (arr > 0).sum()
    return frac_built
