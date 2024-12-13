import numpy as np
import pandas as pd

from affine import Affine
from dagster import asset, AssetIn


@asset(
    name="built_after_2000",
    key_prefix="stats",
    ins={"built_data_map": AssetIn("built")},
    io_manager_key="csv_manager",
)
def built_after_2000(built_data_map: dict[str, tuple[np.ndarray, Affine]]):
    df = []
    for zone, built_data in built_data_map.items():
        arr, _ = built_data
        frac_built = (arr >= 2000).sum() / (arr > 0).sum()
        df.append(dict(zone=zone, built=frac_built))

    df = pd.DataFrame(df)
    return df
