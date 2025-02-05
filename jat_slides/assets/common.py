import geopandas as gpd
import pandas as pd

from jat_slides.resources import PathResource
from pathlib import Path


def get_mun_cells(partition_key: str, path_resource: PathResource, agebs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if len(partition_key) == 4:
        ent = partition_key[0].rjust(2, "0")
    else:
        ent = partition_key[:2]

    diff_path = Path(path_resource.pg_path) / "differences/2000_2020"
    df = []
    for path in diff_path.glob(f"{ent}.*.gpkg"):
        temp = gpd.read_file(path)
        df.append(temp)
    df = pd.concat(df)

    joined = df.sjoin(agebs.to_crs("EPSG:6372"), how="inner", predicate="intersects")["codigo"].unique()
    df = df[df["codigo"].isin(joined)]
    return df