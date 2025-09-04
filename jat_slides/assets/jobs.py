from upath import UPath as Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from jat_slides.partitions import zone_partitions
from jat_slides.resources import PathResource

from cfc_core_utils import gdal_azure_session, storage_options

@dg.asset(name="geo", key_prefix="jobs", io_manager_key="geoparquet_manager")
def jobs_geo(path_resource: PathResource) -> gpd.GeoDataFrame:
    jobs_path = Path(path_resource.jobs_path) / "denue_2023_estimaciones.parquet"
    return gpd.GeoDataFrame(
        (
            pd.read_parquet(
                jobs_path,
                columns=["num_empleos_esperados", "longitud", "latitud"],
                storage_options=storage_options(jobs_path)
            )
            .assign(geometry=lambda x: gpd.points_from_xy(x["longitud"], x["latitud"]))
            .drop(columns=["longitud", "latitud"])
        ),
        crs="EPSG:4326",
        geometry="geometry",
    ).to_crs("EPSG:6372")


@dg.asset(
    name="partitioned",
    key_prefix="jobs",
    ins={
        "jobs": dg.AssetIn(["jobs", "geo"]),
        "df_agebs": dg.AssetIn(["agebs", "2020"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
)
def jobs_partitioned(
    context: dg.AssetExecutionContext,
    jobs: gpd.GeoDataFrame,
    df_agebs: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    
    # Asegurar geometrías válidas y CRS coherente
    jobs = jobs.dropna(subset=["geometry"])
    if df_agebs.crs != jobs.crs:
        df_agebs = df_agebs.to_crs(jobs.crs)

    # Pre-filtro por bbox de la partición 
    minx, miny, maxx, maxy = df_agebs.total_bounds
    jobs_pref = jobs.cx[minx:maxx, miny:maxy].copy()

    context.log.info(
        f"[jobs_partitioned] prefilter -> {len(jobs_pref)} / {len(jobs)} puntos "
        f"({(len(jobs_pref)/max(len(jobs),1))*100:.2f}% del total)"
    )

    return gpd.GeoDataFrame(
        jobs_pref.sjoin(
            df_agebs[["geometry"]],
            how="inner",
            predicate="within",
        ).drop(columns=["index_right"])
    )


@dg.asset(
    name="reprojected",
    key_prefix="jobs",
    ins={
        "jobs": dg.AssetIn(["jobs", "partitioned"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
)
def jobs_reprojected(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
    jobs: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    mesh_path = (
        Path(path_resource.pg_path)
        / "reprojected"
        / "base"
        / "2020"
        / f"{context.partition_key}.gpkg"
    )

    with gdal_azure_session(path=mesh_path):
        df_mesh = gpd.read_file(mesh_path).drop(columns=["pop_fraction"])
    joined = df_mesh.sjoin(jobs, how="inner", predicate="contains").drop(
        columns=["index_right"],
    )
    job_count = joined.groupby("codigo")["num_empleos_esperados"].sum()

    return (
        df_mesh.set_index("codigo")
        .assign(jobs=job_count)
        .dropna(subset=["jobs"])
        .reset_index()
    )
