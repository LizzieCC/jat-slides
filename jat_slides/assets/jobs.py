from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from jat_slides.partitions import zone_partitions
from jat_slides.resources import PathResource


@dg.asset(name="geo", key_prefix="jobs", io_manager_key="gpkg_manager")
def jobs_geo(path_resource: PathResource) -> gpd.GeoDataFrame:
    jobs_path = Path(path_resource.jobs_path) / "denue_2023_estimaciones.csv"

    return gpd.GeoDataFrame(
        (
            pd.read_csv(
                jobs_path,
                usecols=["num_empleos_esperados", "longitud", "latitud"],
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
    jobs: gpd.GeoDataFrame,
    df_agebs: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        jobs.sjoin(
            df_agebs[["geometry"]].to_crs(jobs.crs),
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
