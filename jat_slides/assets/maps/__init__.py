import dagster as dg

from jat_slides.assets.maps import built, income, jobs, population_grid_plot


defs = dg.Definitions(
    assets=(
        dg.load_assets_from_modules([population_grid_plot])
        + dg.load_assets_from_modules([income])
        + dg.load_assets_from_modules([jobs])
        + dg.load_assets_from_modules([built])
    )
)
