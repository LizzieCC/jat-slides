import dagster as dg
from jat_slides.assets.maps import built, income, jobs, population_grid

defs = dg.Definitions(
    assets=(
        list(dg.load_assets_from_modules([population_grid]))
        + list(dg.load_assets_from_modules([income]))
        + list(dg.load_assets_from_modules([jobs]))
        + list(dg.load_assets_from_modules([built]))
    ),
)
