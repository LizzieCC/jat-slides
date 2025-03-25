import dagster as dg

from jat_slides.assets.stats import (
    built_area,
    built_after_2000,
    built_urban_area,
    lost_pop_after_2000,
    population,
)

defs = dg.Definitions(
    assets=(
        dg.load_assets_from_modules([built_area])
        + dg.load_assets_from_modules([built_after_2000])
        + dg.load_assets_from_modules([built_urban_area])
        + dg.load_assets_from_modules([lost_pop_after_2000])
        + dg.load_assets_from_modules([population])
    )
)
