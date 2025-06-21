import dagster as dg
from jat_slides.assets.agebs import base, trimmed

defs = dg.Definitions(
    assets=(
        list(dg.load_assets_from_modules([base], group_name="agebs_base"))
        + list(dg.load_assets_from_modules([trimmed], group_name="agebs_trimmed"))
    ),
)
