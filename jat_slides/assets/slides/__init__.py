from dagster import load_assets_from_modules, Definitions
from jat_slides.assets.slides import base, merged


defs = Definitions(assets=load_assets_from_modules([base, merged], group_name="slides"))
