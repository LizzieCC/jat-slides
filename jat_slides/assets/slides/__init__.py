from dagster import load_assets_from_modules, Definitions
from jat_slides.assets.slides import base


defs = Definitions(assets=load_assets_from_modules([base], group_name="slides"))
