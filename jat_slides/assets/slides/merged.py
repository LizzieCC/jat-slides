from dagster import asset, AssetExecutionContext
from jat_slides.resources import ZonesResource


@asset(name="merged", key_prefix="slides")
def slides_merged(context: AssetExecutionContext, zones_resource: ZonesResource):
    pass
