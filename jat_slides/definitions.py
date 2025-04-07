import dagster as dg
import jat_slides.managers as managers
import jat_slides.resources as resources

from jat_slides.assets import agebs, built, cells, jobs, maps, muns, slides, stats


# Definitions
defs = dg.Definitions.merge(
    dg.Definitions(
        assets=(
            dg.load_assets_from_modules([agebs], group_name="agebs")
            + dg.load_assets_from_modules([built], group_name="built_rasters")
            + dg.load_assets_from_modules([muns], group_name="muns")
            + dg.load_assets_from_modules([cells], group_name="cells")
            + dg.load_assets_from_modules([slides], group_name="slides")
            + dg.load_assets_from_modules([jobs], group_name="jobs")
        ),
    ),
    agebs.defs,
    maps.defs,
    stats.defs,
    managers.defs,
    resources.defs,
)
