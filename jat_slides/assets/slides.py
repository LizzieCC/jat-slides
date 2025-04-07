import datetime

import dagster as dg
import numpy as np
import pandas as pd

from babel.dates import format_date
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.presentation import Presentation as PresentationType
from pptx.shapes.shapetree import SlideShapes
from pptx.slide import SlideLayout
from pptx.text.text import TextFrame
from pptx.util import Cm, Pt
from dagster import asset, AssetIn
from pathlib import Path
from typing import Optional

from jat_slides.partitions import zone_partitions
from jat_slides.resources import ZonesListResource, ZonesMapStrResource


def find_layouts(pres: PresentationType) -> dict[str, SlideLayout]:
    layouts = {}
    for layout in pres.slide_layouts:
        name = layout.name
        if name == "Title Slide":
            layouts["title"] = layout
        elif name == "Map and Content":
            layouts["pop"] = layout
        elif name == "Map and Table":
            layouts["built"] = layout
        elif name == "Section Divider":
            layouts["section"] = layout
        elif name == "Income":
            layouts["income"] = layout
        elif name == "Jobs":
            layouts["jobs"] = layout
    return layouts


def find_shape(shapes: SlideShapes, prefix: str):
    for shape in shapes:
        if shape.name.startswith(prefix):
            return shape


def add_title_slide(pres: PresentationType, title_layout: SlideLayout) -> None:
    title = pres.slides.add_slide(title_layout)

    placeholders = title.shapes
    placeholders[0].text = "Dinámicas Urbanas"
    placeholders[2].text = format_date(
        datetime.date.today(), locale="es", format="long"
    )


def add_section_slide(
    pres: PresentationType, section_layout: SlideLayout, section_name: str
) -> None:
    section = pres.slides.add_slide(section_layout)
    section.shapes[0].text = section_name


def add_highlighted_text_to_frame(
    frame: TextFrame, *, text_left: str, highlight: float, text_right: str
) -> None:
    p = frame.paragraphs[0]
    run = p.add_run()
    run.text = text_left

    run = p.add_run()
    run.text = f"{highlight:.0%}"
    run.font.size = Pt(28)
    run.font.bold = True
    run.font.color.rgb = RGBColor(0x00, 0x70, 0xC0)

    run = p.add_run()
    run.text = text_right


def add_lost_pop_slide(
    pres: PresentationType,
    layout: SlideLayout,
    lost_pop_frac: float,
    picture_path: Path,
) -> None:
    pop_slide = pres.slides.add_slide(layout)

    text_shape = find_shape(pop_slide.shapes, prefix="Text")
    frame: TextFrame = text_shape.text_frame
    frame.clear()

    add_highlighted_text_to_frame(
        frame,
        text_left="El ",
        highlight=lost_pop_frac,
        text_right=" de la superficie de 2020 perdió población con respecto al 2000.",
    )

    if picture_path.exists():
        figure_shape = find_shape(pop_slide.shapes, prefix="Picture")
        figure_shape.insert_picture(str(picture_path))


# pylint: disable=protected-access
def add_built_slide(
    pres: PresentationType,
    layout: SlideLayout,
    built_after_frac: float,
    *,
    pop_df: pd.DataFrame,
    built_area_df: pd.DataFrame,
    urban_area_df: pd.DataFrame,
    picture_path: Path,
) -> None:
    pop_df = pop_df.set_index("year")["pop"] / 1e6
    built_area_df = built_area_df.set_index("year")["area"] / 1e6
    urban_area_df = urban_area_df.set_index("year")["area"] / 1e6

    pop_change = (pop_df.loc[2020] - pop_df.loc[2000]) / pop_df.loc[2000] + 1
    built_change = (
        built_area_df.loc[2020] - built_area_df.loc[2000]
    ) / built_area_df.loc[2000] + 1
    urban_change = (
        urban_area_df.loc[2020] - urban_area_df.loc[2000]
    ) / urban_area_df.loc[2000] + 1

    built_slide = pres.slides.add_slide(layout)

    text_shape = find_shape(built_slide.shapes, prefix="Text")
    frame: TextFrame = text_shape.text_frame
    frame.clear()

    add_highlighted_text_to_frame(
        frame,
        text_left="El ",
        highlight=built_after_frac,
        text_right=" de los píxeles se urbanizaron en el año 2000 o posterior.",
    )

    table_shape = find_shape(built_slide.shapes, prefix="Table")
    table_frame = table_shape.insert_table(rows=4, cols=4)
    table = table_frame.table

    table.columns[0].width = Cm(1.8)
    table.columns[1].width = Cm(3.0)
    table.columns[2].width = Cm(3.0)
    table.columns[3].width = Cm(2.9)

    tbl = table_frame._element.graphic.graphicData.tbl
    tbl[0][-1].text = "{2D5ABB26-0587-4C30-8999-92F81FD0307C}"

    text_arr = np.array(
        [
            [
                "",
                "Millones de habitantes",
                "Superficie  construida (km²)",
                "Superficie urbana (km²)",
            ],
            [
                "2000",
                f"{pop_df.loc[2000]:.2f}",
                f"{built_area_df.loc[2000]:.1f}",
                f"{urban_area_df.loc[2000]:.1f}",
            ],
            [
                "2020",
                f"{pop_df.loc[2020]:.2f}",
                f"{built_area_df.loc[2020]:.1f}",
                f"{urban_area_df.loc[2020]:.1f}",
            ],
            ["", f"x{pop_change:.2f}", f"x{built_change:.2f}", f"x{urban_change:.2f}"],
        ]
    )

    style_arr = np.array(
        [
            [False, True, True, True],
            [True, False, False, False],
            [True, False, False, False],
            [False, True, True, True],
        ]
    )

    for row_idx in range(text_arr.shape[0]):
        for col_idx in range(text_arr.shape[1]):
            cell = table.cell(row_idx, col_idx)
            p = cell.text_frame.paragraphs[0]
            run = p.add_run()
            run.text = text_arr[row_idx, col_idx]
            run.font.size = Pt(14)

            if style_arr[row_idx, col_idx]:
                run.font.bold = True
                run.font.color.rgb = RGBColor(0x00, 0x70, 0xC0)

    if picture_path.exists():
        figure_shape = find_shape(built_slide.shapes, prefix="Picture")
        figure_shape.insert_picture(str(picture_path))


def add_single_picture_slide(
    pres: PresentationType, layout: SlideLayout, *, picture_path: Path
):
    income_slide = pres.slides.add_slide(layout)
    if picture_path.exists():
        figure_shape = find_shape(income_slide.shapes, prefix="Picture")
        figure_shape.insert_picture(str(picture_path))


def generate_slides(
    *,
    id_list: list[str],
    name_map: ZonesMapStrResource,
    lost_pop_after_2000: dict[str, float],
    built_after_2000: dict[str, float],
    pop_df: dict[str, pd.DataFrame],
    built_df: dict[str, pd.DataFrame],
    built_urban_df: dict[str, pd.DataFrame],
    pg_figure_paths: dict[str, Path],
    built_figure_paths: dict[str, Path],
    income_figure_paths: dict[str, Path] | None = None,
    jobs_figure_paths: dict[str, Path] | None = None,
):
    pres = Presentation("./template.pptx")
    layouts = find_layouts(pres)

    add_title_slide(pres, layouts["title"])

    for zone in id_list:
        add_section_slide(pres, layouts["section"], name_map.zones[zone])

        lost_pop_frac = lost_pop_after_2000[zone]
        add_lost_pop_slide(pres, layouts["pop"], lost_pop_frac, pg_figure_paths[zone])

        built_after_frac = built_after_2000[zone]
        add_built_slide(
            pres,
            layouts["built"],
            built_after_frac,
            pop_df=pop_df[zone],
            built_area_df=built_df[zone],
            urban_area_df=built_urban_df[zone],
            picture_path=built_figure_paths[zone],
        )

        if income_figure_paths is not None:
            add_single_picture_slide(
                pres, layouts["income"], picture_path=income_figure_paths[zone]
            )

        if jobs_figure_paths is not None:
            add_single_picture_slide(
                pres, layouts["jobs"], picture_path=jobs_figure_paths[zone]
            )

    return pres


def generate_single_slide(
    *,
    name: str,
    lost_pop_after_2000: float,
    built_after_2000: float,
    pop_df: pd.DataFrame,
    built_df: pd.DataFrame,
    built_urban_df: pd.DataFrame,
    built_figure_path: Path,
    pg_figure_path: Path,
    income_figure_path: Path | None,
    jobs_figure_path: Path | None,
):
    pres = Presentation("./template.pptx")
    layouts = find_layouts(pres)

    add_section_slide(pres, layouts["section"], name)

    add_lost_pop_slide(pres, layouts["pop"], lost_pop_after_2000, pg_figure_path)

    add_built_slide(
        pres,
        layouts["built"],
        built_after_2000,
        pop_df=pop_df,
        built_area_df=built_df,
        urban_area_df=built_urban_df,
        picture_path=built_figure_path,
    )

    if income_figure_path is not None:
        add_single_picture_slide(
            pres, layouts["income"], picture_path=income_figure_path
        )

    if jobs_figure_path is not None:
        add_single_picture_slide(pres, layouts["jobs"], picture_path=jobs_figure_path)

    return pres


@asset(
    ins={
        "lost_pop_after_2000": AssetIn(key=["stats", "lost_pop_after_2000"]),
        "built_after_2000": AssetIn(key=["stats", "built_after_2000"]),
        "pop_df": AssetIn(key=["stats", "population"]),
        "built_df": AssetIn(key=["stats", "built_area"]),
        "built_urban_df": AssetIn(key=["stats", "built_urban_area"]),
        "pg_figure_path": AssetIn(
            key=["plot", "population_grid"], input_manager_key="path_manager"
        ),
        "built_figure_path": AssetIn(
            key=["plot", "built"], input_manager_key="path_manager"
        ),
        "income_figure_path": AssetIn(
            key=["plot", "income"], input_manager_key="path_manager"
        ),
        "jobs_figure_path": AssetIn(
            key=["plot", "jobs"], input_manager_key="path_manager"
        ),
    },
    partitions_def=zone_partitions,
    io_manager_key="presentation_manager",
)
def slides(
    context: dg.AssetExecutionContext,
    zone_names_resource: ZonesMapStrResource,
    lost_pop_after_2000: float,
    built_after_2000: float,
    pop_df: pd.DataFrame,
    built_df: pd.DataFrame,
    built_urban_df: pd.DataFrame,
    pg_figure_path: Path,
    built_figure_path: Path,
    income_figure_path: Path,
    jobs_figure_path: Path,
) -> PresentationType:
    return generate_single_slide(
        name=zone_names_resource.zones[context.partition_key],
        lost_pop_after_2000=lost_pop_after_2000,
        built_after_2000=built_after_2000,
        pop_df=pop_df,
        built_df=built_df,
        built_urban_df=built_urban_df,
        built_figure_path=built_figure_path,
        pg_figure_path=pg_figure_path,
        income_figure_path=income_figure_path,
        jobs_figure_path=jobs_figure_path,
    )
    # return generate_slides(
    #     id_list=wanted_zones_resource.zones,
    #     name_map=zone_names_resource,
    #     lost_pop_after_2000=lost_pop_after_2000,
    #     built_after_2000=built_after_2000,
    #     pop_df=pop_df,
    #     built_df=built_df,
    #     built_urban_df=built_urban_df,
    #     pg_figure_paths=pg_figure_paths,
    #     built_figure_paths=built_figure_paths,
    #     income_figure_paths=income_figure_paths,
    #     jobs_figure_paths=jobs_figure_paths,
    # )


@asset(
    ins={
        "lost_pop_after_2000": AssetIn(key=["stats_mun", "lost_pop_after_2000"]),
        "built_after_2000": AssetIn(key=["stats_mun", "built_after_2000"]),
        "pop_df": AssetIn(key=["stats_mun", "population"]),
        "built_df": AssetIn(key=["stats_mun", "built_area"]),
        "built_urban_df": AssetIn(key=["stats_mun", "built_urban_area"]),
        "pg_figure_paths": AssetIn(
            key=["plot_mun", "population_grid"], input_manager_key="path_manager"
        ),
        "built_figure_paths": AssetIn(
            key=["plot_mun", "built"], input_manager_key="path_manager"
        ),
        "income_figure_paths": AssetIn(
            key=["plot_mun", "income"], input_manager_key="path_manager"
        ),
        "jobs_figure_paths": AssetIn(
            key=["plot_mun", "jobs"], input_manager_key="path_manager"
        ),
    },
    io_manager_key="presentation_manager",
)
def slides_mun(
    wanted_muns_resource: ZonesListResource,
    mun_names_resource: ZonesMapStrResource,
    lost_pop_after_2000: dict[str, Optional[float]],
    built_after_2000: dict[str, Optional[float]],
    pop_df: dict[str, Optional[pd.DataFrame]],
    built_df: dict[str, Optional[pd.DataFrame]],
    built_urban_df: dict[str, Optional[pd.DataFrame]],
    pg_figure_paths: dict[str, Path],
    built_figure_paths: dict[str, Path],
    income_figure_paths: dict[str, Path],
    jobs_figure_paths: dict[str, Path],
) -> PresentationType:
    return generate_slides(
        id_list=wanted_muns_resource.zones,
        name_map=mun_names_resource,
        lost_pop_after_2000=lost_pop_after_2000,
        built_after_2000=built_after_2000,
        pop_df=pop_df,
        built_df=built_df,
        built_urban_df=built_urban_df,
        pg_figure_paths=pg_figure_paths,
        built_figure_paths=built_figure_paths,
        income_figure_paths=income_figure_paths,
        jobs_figure_paths=jobs_figure_paths,
    )


@asset(
    ins={
        "lost_pop_after_2000": AssetIn(key=["stats_trimmed", "lost_pop_after_2000"]),
        "built_after_2000": AssetIn(key=["stats_trimmed", "built_after_2000"]),
        "pop_df": AssetIn(key=["stats_trimmed", "population"]),
        "built_df": AssetIn(key=["stats_trimmed", "built_area"]),
        "built_urban_df": AssetIn(key=["stats_trimmed", "built_urban_area"]),
        "pg_figure_paths": AssetIn(
            key=["plot_trimmed", "population_grid"], input_manager_key="path_manager"
        ),
        "built_figure_paths": AssetIn(
            key=["plot_trimmed", "built"], input_manager_key="path_manager"
        ),
        "income_figure_paths": AssetIn(
            key=["plot_trimmed", "income"], input_manager_key="path_manager"
        ),
        "jobs_figure_paths": AssetIn(
            key=["plot_trimmed", "jobs"], input_manager_key="path_manager"
        ),
    },
    io_manager_key="presentation_manager",
)
def slides_trimmed(
    wanted_trimmed_resource: ZonesListResource,
    zone_names_resource: ZonesMapStrResource,
    lost_pop_after_2000: dict[str, Optional[float]],
    built_after_2000: dict[str, Optional[float]],
    pop_df: dict[str, Optional[pd.DataFrame]],
    built_df: dict[str, Optional[pd.DataFrame]],
    built_urban_df: dict[str, Optional[pd.DataFrame]],
    pg_figure_paths: dict[str, Path],
    built_figure_paths: dict[str, Path],
    income_figure_paths: dict[str, Path],
    jobs_figure_paths: dict[str, Path],
) -> PresentationType:
    return generate_slides(
        id_list=wanted_trimmed_resource.zones,
        name_map=zone_names_resource,
        lost_pop_after_2000=lost_pop_after_2000,
        built_after_2000=built_after_2000,
        pop_df=pop_df,
        built_df=built_df,
        built_urban_df=built_urban_df,
        pg_figure_paths=pg_figure_paths,
        built_figure_paths=built_figure_paths,
        income_figure_paths=income_figure_paths,
        jobs_figure_paths=jobs_figure_paths,
    )
