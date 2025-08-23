import os
from pathlib import Path
import geopandas as gpd
import json

mun_2020_path = Path(os.getenv("POPULATION_GRIDS_PATH")) / "framework/mun/2020.gpkg"
df_mun = gpd.read_file(mun_2020_path)
#mun_list = df_mun["CVEGEO"].sort_values().astype(str).to_numpy().tolist()
#print(mun_list)

codes = sorted({str(x) for x in df_mun["CVEGEO"].astype(str)})
with open("mun_2020.json", "w", encoding="utf-8") as f:
    json.dump({"CVEGEO": codes}, f, ensure_ascii=False, indent=2)
