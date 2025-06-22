import os
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from tqdm import tqdm

from config import RGIS_CRS_FILE

DUMP_RYD = Path("dump_ryd")
DUMP_OAS = Path("dump_id")
OUTPUT_MAP = "map_oas_buildings.html"

def load_dumps(dump_path: Path, label: str) -> pd.DataFrame:
    df_list = []
    for file in dump_path.glob("*.pickle"):
        df = pd.read_pickle(file)
        df = df[df["idrgis"].notna()].copy()
        df["Тип"] = label
        df_list.append(df)
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    geoms = []
    ids = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Создание GeoDataFrame"):
        features = row.get("coord_info", {}).get("features", [])
        for feature in features:
            try:
                geom = shape(feature["geometry"])
                geoms.append(geom)
                ids.append(row["idrgis"])
            except Exception:
                continue

    gdf = gpd.GeoDataFrame(geometry=geoms)
    gdf["idrgis"] = ids
    return gdf

def build_map():
    print("🔹 Загружаем рядовые...")
    df1 = load_dumps(DUMP_RYD, label="Рядовые средовые")
    print("🔹 Загружаем средовые...")
    df2 = load_dumps(DUMP_OAS, label="Ценные средовые")

    df_all = pd.concat([df1, df2], ignore_index=True)
    gdf_geom = to_geodataframe(df_all)

    gdf_full = gdf_geom.merge(df_all, on="idrgis", how="right")

    # Устанавливаем систему координат
    with open(RGIS_CRS_FILE, encoding="utf-8") as f:
        wkt = f.read()

    gdf_full = gdf_full.set_crs(wkt).to_crs("EPSG:4326")

    # Сохраняем интерактивную карту
    m = gdf_full.explore(column="Тип", tooltip=["Тип"], cmap="Set2")
    m.save(OUTPUT_MAP)

    print(f"✅ HTML-карта сохранена в: {OUTPUT_MAP}")

if __name__ == "__main__":
    build_map()
