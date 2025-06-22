import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from pathlib import Path
from tqdm import tqdm

OUTPUT_FILE = "map_gradplan_plots.html"
DUMP_DIR = Path("dump_gradplan")
BACKUP_FILE = Path("gdf_gradplan.pickle")

def load_all_pickles(folder: Path) -> pd.DataFrame:
    df_list = []
    for pickle_file in folder.glob("*.pickle"):
        df = pd.read_pickle(pickle_file)
        df_list.append(df)
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def main():
    print("Загружаем куски из dump_gradplan/...")
    df_live = load_all_pickles(DUMP_DIR)

    df_combined = df_live
    if BACKUP_FILE.exists():
        print("Добавляем резервную выборку...")
        df_backup = pd.read_pickle(BACKUP_FILE)
        df_combined = pd.concat([df_live, df_backup], ignore_index=True)

    tqdm.pandas()
    gdf = gpd.GeoDataFrame(df_combined.progress_apply(lambda x: x), geometry="geometry")
    gdf = gdf.set_crs("EPSG:4326")

    # Сохраняем интерактивную карту
    print("Генерируем карту градпланов...")
    m = gdf.explore(tooltip=["Кадастровый номер"], cmap="YlOrRd")
    m.save(OUTPUT_FILE)

    print(f"Карта сохранена в: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
