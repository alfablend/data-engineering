import pandas as pd
from shapely.geometry import Polygon
import json
from tqdm import tqdm
from more_itertools import sliced
from pathlib import Path

from rosreestr2coord import Area

# --- Константы ---
INPUT_FILE = Path("gradplan.csv")
DUMP_DIR = Path("dump_gradplan")
SLICE_SIZE = 10
TARGET_REGIONS = [
    "78:36", "78:10", "78:15", "78:11", "78:14", "78:12",
    "78:34", "78:13", "78:07", "78:06", "78:31", "78:32"
]  # Спальники

# --- Получение геометрии по КН ---
def fetch_polygon(kad_number: str):
    try:
        area = Area(kad_number, with_proxy=True, use_cache=True)
        geojson = area.to_geojson_poly()
        coords = json.loads(geojson)["geometry"]["coordinates"][0]
        return Polygon(coords)
    except Exception:
        return None

def main():
    df = pd.read_csv(INPUT_FILE)
    # Исключаем спальные районы
    mask = ~df["Кадастровый номер"].str.contains("|".join(TARGET_REGIONS), regex=True)
    df = df[mask].copy()
    tqdm.pandas()

    DUMP_DIR.mkdir(exist_ok=True)

    for index in tqdm(list(sliced(range(len(df)), SLICE_SIZE)), desc="Получение градпланов"):
        out_path = DUMP_DIR / f"df{index}.pickle"
        if out_path.exists():
            continue

        df_slice = df.iloc[index].copy()
        df_slice["geometry"] = df_slice["Кадастровый номер"].progress_apply(fetch_polygon)
        df_slice.to_pickle(out_path)

if __name__ == "__main__":
    main()
