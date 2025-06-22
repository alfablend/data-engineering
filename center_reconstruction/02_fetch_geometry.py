import pandas as pd
import json
from tqdm import tqdm
from pathlib import Path
from more_itertools import sliced
from config import RGIS_HEADERS, COOKIES, RGIS_AUTHKEY
import requests

# --- Константы ---
INPUT_FILE = Path("df_ryd.csv")
DUMP_DIR = Path("dump_ryd")
SLICE_SIZE = 10

# --- Функция запроса геометрии по RGIS id ---
def fetch_coords(prf_id):
    try:
        url = (
            f"https://gs1.rgis.spb.ru/geoserver/wfs?"
            f"service=wfs&version=2.0.0&OUTPUTFORMAT=application/json&"
            f"REQUEST=GetFeature&TYPENAME=rgis:OAS&FeatureId=OAS.{int(prf_id)}&authkey={RGIS_AUTHKEY}"
        )
        response = requests.get(url, cookies=COOKIES, headers=RGIS_HEADERS, timeout=10, verify=False)
        return json.loads(response.content)
    except Exception as e:
        print(f"Ошибка для ID {prf_id}: {e}")
        return None

def main():
    df = pd.read_csv(INPUT_FILE)
    df = df[df["idrgis"].notna()]

    tqdm.pandas()
    DUMP_DIR.mkdir(exist_ok=True)

    for index in tqdm(list(sliced(range(len(df)), SLICE_SIZE)), desc="Общий ход"):
        dump_path = DUMP_DIR / f"df{index}.pickle"
        if dump_path.exists():
            continue

        df_slice = df.iloc[index].copy()
        df_slice["coord_info"] = df_slice["idrgis"].progress_apply(fetch_coords)
        df_slice.to_pickle(dump_path)

if __name__ == "__main__":
    main()
