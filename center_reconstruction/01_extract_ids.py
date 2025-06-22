import os
from pathlib import Path

import pandas as pd
import json
from tqdm import tqdm 
from more_itertools import sliced
import requests

from config import HEADERS, COOKIES

# --- Константы ---
INPUT_DIR = Path("Ценные рядовые объекты")
DUMP_DIR = Path("dump_id")
SLICE_SIZE = 10
LAYER_NAME = "OAS"

# --- Предобработка адреса ---
def normalize_address(addr: str) -> str:
    s = ' '.join(addr.split()[2:])
    replacements = {
        'наб. р.': 'набережная реки',
        'М. Сампсониевский': 'Малый Сампсониевский',
        'М.': 'Малая',
        'Ср.': 'Средняя',
        'Б. Сампсониевский': 'Большой Сампсониевский',
        'Б.': 'Большая',
        'наб.': 'набережная',
        'кан.': 'канала',
        'пл.': 'площадь',
        'бульв.': 'бульвар',
        'корп.': 'корпус',
        'ул.': 'улица',
        'пр.': 'проспект',
        'пер.': 'переулок',
        'д.': 'дом',
        ' (часть здания)': ''
    }
    for k, v in replacements.items():
        s = s.replace(k, v)

    s = s.replace(' (лицевой фасад)', '')
    return s

# --- Поиск ID в RGIS ---
def fetch_id_from_rgis(address: str) -> str | None:
    json_data = {
        'layerName': LAYER_NAME,
        'prfCode': -1,
        'searchString': address,
        'count': 100,
        'includePrefix': False,
    }

    try:
        response = requests.post(
            'https://rgis.spb.ru/mapui/map_api/search/',
            cookies=COOKIES,
            headers=HEADERS,
            json=json_data,
            timeout=10,
            verify=False
        )
        results = response.json()
    except Exception as e:
        print(f"Ошибка при запросе для адреса: {address} — {e}")
        return None

    if not results:
        return None

    try:
        shortest_name = min((obj["prfName"] for obj in results), key=len)
        match = next(obj for obj in results if obj["prfName"] == shortest_name)
        return match["id"]
    except Exception:
        return None

# --- Чтение txt-файлов с адресами ---
def load_address_dataframe() -> pd.DataFrame:
    df_list = []
    for txt_file in INPUT_DIR.rglob("*.txt"):
        df = pd.read_csv(txt_file, sep="8899998", header=None, engine="python")
        df["район"] = txt_file.name
        df_list.append(df)

    full_df = pd.concat(df_list, ignore_index=True)
    full_df["часть здания"] = full_df[0].str.contains("часть здания|лицевой|восстановленный", regex=True)
    full_df["addr4rgis"] = full_df[0].apply(normalize_address)
    return full_df

# --- Основной цикл выгрузки ---
def main():
    df = load_address_dataframe()
    tqdm.pandas()

    slices = sliced(range(len(df)), SLICE_SIZE)
    DUMP_DIR.mkdir(exist_ok=True)

    for index in tqdm(list(slices), desc="Общий ход"):
        dump_path = DUMP_DIR / f"df{index}.pickle"
        if dump_path.exists():
            continue

        df_slice = df.iloc[index].copy()
        df_slice["idrgis"] = df_slice["addr4rgis"].progress_apply(fetch_id_from_rgis)
        df_slice.to_pickle(dump_path)

if __name__ == "__main__":
    main()
