from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import pandas as pd
from tqdm import tqdm
import os

# Константы для настроек
COOKIE_TIMEOUT = 2  # Уменьшено с 3
ELEMENT_TIMEOUT = 5  # Уменьшено с 10
RESULT_TIMEOUT = 3   # Уменьшено с 5
SHORT_DELAY = 0.5    # Уменьшено с 1-2

def load_progress():
    if os.path.exists('progress.json'):
        with open('progress.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'last_index': 0, 'results': []}

def save_progress(last_index, results):
    with open('progress.json', 'w', encoding='utf-8') as f:
        json.dump({'last_index': last_index, 'results': results}, f, ensure_ascii=False, indent=4)

df = pd.read_csv('RAION.TXT', sep='>', encoding='windows-1251', header=None)
districts = list(set(df[2].str.split().str[0].to_list()))



progress = load_progress()
last_index = progress['last_index']
results = progress['results']

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_argument("--disable-notifications")
# options.add_argument("--headless")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

try:
    for index, district in enumerate(tqdm(districts[last_index:], initial=last_index, total=len(districts))):   
        print(f"Загружаем {district} район")
        driver.get(f"https://aotek.spb.ru/grafik/?district={district}&street=&house=")
        time.sleep(3)
        
        # Ожидание результатов с уменьшенным таймаутом
        try:
            WebDriverWait(driver, RESULT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".graph")))
            
            table = driver.find_element(By.CSS_SELECTOR, ".graph")
            rows = table.find_elements(By.TAG_NAME, "tr")
            print('Число строк ', len(rows))
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    data = {
                        "number_index": cols[0].text,
                        "district": cols[1].text,
                        "street": cols[2].text,
                        "house_number": cols[3].text,
                        "korpus":cols[4].text,
                        "liter":cols[5].text,
                        "period-ppr":cols[6].text,
                        "period-ispit":cols[7].text,
                    }
                    results.append(data)
                    
            save_progress(index + 1, results)
            time.sleep(SHORT_DELAY)  

            
    
        except Exception as e:
            print(f"Не удалось найти результаты для района {district}")
            save_progress(index + 1, results)
            time.sleep(SHORT_DELAY)    
            continue
    
    # Финальное сохранение
    with open('tek_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    if os.path.exists('progress.json'):
        os.remove('progress.json')       
   

finally:
    driver.quit()
    print("Браузер закрыт")
    
    
    
