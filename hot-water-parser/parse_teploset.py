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
COOKIE_TIMEOUT = 2  
ELEMENT_TIMEOUT = 5  
RESULT_TIMEOUT = 3  
SHORT_DELAY = 0.5   

def load_progress():
    if os.path.exists('progress.json'):
        with open('progress.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'last_index': 0, 'results': []}

def save_progress(last_index, results):
    with open('progress.json', 'w', encoding='utf-8') as f:
        json.dump({'last_index': last_index, 'results': results}, f, ensure_ascii=False, indent=4)

df = pd.read_csv('GEONIM.TXT', sep='>', encoding='windows-1251', header=None)
streets = df[4].to_list()

progress = load_progress()
last_index = progress['last_index']
results = progress['results']

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_argument("--disable-notifications")
# options.add_argument("--headless")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

try:
    print("Открываем страницу...")
    driver.get("https://www.teplosetspb.ru/summer_campaign")
    
    # Обработка cookies с уменьшенным таймаутом
    try:
        print("Пытаемся найти кнопку cookies...")
        cookie_selectors = [
            ".cc_btn",
            "button.cookie-btn",
            "button.cookie__btn",
            "button.cookies-btn",
            "button.cookie-accept",
            "button.js-cookie-accept",
            "#cookie-accept",
            ".cookie-popup__button"
        ]
        
        for selector in cookie_selectors:
            try:
                cookie_btn = WebDriverWait(driver, COOKIE_TIMEOUT).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                cookie_btn.click()
                print(f"Приняли cookies с помощью селектора: {selector}")
                time.sleep(SHORT_DELAY)
                break
            except:
                continue
        else:
            print("Не удалось найти кнопку принятия cookies. Продолжаем без этого.")
    except Exception as e:
        print(f"Ошибка при принятии cookies: {str(e)}")

    # Основная часть
    try:
        print("Ищем поле ввода...")
        input_field = WebDriverWait(driver, ELEMENT_TIMEOUT).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input.summer__form-input")))
        
        for index, street in enumerate(tqdm(streets[last_index:], initial=last_index, total=len(streets))):
            try:
                input_field.clear()
                input_field.send_keys(street)
                
                button = WebDriverWait(driver, ELEMENT_TIMEOUT).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".summer__form-button")))
                button.click()
                
                # Ожидание результатов с уменьшенным таймаутом
                try:
                    WebDriverWait(driver, RESULT_TIMEOUT).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".summer__table > tbody:nth-child(2)")))
                    
                    table = driver.find_element(By.CSS_SELECTOR, ".summer__table > tbody:nth-child(2)")
                    rows = table.find_elements(By.TAG_NAME, "tr")
                    
                    for row in rows:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if len(cols) >= 2:
                            data = {
                                "street": street,
                                "address": cols[0].text,
                                "period": cols[1].text,
                            }
                            results.append(data)
                            print(data)
                    
                except Exception as e:
                    print(f"Не удалось найти результаты для улицы {street}")
                    continue
                
                save_progress(index + 1, results)
                time.sleep(SHORT_DELAY)
                
            except Exception as e:
                print(f"Ошибка при обработке улицы {street}: {str(e)}")
                save_progress(index, results)
                continue
        
        # Финальное сохранение
        with open('teploset_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        
        if os.path.exists('progress.json'):
            os.remove('progress.json')
    
    except Exception as e:
        print(f"Ошибка при работе с формой: {str(e)}")
        save_progress(last_index, results)

finally:
    driver.quit()
    print("Браузер закрыт")