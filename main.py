import os
import time
import csv
import requests
import json
import numpy as np
import shutil
from PIL import Image, ImageStat
from io import BytesIO
from multiprocessing import Pool
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException


EMOTIONS = ["happiness", "sadness", "disgust", "fear", "surprise", "anger"]
IMAGES_PER_EMOTION = 100
PROCESS_COUNTS = [1, 2, 3, 4, 5, 6]
PERFORMANCE_LOG = "performance_log.csv"

def create_driver():
    """Создает и возвращает экземпляр браузера Chrome в headless-режиме."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    service = Service()
    return webdriver.Chrome(service=service, options=chrome_options)


def scroll_and_collect_image_urls(driver, emotion: list[str], max_images: int) -> list[str]:
    """Прокручивает страницу Pinterest и собирает ссылки на изображения."""
    sleep_counter = 0
    search_url = f"https://www.pinterest.com/search/pins/?q={emotion}_vibe"
    driver.get(search_url)
    image_urls = set()
    last_height = driver.execute_script("return document.body.scrollHeight")

    while len(image_urls) < max_images:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        sleep_counter += 1

        try:
            images = driver.find_elements(By.TAG_NAME, "img")
            for img in images:
                try:
                    class_attr = img.get_attribute("class")
                    src = img.get_attribute("src")
                    if src and "https://i.pinimg.com" in src and "hCL kVc L4E MIw" == class_attr:
                        image_urls.add(src)
                    if len(image_urls) >= max_images:
                        break
                except Exception:
                    continue
        except Exception as e:
            print(f"[{emotion}] Ошибка при получении изображений: {e}")
            continue

        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    return list(image_urls)[:max_images], sleep_counter


def download_and_save_images(emotion: str):
    """Основная функция: создает папку, запускает браузер, скачивает изображения."""
    path = f'./content/{emotion}'
    os.makedirs(path, exist_ok=True)
    driver = None
    try:
        driver = create_driver()
        image_urls, sleep_counter = scroll_and_collect_image_urls(driver, emotion, IMAGES_PER_EMOTION)
    except WebDriverException as e:
        print(f"[{emotion}] Ошибка WebDriver: {e}")
        return
    finally:
        if driver:
            driver.quit()

    count = 0
    for url in image_urls:
        try:
            response = requests.get(url, timeout=20)
            image = Image.open(BytesIO(response.content))

            # Проверка размера
            width, height = image.size
            if width > 1440 or height > 1440 or width < 144 or height < 144:
                continue

            # Сохранение изображения
            filename = os.path.join(path, f"{emotion}_{count:05d}.jpg")
            image.convert("RGB").save(filename, "JPEG")
            count += 1
        except Exception as e:
            with open('bad_links', 'w') as f:
                print(url, file=f)
                print(e, file=f)
            continue

    return sleep_counter


def run_experiment(process_count: int) -> float:
    """Запускает парсинг с заданным числом процессов и замеряет время."""
    print(f"\nЗапуск с {process_count} процессами...")
    start = time.perf_counter()
    with Pool(process_count) as pool:
        sleep_counter = pool.map(download_and_save_images, EMOTIONS)
    end = time.perf_counter()
    duration = round(end - start - max(sleep_counter), 2)
    return duration


def take_param(path: str) -> None:
    """Получает и сохраняет информацию об изображениях"""
    parts = os.path.normpath(path).split(os.sep)
    folder_name = parts[-2]
    file_name = parts[-1]
    new_path = f'annotation/{folder_name}/{file_name.replace('jpg', 'json')}'
    
    img = Image.open(path)
    img_gray = img.convert('L')
    img_hsv = img.convert('HSV')
    img_numpy = np.array(img_gray)
    height, weight = img_numpy.shape
    brightness = round(np.sum(img_numpy) / (height * weight * 255), 2)
    contrast = round(ImageStat.Stat(img_gray).stddev[0], 2)
    saturation = round(ImageStat.Stat(img_hsv).mean[1], 2)
    info = {
        'height': height,
        'weight': weight,
        'brightness': brightness,
        'contrast': contrast,
        'saturation': saturation,
    }
    with open(new_path, 'w', encoding='utf-8') as f:
        json.dump(info, f, indent=2)
    

def process_images(image_path: list[str], num_processes: int) -> None:
    """Обрабатывает список изображений с использованием нескольких процессов."""
    with Pool(processes=num_processes) as pool:
        pool.map(take_param, image_path)


def log_results(results):
    """Сохраняет результаты в CSV-файл."""
    with open(PERFORMANCE_LOG, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Process_Count", "Duration_sec"])
        for proc_count, duration in results:
            writer.writerow([proc_count, duration])


if __name__ == "__main__":
    results = []
    for count in PROCESS_COUNTS:
        shutil.rmtree('content')
        shutil.rmtree('annotation')
        duration = run_experiment(count)
        start = time.perf_counter()
        for emotion in EMOTIONS:
            os.makedirs(f'annotation/{emotion}', exist_ok=True)
            folder_path = f'content/{emotion}'
            image_paths = [os.path.join(folder_path, file_name) 
                         for file_name in os.listdir(folder_path) 
                         if file_name.endswith('.jpg')]
            process_images(image_paths, count)

        end = time.perf_counter()
        duration += (end - start)
        duration = round(duration, 2)
        print(f"Время работы программы: {duration} сек.")
        results.append((count, duration))

    log_results(results)
    print("\nРезультаты сохранены")
