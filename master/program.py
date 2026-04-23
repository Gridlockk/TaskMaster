# =============================================================================
# program.py — Рабочий скрипт для Slave
# =============================================================================
#
# Принимает параметры -start и -end, считает сумму чисел в диапазоне,
# сохраняет результат в файл.
#
# Запуск (вручную):
#   python program.py -start 0 -end 100
#
# Slave запускает его как:
#   python program.py -start 0 -end 100
# и ожидает файл результата в ./slave_workspace/{task_id}_{slave_ip}.result
#
# ВАЖНО: имя файла результата передаётся через переменную окружения
#        RESULT_FILENAME, которую устанавливает Slave перед запуском.
#        Если переменная не задана — скрипт использует fallback-имя.
# =============================================================================

import argparse
import json
import os
import sys
import time

import requests
from bs4 import BeautifulSoup

# =============================================================================
# Конфигурация
# =============================================================================

URL_TEMPLATE   = "https://pogoda-service.ru/archive_gsod_res.php?station={}&datepicker_beg={}&datepicker_end={}"
YEAR_START     = 1924
YEAR_END       = 2023
TOTAL_YEARS    = YEAR_END - YEAR_START + 1   # 100

RETRY_ATTEMPTS = 5
RETRY_DELAYS   = [5, 10, 20, 40, 80]        # секунды между попытками

CHECKPOINTS_DIR = "./checkpoints"


# =============================================================================
# Retry
# =============================================================================

def fetch_year(station: int, year: int) -> list[list[str]]:
    """
    Загрузить и распарсить таблицу погоды за один год.
    При сетевой ошибке или пустом ответе повторяет до RETRY_ATTEMPTS раз
    с экспоненциальной задержкой.

    Возвращает список строк таблицы (каждая строка — список ячеек).
    Бросает RuntimeError если все попытки исчерпаны.
    """
    url = URL_TEMPLATE.format(station, f"01.01.{year}", f"31.12.{year}")

    last_error = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            response = requests.get(url, timeout=30)
            response.encoding = "utf-8"
            soup = BeautifulSoup(response.text, "html.parser")

            table = soup.find("table", class_="table_res")
            if table is None:
                raise ValueError(f"Таблица не найдена на странице (год={year})")

            rows = table.find_all("tr")
            data = []
            for row in rows:
                cols = [col.get_text(strip=True) for col in row.find_all(["td", "th"])]
                if cols:
                    data.append(cols)

            if not data:
                raise ValueError(f"Таблица пустая (год={year})")

            return data  # успех

        except Exception as e:
            last_error = e
            if attempt < RETRY_ATTEMPTS - 1:
                delay = RETRY_DELAYS[attempt]
                print(
                    f"[program.py] Ошибка год={year} попытка {attempt+1}/{RETRY_ATTEMPTS}: "
                    f"{e}. Повтор через {delay}с...",
                    flush=True
                )
                time.sleep(delay)
            else:
                print(
                    f"[program.py] Год={year} — все {RETRY_ATTEMPTS} попытки исчерпаны. "
                    f"Последняя ошибка: {last_error}",
                    flush=True
                )

    raise RuntimeError(
        f"Не удалось загрузить данные за год {year} после {RETRY_ATTEMPTS} попыток: {last_error}"
    )


# =============================================================================
# Checkpoint
# =============================================================================

def checkpoint_path(result_filename: str) -> str:
    """Путь к файлу чекпоинта для данного result_filename."""
    os.makedirs(CHECKPOINTS_DIR, exist_ok=True)
    stem = os.path.splitext(os.path.basename(result_filename))[0]
    return os.path.join(CHECKPOINTS_DIR, f"{stem}.json")


def load_checkpoint(result_filename: str) -> int:
    """
    Загрузить чекпоинт.
    Возвращает последний успешно записанный год (или YEAR_START - 1 если чекпоинта нет).
    """
    path = checkpoint_path(result_filename)
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            last_year = int(data.get("last_year", YEAR_START - 1))
            print(f"[program.py] Чекпоинт найден: продолжаем с года {last_year + 1}", flush=True)
            return last_year
        except Exception as e:
            print(f"[program.py] Ошибка чтения чекпоинта: {e}. Начинаем сначала.", flush=True)
    return YEAR_START - 1


def save_checkpoint(result_filename: str, last_year: int) -> None:
    """Сохранить чекпоинт — последний успешно обработанный год."""
    path = checkpoint_path(result_filename)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"last_year": last_year}, f)
    except Exception as e:
        print(f"[program.py] Предупреждение: не удалось сохранить чекпоинт: {e}", flush=True)


def delete_checkpoint(result_filename: str) -> None:
    """Удалить чекпоинт после успешного завершения."""
    path = checkpoint_path(result_filename)
    try:
        if os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass


# =============================================================================
# Заголовки CSV
# =============================================================================

HEADERS = ["Дата", "Тмакс", "Тмин", "Тср", "АтмД(гПа)", "Вскор(м/с)", "Осадки(мм)", "Тэффективн"]


def format_row(row: list[str]) -> str:
    return ",".join(row)


# =============================================================================
# Главная функция
# =============================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-start",           type=int, required=True,  help="ID метеостанции")
    parser.add_argument("-end",             type=int, required=False, default=0)
    parser.add_argument("-result_filename", type=str, required=True,  help="Имя файла результата")
    args = parser.parse_args()

    station         = args.start
    result_filename = args.result_filename

    print(f"[program.py] Запущен: station={station}, result={result_filename}", flush=True)

    # ------------------------------------------------------------------
    # Загрузить чекпоинт — узнать с какого года продолжать
    # ------------------------------------------------------------------
    last_done_year = load_checkpoint(result_filename)
    start_year     = last_done_year + 1

    if start_year > YEAR_END:
        print(f"[program.py] Все годы уже обработаны (чекпоинт). Выходим.", flush=True)
        sys.exit(0)

    years_already_done = last_done_year - YEAR_START + 1 if last_done_year >= YEAR_START else 0

    # ------------------------------------------------------------------
    # Открыть файл результата (append если продолжаем, write если сначала)
    # ------------------------------------------------------------------
    file_mode = "a" if last_done_year >= YEAR_START else "w"
    os.makedirs(os.path.dirname(result_filename) if os.path.dirname(result_filename) else ".", exist_ok=True)

    with open(result_filename, file_mode, encoding="utf-8") as f:

        # Заголовок пишем только при старте с нуля
        if file_mode == "w":
            f.write(",".join(HEADERS) + ";\n")

        # ------------------------------------------------------------------
        # Основной цикл по годам
        # ------------------------------------------------------------------
        for year in range(start_year, YEAR_END + 1):
            try:
                data = fetch_year(station, year)
            except RuntimeError as e:
                # Все retry исчерпаны — завершаем с ошибкой
                # Чекпоинт остаётся — при переназначении задачи продолжим
                print(f"[program.py] КРИТИЧЕСКАЯ ОШИБКА: {e}", flush=True)
                sys.exit(1)

            # Убираем строку заголовка таблицы (она уже написана один раз)
            data_rows = data[1:] if len(data) > 1 else data

            # Пустой разделитель между годами
            if year > start_year or file_mode == "a":
                f.write("\n")

            result = ";\n".join(format_row(row) for row in data_rows)
            f.write(result)
            f.flush()  # сбрасываем на диск после каждого года

            # Сохранить чекпоинт
            save_checkpoint(result_filename, year)

            # Прогресс в stdout — Slave читает эту строку и отправляет Master
            done  = years_already_done + (year - start_year + 1)
            total = TOTAL_YEARS
            print(f"PROGRESS:{done}/{total}", flush=True)
            print(f"[program.py] Обработан год {year} ({done}/{total})", flush=True)

    # ------------------------------------------------------------------
    # Успешное завершение — удалить чекпоинт
    # ------------------------------------------------------------------
    delete_checkpoint(result_filename)
    print(f"[program.py] Готово. Файл результата: {result_filename}", flush=True)


if __name__ == "__main__":
    main()