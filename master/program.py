# =============================================================================
# program.py — Тестовый рабочий скрипт для Slave
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
import requests
from bs4 import BeautifulSoup

# Шаблон ссылки для парсинга
url_template = "https://pogoda-service.ru/archive_gsod_res.php?station={}&datepicker_beg={}&datepicker_end={}"

def main():
    # --- Парсинг аргументов ---
    parser = argparse.ArgumentParser(description="Тестовый рабочий скрипт")
    parser.add_argument("-start", type=int, required=True, help="Начало диапазона")
    parser.add_argument("-end",   type=int, required=True, help="Конец диапазона")
    parser.add_argument("-result_filename",   type=str, required=True, help="Bмя файла резльтата")
    args = parser.parse_args()

    start = args.start
    end   = args.end
    result_filename = args.result_filename

    print(f"[program.py] Запущен: start={start}, end={end}")

    # ========= основная программа =====================================================  
    # --- Имя файла результата ---
    if not result_filename:
        # Fallback: если запущен вручную без Slave
        result_filename = f"result_{start}_{end}.result"

    # Сохраняем в текущую директорию (slave_workspace)
    result_path = result_filename
    
    with open(result_path, "w", encoding="utf-8") as f:
        f.close()
    
    with open(result_path, "r+", encoding="utf-8") as f:
        for i in range (1924,2024):
            url = url_template.format(start, f"01.01.{i}", f"31.12.{i}")
            response = requests.get(url)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, "html.parser")

            # Находим таблицу
            table = soup.find("table", class_="table_res")

            # Все строки таблицы
            rows = table.find_all("tr")

            data = []

            for row in rows:
                cols = row.find_all(["td", "th"])
                cols = [col.get_text(strip=True) for col in cols]
                if cols:
                    data.append(cols)
            
            # Уменьшаем/убираем заголовки
            if i == 1924:
                headers = ['Дата', 'Тмакс', 'Тмин', 'Тср', 'АтмД(гПа)', 'Вскор(м/с)', 'Осадки(мм)', 'Тэффективн;\n']
                data[0] = headers
            else:
                data = data[1:]
            data.append([""])
            result = ";\n".join(",".join(row) for row in data)
            
            # Вывод
            print(f'Обработан год {i}')       
            
            f.write(result)

    print(f"[program.py] Файл результата сохранён: {result_path}")


if __name__ == "__main__":
    main()
