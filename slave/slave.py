# =============================================================================
# slave.py — Агент на подчинённой машине
# =============================================================================
#
# Жизненный цикл Slave:
#   1. Запуск → слушает UDP broadcast от Master (discovery)
#   2. Получив MASTER_DISCOVERY → отвечает SLAVE_READY
#   3. Принимает TCP-соединение от Master → получает скрипт + параметры
#   4. Запускает subprocess: python <script> <params>
#   5. Отправляет файл результата обратно Master
#   6. Параллельно отвечает на heartbeat-пинги
#   7. Готов к следующей задаче
#
# Запуск: python slave.py
# =============================================================================

import socket
import subprocess
import threading
import logging
import os
import sys
import time

from config import (
    UDP_BROADCAST_PORT,
    TCP_PORT,
    HEARTBEAT_UDP_PORT,
    DISCOVERY_REQUEST,
    DISCOVERY_RESPONSE,
    SLAVE_WORK_DIR,
    MASTER_RESULTS_DIR,
    RESULT_FILENAME_TEMPLATE,
    SOCKET_TIMEOUT_SEC,
    LOG_LEVEL,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
    TASK_STATUS_DONE,
)
from protocol import (
    recv_message,
    recv_file,
    send_message,
    send_file,
    make_result_header,
    make_pong_header,
    make_error_header,
    create_tcp_server_socket,
    create_udp_listener_socket,
)

# =============================================================================
# Настройка логирования
# =============================================================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("slave.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("slave")


# =============================================================================
# Определение своего IP-адреса
# =============================================================================

def get_local_ip() -> str:
    """
    Определить IP-адрес этой машины в локальной сети.
    Используется для подстановки в имя файла результата.
    """
    try:
        # Открываем фиктивное соединение — ОС выбирает правильный интерфейс
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"


# =============================================================================
# Discovery — ответ на UDP broadcast от Master
# =============================================================================

class DiscoveryListener(threading.Thread):
    """
    Поток, слушающий UDP broadcast от Master.
    При получении MASTER_DISCOVERY отвечает SLAVE_READY.
    Работает непрерывно — Slave может быть обнаружен несколько раз
    (например, если Master перезапускается).
    """

    def __init__(self, local_ip: str):
        super().__init__(daemon=True, name="DiscoveryListener")
        self.local_ip = local_ip
        self._stop_event = threading.Event()

    def run(self):
        sock = create_udp_listener_socket(UDP_BROADCAST_PORT)
        logger.info("Discovery listener запущен (UDP порт %d)", UDP_BROADCAST_PORT)

        while not self._stop_event.is_set():
            try:
                data, addr = sock.recvfrom(1024)
                message = data.decode("utf-8").strip()

                if message == DISCOVERY_REQUEST:
                    logger.info("Discovery запрос от Master %s", addr[0])
                    response = DISCOVERY_RESPONSE.encode("utf-8")
                    sock.sendto(response, addr)
                    logger.info("Отправлен SLAVE_READY → %s", addr[0])

            except socket.timeout:
                continue
            except Exception as e:
                logger.error("Ошибка в DiscoveryListener: %s", e)

    def stop(self):
        self._stop_event.set()


# =============================================================================
# Heartbeat — ответ на UDP пинги от Master
# =============================================================================

class HeartbeatListener(threading.Thread):
    """
    Поток, отвечающий на UDP heartbeat-пинги от Master.
    Master периодически шлёт PING → Slave отвечает PONG.
    Если Slave не отвечает — Master помечает его как DEAD.
    """

    def __init__(self):
        super().__init__(daemon=True, name="HeartbeatListener")
        self._stop_event = threading.Event()

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", HEARTBEAT_UDP_PORT))
        sock.settimeout(1.0)
        logger.info("Heartbeat listener запущен (UDP порт %d)", HEARTBEAT_UDP_PORT)

        while not self._stop_event.is_set():
            try:
                data, addr = sock.recvfrom(1024)
                message = data.decode("utf-8").strip()

                if message == "PING":
                    sock.sendto(b"PONG", addr)
                    logger.debug("PONG → %s", addr[0])

            except socket.timeout:
                continue
            except Exception as e:
                logger.error("Ошибка в HeartbeatListener: %s", e)

    def stop(self):
        self._stop_event.set()


# =============================================================================
# Обработка задачи
# =============================================================================

class TaskHandler(threading.Thread):
    """
    Поток обработки одной задачи от Master.
    Создаётся для каждого входящего TCP-соединения.

    Порядок работы:
        1. Принять файл скрипта
        2. Принять строку параметров из заголовка
        3. Запустить subprocess
        4. Отправить файл результата обратно
    """

    def __init__(self, conn: socket.socket, master_addr: tuple, local_ip: str):
        super().__init__(daemon=True, name=f"TaskHandler-{master_addr[0]}")
        self.conn = conn
        self.master_addr = master_addr
        self.local_ip = local_ip

    def run(self):
        task_id = None
        try:
            self.conn.settimeout(SOCKET_TIMEOUT_SEC)

            # ------------------------------------------------------------------
            # Шаг 1: Принять скрипт
            # ------------------------------------------------------------------
            logger.info("Принимаем скрипт от Master %s...", self.master_addr[0])
            os.makedirs(SLAVE_WORK_DIR, exist_ok=True)
            header, script_path = recv_file(self.conn, SLAVE_WORK_DIR)

            task_id = header.get("task_id")
            params  = header.get("params", "")
            script_name = os.path.basename(script_path)

            logger.info(
                "Задача получена: task_id=%s script=%s params='%s'",
                task_id, script_name, params
            )

            # ------------------------------------------------------------------
            # Шаг 2: Запустить subprocess
            # ------------------------------------------------------------------
            result_filename = RESULT_FILENAME_TEMPLATE.format(
                task_id=task_id,
                slave_ip=self.local_ip.replace(".", "_"),
            )
            os.makedirs(MASTER_RESULTS_DIR, exist_ok=True)
            result_path = os.path.join(SLAVE_WORK_DIR, result_filename)

            # Строка запуска: python script.py -start 0 -end 100
            cmd = f'python "{script_path}" {params} -result_filename {result_path}'
            logger.info("Запуск: %s", cmd)

            proc = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                cwd='.',
            )

            if proc.returncode != 0:
                # Скрипт завершился с ошибкой
                error_msg = proc.stderr.strip() or f"returncode={proc.returncode}"
                logger.error("Скрипт завершился с ошибкой: %s", error_msg)
                error_header = make_error_header(task_id, self.local_ip, error_msg)
                send_message(self.conn, error_header)
                return

            logger.info("Скрипт выполнен успешно (stdout: %d символов)", len(proc.stdout))

            # ------------------------------------------------------------------
            # Шаг 3: Найти файл результата
            # ------------------------------------------------------------------
            # Скрипт должен создать файл с именем result_filename в SLAVE_WORK_DIR.
            # Если файл не создан — отправляем ошибку.
            if not os.path.isfile(result_path):
                error_msg = (
                    f"Скрипт не создал файл результата: {result_filename}. "
                    f"Убедитесь, что скрипт сохраняет результат по пути: "
                    f"./slave_workspace/{result_filename}"
                )
                logger.error(error_msg)
                error_header = make_error_header(task_id, self.local_ip, error_msg)
                send_message(self.conn, error_header)
                return

            # ------------------------------------------------------------------
            # Шаг 4: Отправить файл результата
            # ------------------------------------------------------------------
            result_header = make_result_header(task_id, self.local_ip, "ok")
            send_file(self.conn, result_header, result_path)
            logger.info("Результат отправлен: %s", result_path)

        except ConnectionError as e:
            logger.warning("Соединение с Master потеряно: %s", e)
        except Exception as e:
            logger.error("Необработанная ошибка в TaskHandler: %s", e, exc_info=True)
            if task_id:
                try:
                    error_header = make_error_header(task_id, self.local_ip, str(e))
                    send_message(self.conn, error_header)
                except Exception:
                    pass
        finally:
            self.conn.close()
            logger.debug("TCP соединение закрыто")


# =============================================================================
# TCP-сервер — приём задач от Master
# =============================================================================

class TaskServer(threading.Thread):
    """
    Основной TCP-сервер Slave.
    Принимает входящие соединения от Master и запускает TaskHandler
    для каждого соединения в отдельном потоке.
    """

    def __init__(self, local_ip: str):
        super().__init__(daemon=True, name="TaskServer")
        self.local_ip = local_ip
        self._stop_event = threading.Event()

    def run(self):
        server_sock = create_tcp_server_socket("", TCP_PORT)
        server_sock.settimeout(1.0)  # чтобы цикл мог проверять _stop_event
        logger.info("TCP Task server запущен (порт %d)", TCP_PORT)

        while not self._stop_event.is_set():
            try:
                conn, addr = server_sock.accept()
                logger.info("Входящее соединение от Master: %s", addr[0])
                handler = TaskHandler(conn, addr, self.local_ip)
                handler.start()
            except socket.timeout:
                continue
            except Exception as e:
                logger.error("Ошибка в TaskServer: %s", e)

        server_sock.close()

    def stop(self):
        self._stop_event.set()


# =============================================================================
# Точка входа
# =============================================================================

def main():
    local_ip = get_local_ip()
    logger.info("=" * 60)
    logger.info("Slave запущен. IP: %s", local_ip)
    logger.info("=" * 60)

    # Создать рабочую директорию
    os.makedirs(SLAVE_WORK_DIR, exist_ok=True)

    # Запустить все фоновые потоки
    discovery = DiscoveryListener(local_ip)
    heartbeat = HeartbeatListener()
    task_server = TaskServer(local_ip)

    discovery.start()
    heartbeat.start()
    task_server.start()

    logger.info("Все службы запущены. Ожидание задач...")

    # Главный поток держит процесс живым
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки (Ctrl+C). Завершение...")
        discovery.stop()
        heartbeat.stop()
        task_server.stop()
        logger.info("Slave остановлен.")


if __name__ == "__main__":
    main()
