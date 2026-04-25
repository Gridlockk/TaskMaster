#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# slave.py — Агент на подчинённой машине
# =============================================================================
#
# Жизненный цикл Slave:
#   1. Запуск → слушает UDP broadcast от Master (discovery)
#   2. Получив MASTER_DISCOVERY → отвечает SLAVE_READY
#   3. Принимает TCP-соединение от Master → получает скрипт + параметры
#   4. Запускает subprocess: python <script> <params>
#   5. Читает stdout построчно → пересылает PROGRESS-строки Master
#   6. Отправляет файл результата обратно Master
#   7. Параллельно отвечает на heartbeat-пинги
#   8. Готов к следующей задаче
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
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"


# =============================================================================
# Установка зависимостей
# =============================================================================

def install_requirements(packages: list[str]) -> None:
    """
    Устанавливает pip-пакеты если они ещё не установлены.
    Вызывается перед запуском каждого скрипта.
    Пропускает уже установленные пакеты — повторная установка не происходит.
    """
    if not packages:
        return

    import importlib
    import subprocess as _sp

    # Словарь: pip-имя → имя модуля для проверки через importlib
    # Добавляйте сюда пакеты у которых имя модуля отличается от pip-имени
    PIP_TO_MODULE = {
        "beautifulsoup4": "bs4",
        "opencv-python": "cv2",
        "pillow": "PIL",
        "scikit-learn": "sklearn",
        "pyyaml": "yaml",
    }

    to_install = []
    for package in packages:
        module_name = PIP_TO_MODULE.get(package.lower(), package)
        try:
            importlib.import_module(module_name)
            logger.debug("Пакет уже установлен: %s", package)
        except ImportError:
            to_install.append(package)

    if not to_install:
        logger.info("Все зависимости уже установлены")
        return

    logger.info("Устанавливаю зависимости: %s", to_install)
    for package in to_install:
        try:
            result = _sp.run(
                [sys.executable, "-m", "pip", "install", package],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                logger.info("Установлен: %s", package)
            else:
                logger.error(
                    "Ошибка установки %s: %s", package, result.stderr.strip()
                )
        except Exception as e:
            logger.error("Не удалось установить %s: %s", package, e)


# =============================================================================
# Discovery — ответ на UDP broadcast от Master
# =============================================================================

class DiscoveryListener(threading.Thread):
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
                    sock.sendto(DISCOVERY_RESPONSE.encode("utf-8"), addr)
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
                if data.decode("utf-8").strip() == "PING":
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

    Ключевое изменение: subprocess запускается с stdout=PIPE,
    читается построчно. Строки PROGRESS:<done>/<total> пересылаются
    Master через send_message(type="progress") прямо во время выполнения.
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
            params = header.get("params", "")
            requirements = header.get("requirements", [])
            script_name = os.path.basename(script_path)

            logger.info(
                "Задача получена: task_id=%s script=%s params='%s'",
                task_id, script_name, params
            )

            # ------------------------------------------------------------------
            # Шаг 2: Установить зависимости
            # ------------------------------------------------------------------
            if requirements:
                logger.info("Зависимости задачи: %s", requirements)
            install_requirements(requirements)

            # ------------------------------------------------------------------
            # Шаг 3: Подготовить пути
            # ------------------------------------------------------------------
            result_filename = RESULT_FILENAME_TEMPLATE.format(
                task_id=task_id,
                slave_ip=self.local_ip.replace(".", "_"),
            )
            os.makedirs(SLAVE_WORK_DIR, exist_ok=True)
            result_path = os.path.join(SLAVE_WORK_DIR, result_filename)

            # ------------------------------------------------------------------
            # Шаг 4: Запустить subprocess и читать stdout построчно
            # ------------------------------------------------------------------
            cmd = [
                      sys.executable,
                      script_path,
                  ] + params.split() + ["-result_filename", result_path]

            logger.info("Запуск: %s", " ".join(cmd))

            # Снимаем таймаут на время выполнения — скрипт работает долго
            self.conn.settimeout(None)

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                cwd=".",
            )

            stderr_lines = []

            # Читаем stdout построчно в реальном времени
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line.startswith("PROGRESS:"):
                    # Парсим и пересылаем Master
                    self._send_progress(task_id, line)
                else:
                    logger.info("[subprocess] %s", line)

            # Дождаться завершения и прочитать stderr
            proc.wait()
            stderr_output = proc.stderr.read()
            if stderr_output:
                for l in stderr_output.splitlines():
                    stderr_lines.append(l)
                    logger.warning("[subprocess stderr] %s", l)

            if proc.returncode != 0:
                error_msg = "\n".join(stderr_lines) or f"returncode={proc.returncode}"
                logger.error("Скрипт завершился с ошибкой: %s", error_msg)
                error_header = make_error_header(task_id, self.local_ip, error_msg)
                send_message(self.conn, error_header)
                return

            logger.info("Скрипт выполнен успешно")

            # ------------------------------------------------------------------
            # Шаг 5: Проверить и отправить файл результата
            # ------------------------------------------------------------------
            if not os.path.isfile(result_path):
                error_msg = (
                    f"Скрипт не создал файл результата: {result_filename}. "
                    f"Ожидался путь: {result_path}"
                )
                logger.error(error_msg)
                error_header = make_error_header(task_id, self.local_ip, error_msg)
                send_message(self.conn, error_header)
                return

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

    def _send_progress(self, task_id: str, progress_line: str) -> None:
        """
        Отправить прогресс Master по TCP.
        Формат строки: PROGRESS:<done>/<total>
        Отправляем заголовок: type="progress", task_id, done, total, slave_ip
        """
        try:
            # Парсим PROGRESS:45/100
            payload_str = progress_line[len("PROGRESS:"):]
            done_str, total_str = payload_str.split("/")
            done = int(done_str)
            total = int(total_str)
            percent = round(done / total * 100, 1) if total > 0 else 0

            header = {
                "type": "progress",
                "task_id": task_id,
                "slave_ip": self.local_ip,
                "done": done,
                "total": total,
                "percent": percent,
            }
            send_message(self.conn, header)
            logger.debug("PROGRESS отправлен: %d/%d (%.1f%%)", done, total, percent)
        except Exception as e:
            logger.warning("Не удалось отправить прогресс: %s", e)


# =============================================================================
# TCP-сервер — приём задач от Master
# =============================================================================

class TaskServer(threading.Thread):
    def __init__(self, local_ip: str):
        super().__init__(daemon=True, name="TaskServer")
        self.local_ip = local_ip
        self._stop_event = threading.Event()

    def run(self):
        server_sock = create_tcp_server_socket("", TCP_PORT)
        server_sock.settimeout(1.0)
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

    os.makedirs(SLAVE_WORK_DIR, exist_ok=True)

    discovery = DiscoveryListener(local_ip)
    heartbeat = HeartbeatListener()
    task_server = TaskServer(local_ip)

    discovery.start()
    heartbeat.start()
    task_server.start()

    logger.info("Все службы запущены. Ожидание задач...")

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