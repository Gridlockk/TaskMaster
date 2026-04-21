# =============================================================================
# protocol.py — Протокол обмена данными Master-Slave
# =============================================================================
#
# Формат TCP-сообщения:
#   [4 байта: длина JSON-заголовка][JSON-заголовок][бинарные данные (если есть)]
#
# Это позволяет передавать как простые команды (только заголовок),
# так и файлы (заголовок + бинарное тело).
#
# =============================================================================

import socket
import json
import struct
import logging
import os

from config import SOCKET_BUFFER_SIZE, SOCKET_TIMEOUT_SEC, MAX_FILE_SIZE_BYTES

logger = logging.getLogger(__name__)


# =============================================================================
# Низкоуровневые функции отправки / приёма байт
# =============================================================================

def send_bytes(sock: socket.socket, data: bytes) -> None:
    """
    Надёжная отправка байтового буфера через сокет.
    Гарантирует отправку ВСЕХ байт (обходит частичную запись).
    """
    total_sent = 0
    while total_sent < len(data):
        sent = sock.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError("Соединение разорвано при отправке данных")
        total_sent += sent


def recv_bytes(sock: socket.socket, length: int) -> bytes:
    """
    Надёжное чтение ровно `length` байт из сокета.
    Гарантирует получение ВСЕХ запрошенных байт.
    """
    chunks = []
    received = 0
    while received < length:
        chunk = sock.recv(min(SOCKET_BUFFER_SIZE, length - received))
        if not chunk:
            raise ConnectionError("Соединение разорвано при получении данных")
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)


# =============================================================================
# Отправка сообщений
# =============================================================================

def send_message(sock: socket.socket, header: dict, payload: bytes = b"") -> None:
    """
    Отправить сообщение: JSON-заголовок + опциональные бинарные данные.

    Структура:
        [4 байта uint32: длина заголовка][заголовок UTF-8][payload байты]

    Аргументы:
        sock    — TCP-сокет
        header  — словарь с метаданными сообщения
        payload — бинарные данные (файл); по умолчанию пусто
    """
    header["payload_size"] = len(payload)
    header_bytes = json.dumps(header, ensure_ascii=False).encode("utf-8")
    header_len = struct.pack(">I", len(header_bytes))  # 4 байта, big-endian

    send_bytes(sock, header_len)
    send_bytes(sock, header_bytes)

    if payload:
        send_bytes(sock, payload)

    logger.debug(
        "Отправлено: type=%s payload=%d байт",
        header.get("type", "?"),
        len(payload),
    )


def send_file(sock: socket.socket, header: dict, filepath: str) -> None:
    """
    Отправить файл с заголовком.
    Читает файл целиком и передаёт как payload.

    Аргументы:
        sock     — TCP-сокет
        header   — словарь с метаданными (type, task_id и т.д.)
        filepath — путь к файлу на диске
    """
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Файл не найден: {filepath}")

    file_size = os.path.getsize(filepath)
    if file_size > MAX_FILE_SIZE_BYTES:
        raise ValueError(
            f"Файл {filepath} слишком большой: {file_size} байт "
            f"(максимум {MAX_FILE_SIZE_BYTES})"
        )

    with open(filepath, "rb") as f:
        payload = f.read()

    header["filename"] = os.path.basename(filepath)
    send_message(sock, header, payload)
    logger.info("Файл отправлен: %s (%d байт)", filepath, file_size)


# =============================================================================
# Приём сообщений
# =============================================================================

def recv_message(sock: socket.socket) -> tuple[dict, bytes]:
    """
    Принять сообщение: JSON-заголовок + бинарные данные.

    Возвращает:
        (header: dict, payload: bytes)
        payload может быть пустым (b""), если файл не передавался
    """
    # Читаем 4 байта длины заголовка
    raw_len = recv_bytes(sock, 4)
    header_len = struct.unpack(">I", raw_len)[0]

    # Читаем заголовок
    header_bytes = recv_bytes(sock, header_len)
    header = json.loads(header_bytes.decode("utf-8"))

    # Читаем payload (если есть)
    payload_size = header.get("payload_size", 0)
    payload = b""
    if payload_size > 0:
        if payload_size > MAX_FILE_SIZE_BYTES:
            raise ValueError(
                f"Входящий payload слишком большой: {payload_size} байт"
            )
        payload = recv_bytes(sock, payload_size)

    logger.debug(
        "Получено: type=%s payload=%d байт",
        header.get("type", "?"),
        len(payload),
    )
    return header, payload


def recv_file(sock: socket.socket, save_dir: str) -> tuple[dict, str]:
    """
    Принять файл и сохранить его на диск.

    Аргументы:
        sock     — TCP-сокет
        save_dir — директория для сохранения файла

    Возвращает:
        (header: dict, filepath: str) — заголовок и путь к сохранённому файлу
    """
    header, payload = recv_message(sock)

    filename = header.get("filename")
    if not filename:
        raise ValueError("В заголовке отсутствует имя файла (filename)")

    os.makedirs(save_dir, exist_ok=True)
    filepath = os.path.join(save_dir, filename)

    with open(filepath, "wb") as f:
        f.write(payload)

    logger.info("Файл сохранён: %s (%d байт)", filepath, len(payload))
    return header, filepath


# =============================================================================
# Готовые конструкторы заголовков (фабричные функции)
# =============================================================================

def make_task_header(task_id: str, params: str) -> dict:
    """
    Заголовок для отправки задачи Master → Slave.

    task_id — уникальный идентификатор задачи (uuid)
    params  — строка параметров запуска, например "-start 0 -end 100"
    """
    return {
        "type":    "task",
        "task_id": task_id,
        "params":  params,
    }


def make_result_header(task_id: str, slave_ip: str, status: str) -> dict:
    """
    Заголовок для отправки результата Slave → Master.

    task_id  — идентификатор задачи
    slave_ip — IP-адрес Slave (подставляется в имя файла результата)
    status   — "ok" или "error"
    """
    return {
        "type":     "result",
        "task_id":  task_id,
        "slave_ip": slave_ip,
        "status":   status,
    }


def make_ping_header() -> dict:
    """Заголовок heartbeat-пинга от Master."""
    return {"type": "ping"}


def make_pong_header() -> dict:
    """Заголовок heartbeat-ответа от Slave."""
    return {"type": "pong"}


def make_error_header(task_id: str, slave_ip: str, message: str) -> dict:
    """
    Заголовок сообщения об ошибке выполнения скрипта на Slave.
    Slave отправляет его вместо файла результата при падении subprocess.
    """
    return {
        "type":     "error",
        "task_id":  task_id,
        "slave_ip": slave_ip,
        "status":   "error",
        "message":  message,
    }


# =============================================================================
# Вспомогательные утилиты
# =============================================================================

def set_socket_timeout(sock: socket.socket, timeout: float = SOCKET_TIMEOUT_SEC) -> None:
    """Установить таймаут на сокет."""
    sock.settimeout(timeout)


def create_tcp_server_socket(host: str, port: int) -> socket.socket:
    """
    Создать и подготовить серверный TCP-сокет.
    SO_REUSEADDR позволяет повторно занять порт после перезапуска.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(50)
    logger.info("TCP-сервер слушает %s:%d", host, port)
    return sock


def create_tcp_client_socket(host: str, port: int, timeout: float = SOCKET_TIMEOUT_SEC) -> socket.socket:
    """
    Создать TCP-сокет и подключиться к серверу.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect((host, port))
    logger.debug("TCP подключение к %s:%d", host, port)
    return sock


def create_udp_broadcast_socket(timeout: float = 5.0) -> socket.socket:
    """
    Создать UDP-сокет с поддержкой широковещательной рассылки.
    Используется Master при discovery.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(timeout)
    return sock


def create_udp_listener_socket(port: int) -> socket.socket:
    """
    Создать UDP-сокет для прослушивания broadcast-запросов.
    Используется Slave при discovery.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", port))
    logger.info("UDP listener запущен на порту %d", port)
    return sock
