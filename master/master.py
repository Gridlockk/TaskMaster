# =============================================================================
# master.py — Главная программа системы Master-Slave
# =============================================================================
#
# Жизненный цикл Master:
#   1. Discovery  — UDP broadcast, сбор списка живых Slaves
#   2. Dispatch   — отправка скрипта + параметров Slave-ам
#   3. Watchdog   — фоновый мониторинг живости Slaves (heartbeat)
#   4. Collect    — ожидание результатов (с промежуточными progress-сообщениями)
#   5. Report     — итоговый отчёт
#
# Использование:
#   master = Master()
#   slaves = master.discover()
#   results = master.run(
#       script_path = "program.py",
#       tasks       = [
#           {"params": "-start 0 -end 100"},
#           {"params": "-start 100 -end 200"},
#       ]
#   )
# =============================================================================
from __future__ import annotations

import socket
import threading
import logging
import os
import sys
import time
import uuid
import queue
from typing import Optional, Callable

from config import (
    UDP_BROADCAST_PORT,
    TCP_PORT,
    HEARTBEAT_UDP_PORT,
    BROADCAST_ADDRESS,
    DISCOVERY_REQUEST,
    DISCOVERY_RESPONSE,
    DISCOVERY_TIMEOUT_SEC,
    HEARTBEAT_INTERVAL_SEC,
    HEARTBEAT_TIMEOUT_SEC,
    MASTER_RESULTS_DIR,
    RESULT_FILENAME_TEMPLATE,
    SOCKET_TIMEOUT_SEC,
    TASK_STATUS_PENDING,
    TASK_STATUS_SENT,
    TASK_STATUS_DONE,
    TASK_STATUS_LOST,
    SLAVE_STATUS_ALIVE,
    SLAVE_STATUS_BUSY,
    SLAVE_STATUS_DEAD,
    LOG_LEVEL,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
)
from protocol import (
    recv_message,
    recv_file,
    send_file,
    create_tcp_client_socket,
    create_udp_broadcast_socket,
)


# =============================================================================
# Чтение зависимостей из скрипта
# =============================================================================

def read_requirements(script_path: str) -> list[str]:
    """
    Читает переменную REQUIREMENTS из скрипта не импортируя его.
    Ищет строку вида: REQUIREMENTS = ["requests", "beautifulsoup4"]
    Возвращает список пакетов или [] если переменная не найдена.
    """
    import ast
    try:
        with open(script_path, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if (
                    isinstance(node, ast.Assign)
                    and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)
                    and node.targets[0].id == "REQUIREMENTS"
                    and isinstance(node.value, ast.List)
            ):
                return [
                    elt.s for elt in node.value.elts
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                ]
    except Exception as e:
        logger.warning("Не удалось прочитать REQUIREMENTS из %s: %s", script_path, e)
    return []


# =============================================================================
# Логирование
# =============================================================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("master.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("master")


# =============================================================================
# Структуры данных
# =============================================================================

class SlaveInfo:
    def __init__(self, ip: str):
        self.ip: str = ip
        self.status: str = SLAVE_STATUS_ALIVE
        self.last_pong: float = time.time()
        self.task_id: Optional[str] = None
        self._lock = threading.Lock()

    def mark_busy(self, task_id: str):
        with self._lock:
            self.status = SLAVE_STATUS_BUSY
            self.task_id = task_id

    def mark_alive(self):
        with self._lock:
            self.last_pong = time.time()
            if self.status != SLAVE_STATUS_BUSY:
                self.status = SLAVE_STATUS_ALIVE

    def mark_dead(self):
        with self._lock:
            self.status = SLAVE_STATUS_DEAD

    def mark_free(self):
        with self._lock:
            self.status = SLAVE_STATUS_ALIVE
            self.task_id = None

    @property
    def is_dead(self) -> bool:
        with self._lock:
            return self.status == SLAVE_STATUS_DEAD

    def __repr__(self):
        return f"<Slave {self.ip} [{self.status}] task={self.task_id}>"


class TaskInfo:
    """
    Информация об одной задаче.
    Поля done/total/percent обновляются в реальном времени
    по мере прихода progress-сообщений от Slave.
    """

    def __init__(self, task_id: str, slave_ip: str, params: str):
        self.task_id: str = task_id
        self.slave_ip: str = slave_ip
        self.params: str = params
        self.status: str = TASK_STATUS_PENDING
        self.result_path: Optional[str] = None
        self.error: Optional[str] = None

        # Прогресс (обновляется live)
        self.done: int = 0
        self.total: int = 0
        self.percent: float = 0.0

    def update_progress(self, done: int, total: int, percent: float):
        self.done = done
        self.total = total
        self.percent = percent

    def __repr__(self):
        return (
            f"<Task {self.task_id[:8]}... [{self.status}] "
            f"slave={self.slave_ip} {self.percent:.0f}%>"
        )


# =============================================================================
# Watchdog
# =============================================================================

class Watchdog(threading.Thread):
    def __init__(self, master: Master):
        super().__init__(daemon=True, name="Watchdog")
        self.master = master
        self._stop_event = threading.Event()

    def run(self):
        logger.info("Watchdog запущен (интервал=%ds, таймаут=%ds)",
                    HEARTBEAT_INTERVAL_SEC, HEARTBEAT_TIMEOUT_SEC)
        while not self._stop_event.is_set():
            time.sleep(HEARTBEAT_INTERVAL_SEC)
            self._ping_all()

    def _ping_all(self):
        for ip, slave in list(self.master.slaves.items()):
            if slave.is_dead:
                continue
            if self._ping(ip):
                slave.mark_alive()
                logger.debug("PONG от %s", ip)
            else:
                if not slave.is_dead:
                    slave.mark_dead()
                    logger.warning("Slave %s не отвечает — помечен как DEAD", ip)
                    if slave.task_id:
                        task = self.master.tasks.get(slave.task_id)
                        if task:
                            self.master._handle_lost_task(slave, task, "Detected dead by watchdog")

    def _ping(self, ip: str) -> bool:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(HEARTBEAT_TIMEOUT_SEC)
            sock.sendto(b"PING", (ip, HEARTBEAT_UDP_PORT))
            data, _ = sock.recvfrom(1024)
            sock.close()
            return data.decode("utf-8").strip() == "PONG"
        except Exception:
            return False

    def stop(self):
        self._stop_event.set()


# =============================================================================
# Discovery
# =============================================================================

def discover_slaves(timeout: float = DISCOVERY_TIMEOUT_SEC) -> dict[str, SlaveInfo]:
    logger.info("Discovery: рассылка UDP broadcast (ожидание %ds)...", timeout)
    slaves: dict[str, SlaveInfo] = {}

    sock = create_udp_broadcast_socket(timeout=timeout)
    sock.bind(("", 0))

    try:
        sock.sendto(DISCOVERY_REQUEST.encode("utf-8"), (BROADCAST_ADDRESS, UDP_BROADCAST_PORT))

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                data, addr = sock.recvfrom(1024)
                message = data.decode("utf-8").strip()
                ip = addr[0]
                if message == DISCOVERY_RESPONSE and ip not in slaves:
                    slaves[ip] = SlaveInfo(ip)
                    logger.info("Обнаружен Slave: %s", ip)
            except socket.timeout:
                break
            except Exception as e:
                logger.error("Ошибка при приёме discovery-ответа: %s", e)
    finally:
        sock.close()

    logger.info("Discovery завершён. Найдено Slaves: %d", len(slaves))
    return slaves


# =============================================================================
# Master
# =============================================================================

class Master:
    """
    Главный класс системы.

    on_progress — колбэк, вызывается при получении progress-сообщения:
        on_progress(task: TaskInfo)
    GUI переопределяет его для обновления прогресс-баров в реальном времени.
    """

    def __init__(self):
        self.slaves: dict[str, SlaveInfo] = {}
        self.tasks: dict[str, TaskInfo] = {}
        self._watchdog: Optional[Watchdog] = None
        self.free_slaves = queue.Queue()
        self.task_queue = queue.Queue()

        self.on_progress: Optional[Callable[[TaskInfo], None]] = None

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover(self, timeout: float = DISCOVERY_TIMEOUT_SEC) -> dict[str, SlaveInfo]:
        self.slaves = discover_slaves(timeout)
        return self.slaves

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self, script_path: str, tasks: list[dict]) -> dict[str, TaskInfo]:
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Скрипт не найден: {script_path}")
        if not self.slaves:
            raise RuntimeError("Список Slaves пуст. Сначала вызовите discover().")

        for t in tasks:
            task_id = str(uuid.uuid4())
            task = TaskInfo(task_id=task_id, slave_ip="", params=t["params"])
            self.tasks[task_id] = task
            self.task_queue.put(task)

        logger.info("Подготовлено задач: %d", self.task_queue.qsize())

        for slave in self.slaves.values():
            if not slave.is_dead:
                self.free_slaves.put(slave)

        logger.info("Свободных Slaves: %d", self.free_slaves.qsize())

        self._watchdog = Watchdog(self)
        self._watchdog.start()

        active_threads: set[threading.Thread] = set()

        while not self.task_queue.empty() or active_threads:
            if not self.free_slaves.empty() and not self.task_queue.empty():
                slave = self.free_slaves.get()
                task = self.task_queue.get()
                task.slave_ip = slave.ip

                t = threading.Thread(
                    target=self._dispatch_with_reassignment,
                    args=(slave, task, script_path),
                    name=f"Dispatch-{slave.ip}-{task.task_id[:8]}",
                    daemon=True,
                )
                active_threads.add(t)
                t.start()

            time.sleep(0.1)
            active_threads = {t for t in active_threads if t.is_alive()}

        for t in list(active_threads):
            t.join()

        self._watchdog.stop()
        self._print_report()
        return self.tasks

    # ------------------------------------------------------------------
    # _run_dispatch — запуск диспетчеризации без создания задач
    # Используется GUI: задачи уже созданы и добавлены в task_queue
    # ------------------------------------------------------------------

    def _run_dispatch(self, script_path: str) -> dict[str, TaskInfo]:
        """
        Запустить диспетчеризацию уже подготовленных задач.
        GUI вызывает этот метод вместо run(), потому что задачи
        создаются заранее (чтобы сразу построить виджеты прогресса).
        """
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Скрипт не найден: {script_path}")
        if not self.slaves:
            raise RuntimeError("Список Slaves пуст.")

        logger.info("Подготовлено задач: %d", self.task_queue.qsize())

        # Сбросить пул Slaves и заполнить заново
        while not self.free_slaves.empty():
            try:
                self.free_slaves.get_nowait()
            except Exception:
                break
        for slave in self.slaves.values():
            if not slave.is_dead:
                self.free_slaves.put(slave)

        logger.info("Свободных Slaves: %d", self.free_slaves.qsize())

        self._watchdog = Watchdog(self)
        self._watchdog.start()

        active_threads: set[threading.Thread] = set()

        while not self.task_queue.empty() or active_threads:
            if not self.free_slaves.empty() and not self.task_queue.empty():
                slave = self.free_slaves.get()
                task = self.task_queue.get()
                task.slave_ip = slave.ip

                t = threading.Thread(
                    target=self._dispatch_with_reassignment,
                    args=(slave, task, script_path),
                    name=f"Dispatch-{slave.ip}-{task.task_id[:8]}",
                    daemon=True,
                )
                active_threads.add(t)
                t.start()

            time.sleep(0.1)
            active_threads = {t for t in active_threads if t.is_alive()}

        for t in list(active_threads):
            t.join()

        self._watchdog.stop()
        self._print_report()
        return self.tasks

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _dispatch_with_reassignment(
            self,
            slave: SlaveInfo,
            task: TaskInfo,
            script_path: str,
    ) -> None:
        logger.info("Dispatch → %s | task=%s", slave.ip, task.task_id[:8])
        slave.mark_busy(task.task_id)
        task.status = TASK_STATUS_SENT

        conn = None
        try:
            conn = create_tcp_client_socket(slave.ip, TCP_PORT, timeout=SOCKET_TIMEOUT_SEC)
            conn.settimeout(SOCKET_TIMEOUT_SEC)

            requirements = read_requirements(script_path)
            task_header = {
                "type": "task",
                "task_id": task.task_id,
                "params": task.params,
                "requirements": requirements,
            }
            if requirements:
                logger.info("Зависимости для задачи %s: %s", task.task_id[:8], requirements)
            send_file(conn, task_header, script_path)
            logger.info("Скрипт отправлен → %s", slave.ip)

            self._receive_result_with_reassignment(conn, slave, task)

        except (ConnectionRefusedError, socket.timeout, Exception) as e:
            logger.error("Ошибка dispatch → %s: %s", slave.ip, e)
            self._handle_lost_task(slave, task, str(e))
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _receive_result_with_reassignment(
            self,
            conn: socket.socket,
            slave: SlaveInfo,
            task: TaskInfo,
    ) -> None:
        """
        Читаем поток сообщений от Slave до получения финального result/error.

        Сообщения трёх типов:
          - type="progress" — обновляем task.done/total/percent, вызываем on_progress
          - type="result"   — получаем файл результата, завершаем
          - type="error"    — задача провалилась, переназначаем
        """
        conn.settimeout(None)

        while True:
            header, payload = recv_message(conn)
            msg_type = header.get("type")

            # ------------------------------------------------------------------
            # Прогресс
            # ------------------------------------------------------------------
            if msg_type == "progress":
                done = header.get("done", 0)
                total = header.get("total", 0)
                percent = header.get("percent", 0.0)
                task.update_progress(done, total, percent)
                logger.info(
                    "Прогресс %s: %d/%d (%.1f%%)",
                    slave.ip, done, total, percent
                )
                # Вызвать GUI-колбэк если он задан
                if self.on_progress:
                    try:
                        self.on_progress(task)
                    except Exception as e:
                        logger.warning("Ошибка в on_progress колбэке: %s", e)
                continue  # ждём следующего сообщения

            # ------------------------------------------------------------------
            # Результат
            # ------------------------------------------------------------------
            if msg_type == "result" and header.get("status") == "ok":
                os.makedirs(MASTER_RESULTS_DIR, exist_ok=True)
                filename = header.get("filename", f"{task.task_id}_{slave.ip}.result")
                result_path = os.path.join(MASTER_RESULTS_DIR, filename)

                with open(result_path, "wb") as f:
                    f.write(payload)

                task.status = TASK_STATUS_DONE
                task.result_path = result_path
                task.percent = 100.0
                slave.mark_free()
                self.free_slaves.put(slave)

                logger.info(
                    "Результат получен от %s → %s (%d байт)",
                    slave.ip, result_path, len(payload)
                )

                if self.on_progress:
                    try:
                        self.on_progress(task)
                    except Exception:
                        pass
                return

            # ------------------------------------------------------------------
            # Ошибка
            # ------------------------------------------------------------------
            if msg_type == "error":
                error_msg = header.get("message", "неизвестная ошибка")
                logger.error("Slave %s вернул ошибку: %s", slave.ip, error_msg)
                self._handle_lost_task(slave, task, error_msg)
                return

            # Неожиданный тип
            logger.error("Неожиданный ответ от %s: type=%s", slave.ip, msg_type)
            self._handle_lost_task(slave, task, f"Unexpected message type: {msg_type}")
            return

    def _handle_lost_task(self, slave: SlaveInfo, task: TaskInfo, reason: str) -> None:
        task.status = TASK_STATUS_LOST
        task.error = reason

        if slave.is_dead:
            logger.warning(
                "Slave %s мёртвый — возвращаем задачу %s в очередь",
                slave.ip, task.task_id[:8]
            )
            task.status = TASK_STATUS_PENDING
            task.slave_ip = ""
            self.task_queue.put(task)
        else:
            slave.mark_free()
            self.free_slaves.put(slave)

    # ------------------------------------------------------------------
    # Отчёт
    # ------------------------------------------------------------------

    def _print_report(self):
        done = [t for t in self.tasks.values() if t.status == TASK_STATUS_DONE]
        lost = [t for t in self.tasks.values() if t.status == TASK_STATUS_LOST]
        other = [t for t in self.tasks.values()
                 if t.status not in (TASK_STATUS_DONE, TASK_STATUS_LOST)]

        logger.info("=" * 60)
        logger.info("ИТОГ: всего=%d  выполнено=%d  потеряно=%d  прочее=%d",
                    len(self.tasks), len(done), len(lost), len(other))

        if done:
            logger.info("--- Выполненные задачи ---")
            for t in done:
                logger.info("  [DONE] %s | slave=%s | файл=%s",
                            t.task_id[:8], t.slave_ip, t.result_path)
        if lost:
            logger.info("--- Потерянные задачи ---")
            for t in lost:
                logger.info("  [LOST] %s | slave=%s | причина: %s",
                            t.task_id[:8], t.slave_ip, t.error)

        logger.info("=" * 60)


# =============================================================================
# Точка входа
# =============================================================================

if __name__ == "__main__":
    master = Master()

    print("\n>>> Запуск Discovery...\n")
    slaves = master.discover()

    if not slaves:
        print("Slaves не найдены.")
        sys.exit(1)

    slave_ips = list(slaves.keys())
    SCRIPT = "program.py"
    CHUNK = 100

    tasks = []
    for i in range(len(slave_ips)):
        start = i * CHUNK
        tasks.append({"params": f"-start {start} -end 0"})

    results = master.run(script_path=SCRIPT, tasks=tasks)

    for task_id, task in results.items():
        if task.status == TASK_STATUS_DONE:
            print(f"  [OK]   slave={task.slave_ip}  файл={task.result_path}")
        else:
            print(f"  [LOST] slave={task.slave_ip}  причина={task.error}")
