# =============================================================================
# master.py — Главная программа системы Master-Slave
# =============================================================================
#
# Жизненный цикл Master:
#   1. Discovery  — UDP broadcast, сбор списка живых Slaves
#   2. Dispatch   — отправка скрипта + параметров Slave-ам с переназначением при падении
#   3. Watchdog   — фоновый мониторинг живости Slaves (heartbeat)
#   4. Collect    — ожидание результатов от живых Slaves
#   5. Report     — итоговый отчёт: кто выполнил, кто упал, что потеряно
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

import socket
import threading
import logging
import os
import sys
import time
import uuid
import queue
from typing import Optional

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
# Настройка логирования
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
    """Информация об одной Slave-машине."""

    def __init__(self, ip: str):
        self.ip: str = ip
        self.status: str = SLAVE_STATUS_ALIVE
        self.last_pong: float = time.time()   # время последнего PONG
        self.task_id: Optional[str] = None    # текущая задача (если есть)
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
    """Информация об одной задаче."""

    def __init__(self, task_id: str, slave_ip: str, params: str):
        self.task_id: str = task_id
        self.slave_ip: str = slave_ip
        self.params: str = params
        self.status: str = TASK_STATUS_PENDING
        self.result_path: Optional[str] = None  # путь к файлу результата
        self.error: Optional[str] = None

    def __repr__(self):
        return f"<Task {self.task_id[:8]}... [{self.status}] slave={self.slave_ip}>"


# =============================================================================
# Watchdog — фоновый мониторинг живости Slaves
# =============================================================================

class Watchdog(threading.Thread):
    """
    Периодически пингует каждый Slave по UDP.
    Если Slave не ответил за HEARTBEAT_TIMEOUT_SEC — помечает как DEAD.
    Работает в фоне всё время выполнения задач.
    """

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
            alive = self._ping(ip)
            if alive:
                slave.mark_alive()
                logger.debug("PONG от %s", ip)
            else:
                if not slave.is_dead:
                    slave.mark_dead()
                    logger.warning("Slave %s не отвечает — помечен как DEAD", ip)
                    # Если слейв был занят, переназначить его задачу
                    if slave.task_id:
                        task = self.master.tasks[slave.task_id]
                        self.master._handle_lost_task(slave, task, "Detected dead by watchdog")

    def _ping(self, ip: str) -> bool:
        """Отправить UDP PING, вернуть True если пришёл PONG."""
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
# Discovery — обнаружение Slaves в сети
# =============================================================================

def discover_slaves(timeout: float = DISCOVERY_TIMEOUT_SEC) -> dict[str, SlaveInfo]:
    """
    Разослать UDP broadcast и собрать ответы от Slaves.

    Возвращает словарь: ip → SlaveInfo
    """
    logger.info("Discovery: рассылка UDP broadcast (ожидание %ds)...", timeout)
    slaves: dict[str, SlaveInfo] = {}

    sock = create_udp_broadcast_socket(timeout=timeout)
    sock.bind(("", 0))  # любой свободный локальный порт

    try:
        request = DISCOVERY_REQUEST.encode("utf-8")
        sock.sendto(request, (BROADCAST_ADDRESS, UDP_BROADCAST_PORT))
        logger.debug("Broadcast отправлен → %s:%d", BROADCAST_ADDRESS, UDP_BROADCAST_PORT)

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
    for ip in slaves:
        logger.info("  → %s", ip)

    return slaves


# =============================================================================
# Dispatcher — отправка задачи одному Slave
# =============================================================================

def dispatch_task(
    slave: SlaveInfo,
    task: TaskInfo,
    script_path: str,
) -> None:
    """
    Отправить скрипт + параметры одному Slave по TCP.
    Вызывается в отдельном потоке для каждого Slave.

    После отправки ждёт ответ (файл результата или сообщение об ошибке).
    Обновляет task.status по результату.
    """
    logger.info(
        "Dispatch → %s | task=%s | params='%s'",
        slave.ip, task.task_id[:8], task.params
    )

    slave.mark_busy(task.task_id)
    task.status = TASK_STATUS_SENT

    try:
        conn = create_tcp_client_socket(slave.ip, TCP_PORT, timeout=SOCKET_TIMEOUT_SEC)
        conn.settimeout(SOCKET_TIMEOUT_SEC)

        # Формируем заголовок задачи — он будет встроен в send_file
        task_header = {
            "type":    "task",
            "task_id": task.task_id,
            "params":  task.params,
        }

        # Отправляем скрипт (заголовок + бинарный файл)
        send_file(conn, task_header, script_path)
        logger.info("Скрипт отправлен → %s", slave.ip)

        # Ждём ответ от Slave
        _receive_result(conn, slave, task)

    except ConnectionRefusedError:
        logger.error("Slave %s недоступен (connection refused)", slave.ip)
        _mark_lost(slave, task, "Connection refused")

    except socket.timeout:
        logger.error("Slave %s не ответил вовремя (timeout)", slave.ip)
        _mark_lost(slave, task, "Timeout")

    except Exception as e:
        logger.error("Ошибка при dispatch → %s: %s", slave.ip, e, exc_info=True)
        _mark_lost(slave, task, str(e))

    finally:
        try:
            conn.close()
        except Exception:
            pass


def _receive_result(
    conn: socket.socket,
    slave: SlaveInfo,
    task: TaskInfo,
) -> None:
    """
    Ожидать и принять результат от Slave после выполнения задачи.
    Обновляет task.status и task.result_path.
    """
    # Устанавливаем большой таймаут — скрипт может работать долго
    conn.settimeout(None)  # без таймаута — ждём столько, сколько нужно

    header, payload = recv_message(conn)
    msg_type = header.get("type")

    if msg_type == "error":
        error_msg = header.get("message", "неизвестная ошибка")
        logger.error("Slave %s вернул ошибку: %s", slave.ip, error_msg)
        task.status = TASK_STATUS_LOST
        task.error = error_msg
        slave.mark_free()
        return

    if msg_type == "result" and header.get("status") == "ok":
        # Payload содержит файл результата
        os.makedirs(MASTER_RESULTS_DIR, exist_ok=True)
        filename = header.get("filename", f"{task.task_id}_{slave.ip}.result")
        result_path = os.path.join(MASTER_RESULTS_DIR, filename)

        with open(result_path, "wb") as f:
            f.write(payload)

        task.status = TASK_STATUS_DONE
        task.result_path = result_path
        slave.mark_free()
        logger.info(
            "Результат получен от %s → %s (%d байт)",
            slave.ip, result_path, len(payload)
        )
        return

    # Неожиданный тип сообщения
    logger.error("Неожиданный ответ от %s: type=%s", slave.ip, msg_type)
    _mark_lost(slave, task, f"Unexpected message type: {msg_type}")


def _mark_lost(slave: SlaveInfo, task: TaskInfo, reason: str) -> None:
    """Пометить задачу как потерянную и освободить Slave."""
    task.status = TASK_STATUS_LOST
    task.error = reason
    slave.mark_dead()
    logger.warning("Задача %s помечена как LOST. Причина: %s", task.task_id[:8], reason)


# =============================================================================
# Master — главный класс
# =============================================================================

class Master:
    """
    Главный класс системы.

    Пример использования:
        master = Master()

        # 1. Обнаружить Slaves
        slaves = master.discover()
        print(f"Найдено: {len(slaves)} машин")

        # 2. Раздать задачи и получить результаты (с переназначением при падении)
        results = master.run(
            script_path = "program.py",
            tasks = [
                {"params": "-start 0   -end 100"},
                {"params": "-start 100 -end 200"},
                {"params": "-start 200 -end 300"},
            ]
        )
    """

    def __init__(self):
        self.slaves: dict[str, SlaveInfo] = {}   # ip → SlaveInfo
        self.tasks:  dict[str, TaskInfo]  = {}   # task_id → TaskInfo
        self._watchdog: Optional[Watchdog] = None
        
        # Пул свободных Slaves
        self.free_slaves = queue.Queue()
        # Очередь задач для динамического распределения
        self.task_queue = queue.Queue()

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover(self, timeout: float = DISCOVERY_TIMEOUT_SEC) -> dict[str, SlaveInfo]:
        """
        Обнаружить Slaves в сети.
        Возвращает словарь ip → SlaveInfo.
        """
        self.slaves = discover_slaves(timeout)
        return self.slaves

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    def run(
        self,
        script_path: str,
        tasks: list[dict],
    ) -> dict[str, TaskInfo]:
        
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Скрипт не найден: {script_path}")

        if not self.slaves:
            raise RuntimeError("Список Slaves пуст. Сначала вызовите discover().")

        # Создать объекты задач
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

        # Запустить Watchdog
        self._watchdog = Watchdog(self)
        self._watchdog.start()

        # Активные потоки dispatch
        active_threads = set()

        while not self.task_queue.empty() or active_threads:
            # Если есть свободный slave и задача — запустить
            if not self.free_slaves.empty() and not self.task_queue.empty():
                slave = self.free_slaves.get()
                task = self.task_queue.get()

                task.slave_ip = slave.ip  # Назначить slave

                t = threading.Thread(
                    target=self._dispatch_with_reassignment,
                    args=(slave, task, script_path),
                    name=f"Dispatch-{slave.ip}-{task.task_id[:8]}",
                    daemon=True,
                )
                active_threads.add(t)
                t.start()

            # Подождать немного, чтобы не грузить CPU
            time.sleep(0.1)

            # Убрать завершившиеся потоки
            active_threads = {t for t in active_threads if t.is_alive()}

        # Ждать завершения всех активных потоков
        for t in list(active_threads):
            t.join()

        # Остановить Watchdog
        self._watchdog.stop()

        # Итоговый отчёт
        self._print_report()

        return self.tasks

    def _dispatch_with_reassignment(
        self,
        slave: SlaveInfo,
        task: TaskInfo,
        script_path: str
    ) -> None:
        """
        Отправить задачу slave, и после завершения вернуть slave в пул или переназначить задачу.
        """
        logger.info("Dispatch → %s | task=%s", slave.ip, task.task_id[:8])

        slave.mark_busy(task.task_id)
        task.status = TASK_STATUS_SENT

        try:
            conn = create_tcp_client_socket(slave.ip, TCP_PORT, timeout=SOCKET_TIMEOUT_SEC)
            conn.settimeout(SOCKET_TIMEOUT_SEC)

            task_header = {
                "type":    "task",
                "task_id": task.task_id,
                "params":  task.params,
            }

            send_file(conn, task_header, script_path)
            logger.info("Скрипт отправлен → %s", slave.ip)

            self._receive_result_with_reassignment(conn, slave, task)

        except (ConnectionRefusedError, socket.timeout, Exception) as e:
            logger.error("Ошибка dispatch → %s: %s", slave.ip, e)
            self._handle_lost_task(slave, task, str(e))

    def _receive_result_with_reassignment(
        self,
        conn: socket.socket,
        slave: SlaveInfo,
        task: TaskInfo
    ) -> None:
        conn.settimeout(None)

        header, payload = recv_message(conn)
        msg_type = header.get("type")

        if msg_type == "error":
            error_msg = header.get("message", "неизвестная ошибка")
            logger.error("Slave %s вернул ошибку: %s", slave.ip, error_msg)
            self._handle_lost_task(slave, task, error_msg)
            return

        if msg_type == "result" and header.get("status") == "ok":
            os.makedirs(MASTER_RESULTS_DIR, exist_ok=True)
            filename = header.get("filename", f"{task.task_id}_{slave.ip}.result")
            result_path = os.path.join(MASTER_RESULTS_DIR, filename)

            with open(result_path, "wb") as f:
                f.write(payload)

            task.status = TASK_STATUS_DONE
            task.result_path = result_path
            slave.mark_free()
            self.free_slaves.put(slave)  # Вернуть в пул
            logger.info("Результат получен от %s → %s", slave.ip, result_path)
            return

        logger.error("Неожиданный ответ от %s: type=%s", slave.ip, msg_type)
        slave.mark_dead()
        self._handle_lost_task(slave, task, f"Unexpected message type: {msg_type}")

    def _handle_lost_task(
        self,
        slave: SlaveInfo,
        task: TaskInfo,
        reason: str,
    ) -> None:
        task.status = TASK_STATUS_LOST
        task.error = reason
        
        # Если slave мёртвый, переназначить задачу — вернуть в очередь задач
        if slave.is_dead:
            logger.warning("Slave %s мёртвый — возвращаем задачу %s в очередь", slave.ip, task.task_id[:8])
            task.status = TASK_STATUS_PENDING  # Сбросить статус
            task.slave_ip = ""  # Очистить
            self.task_queue.put(task)  # Вернуть в очередь задач
        else:
            slave.mark_free()
            self.free_slaves.put(slave)

    # ------------------------------------------------------------------
    # Отчёт
    # ------------------------------------------------------------------

    def _print_report(self):
        done  = [t for t in self.tasks.values() if t.status == TASK_STATUS_DONE]
        lost  = [t for t in self.tasks.values() if t.status == TASK_STATUS_LOST]
        other = [t for t in self.tasks.values() if t.status not in (TASK_STATUS_DONE, TASK_STATUS_LOST)]

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
# Точка входа (пример использования)
# =============================================================================

if __name__ == "__main__":
    master = Master()

    # Шаг 1: Discovery
    print("\n>>> Запуск Discovery...\n")
    slaves = master.discover()

    if not slaves:
        print("Slaves не найдены. Убедитесь, что slave.py запущен на подчинённых машинах.")
        sys.exit(1)

    print(f"\n>>> Найдено Slaves: {len(slaves)}")
    slave_ips = list(slaves.keys())
    for ip in slave_ips:
        print(f"    {ip}")

    # Шаг 2: Сформировать задачи вручную
    # (в реальном сценарии параметры формируются программой Master
    #  на основе данных, которые нужно обработать)
    SCRIPT = "program.py"   # скрипт, который будет выслан каждому Slave
    CHUNK  = 100            # условный размер чанка

    tasks = []
    for i in range(len(slave_ips)):
        start = i * CHUNK
        end   = start + CHUNK
        tasks.append({
            "params":   f"-start {start} -end {end}",
        })

    print(f"\n>>> Запуск {len(tasks)} задач на {len(slave_ips)} Slaves...\n")

    # Шаг 3: Запуск
    results = master.run(script_path=SCRIPT, tasks=tasks)

    # Шаг 4: Использовать результаты
    print("\n>>> Результаты:")
    for task_id, task in results.items():
        status = task.status
        if status == TASK_STATUS_DONE:
            print(f"  [OK]   slave={task.slave_ip}  файл={task.result_path}")
        else:
            print(f"  [LOST] slave={task.slave_ip}  причина={task.error}")

