#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# slave_gui.py — Современный графический интерфейс для Slave-агента
# =============================================================================
#
# Запуск: python slave_gui.py
#
# Особенности:
#   - Использование ttk для современного вида
#   - Вкладки: Логи, Статистика, Информация
#   - Запуск/остановка Slave с обратной связью
#   - Отображение IP, портов, статуса
#   - Подсчёт выполненных задач и ошибок
#   - Очистка логов
# =============================================================================

import sys
import os
import threading
import queue
import logging
import time
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox

# Импортируем компоненты из slave.py
from slave import (
    DiscoveryListener,
    HeartbeatListener,
    TaskServer,
    get_local_ip,
    SLAVE_WORK_DIR,
    MASTER_RESULTS_DIR,
    UDP_BROADCAST_PORT,
    TCP_PORT,
    HEARTBEAT_UDP_PORT,
)

# =============================================================================
# Настройка логирования с перенаправлением в GUI
# =============================================================================

class QueueHandler(logging.Handler):
    """Кастомный обработчик логов, отправляющий сообщения в очередь для GUI."""
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(self.format(record))

def configure_gui_logging(log_queue, level=logging.INFO):
    """Настраивает корневой логгер для отправки сообщений в GUI."""
    handler = QueueHandler(log_queue)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s",
                                  datefmt="%H:%M:%S")
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(level)

    # Также выводим в файл (как в slave.py)
    file_handler = logging.FileHandler("slave_gui.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logging.root.addHandler(file_handler)


# =============================================================================
# GUI приложения
# =============================================================================

class SlaveGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title(f"Slave Agent — {get_local_ip()}")
        self.root.geometry("900x650")
        self.root.minsize(800, 500)

        # Стилизация
        self.style = ttk.Style()
        self.style.theme_use('clam')  # современная тема
        self.style.configure('TNotebook.Tab', padding=[10, 5])
        self.style.configure('Accent.TButton', foreground='white', background='#2c3e50')
        self.style.map('Accent.TButton', background=[('active', '#1a252f')])

        # Переменные состояния
        self.running = False
        self.discovery_thread = None
        self.heartbeat_thread = None
        self.task_server_thread = None
        self.log_queue = queue.Queue()
        configure_gui_logging(self.log_queue, level=logging.INFO)
        self.logger = logging.getLogger("slave_gui")

        # Статистика
        self.stats = {
            'tasks_received': 0,
            'tasks_succeeded': 0,
            'tasks_failed': 0,
        }

        # Построение интерфейса
        self.create_menu()
        self.create_widgets()

        # Запуск опроса очереди логов и обновления статистики
        self.poll_log_queue()
        self.update_status_display()

        # Автоматический запуск Slave (можно убрать, если нужно ручное управление)
        self.start_slave()

    def create_menu(self):
        """Создаёт главное меню."""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Файл", menu=file_menu)
        file_menu.add_command(label="Очистить логи", command=self.clear_logs)
        file_menu.add_separator()
        file_menu.add_command(label="Выход", command=self.on_closing)

        control_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Управление", menu=control_menu)
        control_menu.add_command(label="Запустить Slave", command=self.start_slave)
        control_menu.add_command(label="Остановить Slave", command=self.stop_slave)

        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Справка", menu=help_menu)
        help_menu.add_command(label="О программе", command=self.show_about)

    def create_widgets(self):
        """Создаёт все виджеты главного окна."""
        # Основной контейнер
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Верхняя панель с информацией
        info_frame = ttk.LabelFrame(main_frame, text="Информация о Slave", padding="5")
        info_frame.pack(fill=tk.X, pady=(0, 10))

        # Сетка информации
        ttk.Label(info_frame, text="IP адрес:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        self.ip_label = ttk.Label(info_frame, text=get_local_ip(), foreground="blue")
        self.ip_label.grid(row=0, column=1, sticky="w", padx=5, pady=2)

        ttk.Label(info_frame, text="Статус:").grid(row=0, column=2, sticky="w", padx=5, pady=2)
        self.status_label = ttk.Label(info_frame, text="Запуск...", foreground="orange")
        self.status_label.grid(row=0, column=3, sticky="w", padx=5, pady=2)

        ttk.Label(info_frame, text="UDP discovery порт:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(info_frame, text=str(UDP_BROADCAST_PORT)).grid(row=1, column=1, sticky="w", padx=5, pady=2)

        ttk.Label(info_frame, text="TCP task порт:").grid(row=1, column=2, sticky="w", padx=5, pady=2)
        ttk.Label(info_frame, text=str(TCP_PORT)).grid(row=1, column=3, sticky="w", padx=5, pady=2)

        ttk.Label(info_frame, text="UDP heartbeat порт:").grid(row=2, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(info_frame, text=str(HEARTBEAT_UDP_PORT)).grid(row=2, column=1, sticky="w", padx=5, pady=2)

        ttk.Label(info_frame, text="Рабочая директория:").grid(row=3, column=0, sticky="w", padx=5, pady=2)
        workdir = os.path.abspath(SLAVE_WORK_DIR)
        ttk.Label(info_frame, text=workdir, foreground="gray").grid(row=3, column=1, columnspan=3, sticky="w", padx=5, pady=2)

        # Панель управления
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))

        self.start_button = ttk.Button(control_frame, text="▶ Запустить Slave", command=self.start_slave, width=18)
        self.start_button.pack(side=tk.LEFT, padx=5)

        self.stop_button = ttk.Button(control_frame, text="⏹ Остановить Slave", command=self.stop_slave, width=18)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        self.clear_log_button = ttk.Button(control_frame, text="🗑 Очистить логи", command=self.clear_logs, width=18)
        self.clear_log_button.pack(side=tk.LEFT, padx=5)

        # Прогресс-бар для индикации активности (просто декоративный)
        self.progress = ttk.Progressbar(control_frame, mode='indeterminate', length=150)
        self.progress.pack(side=tk.RIGHT, padx=5)

        # Вкладки
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Вкладка "Логи"
        self.log_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.log_frame, text="📋 Логи работы")
        self.create_log_tab()

        # Вкладка "Статистика"
        self.stats_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.stats_frame, text="📊 Статистика")
        self.create_stats_tab()

        # Вкладка "Информация"
        self.info_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.info_tab, text="ℹ️ О системе")
        self.create_info_tab()

        # Строка состояния внизу
        self.status_bar = ttk.Label(self.root, text="Готов", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def create_log_tab(self):
        """Создаёт область для вывода логов."""
        # Текстовое поле с прокруткой
        self.log_text = scrolledtext.ScrolledText(self.log_frame, wrap=tk.WORD, font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.log_text.config(state=tk.DISABLED)

    def create_stats_tab(self):
        """Создаёт виджеты для отображения статистики."""
        frame = ttk.Frame(self.stats_frame, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="Статистика выполнения задач", font=("Arial", 12, "bold")).pack(pady=(0, 20))

        # Карточки статистики
        stats_card = ttk.Frame(frame)
        stats_card.pack()

        # Получено задач
        self.tasks_received_var = tk.StringVar(value="0")
        card1 = ttk.Frame(stats_card, relief=tk.RIDGE, padding=10)
        card1.grid(row=0, column=0, padx=10, pady=10)
        ttk.Label(card1, text="📥 Получено задач", font=("Arial", 10)).pack()
        ttk.Label(card1, textvariable=self.tasks_received_var, font=("Arial", 24, "bold")).pack()

        # Выполнено успешно
        self.tasks_succeeded_var = tk.StringVar(value="0")
        card2 = ttk.Frame(stats_card, relief=tk.RIDGE, padding=10)
        card2.grid(row=0, column=1, padx=10, pady=10)
        ttk.Label(card2, text="✅ Успешно", font=("Arial", 10)).pack()
        ttk.Label(card2, textvariable=self.tasks_succeeded_var, font=("Arial", 24, "bold")).pack()

        # Ошибок
        self.tasks_failed_var = tk.StringVar(value="0")
        card3 = ttk.Frame(stats_card, relief=tk.RIDGE, padding=10)
        card3.grid(row=0, column=2, padx=10, pady=10)
        ttk.Label(card3, text="❌ Ошибок", font=("Arial", 10)).pack()
        ttk.Label(card3, textvariable=self.tasks_failed_var, font=("Arial", 24, "bold")).pack()

        # Кнопка сброса статистики
        ttk.Button(frame, text="Сбросить статистику", command=self.reset_stats).pack(pady=20)

    def create_info_tab(self):
        """Создаёт информационную вкладку."""
        frame = ttk.Frame(self.info_tab, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)

        info_text = f"""
        ⚙️ Slave-агент для распределённой системы Master-Slave

        📡 Сетевые порты:
           • UDP discovery: {UDP_BROADCAST_PORT}
           • TCP задачи:    {TCP_PORT}
           • UDP heartbeat: {HEARTBEAT_UDP_PORT}

        📁 Директории:
           • Рабочая: {os.path.abspath(SLAVE_WORK_DIR)}
           • Результаты: {os.path.abspath(MASTER_RESULTS_DIR)}

        🔄 Компоненты:
           • DiscoveryListener – отвечает на широковещательные запросы Master
           • HeartbeatListener – отвечает на PING для проверки живости
           • TaskServer – принимает и выполняет задачи

        🧠 Особенности:
           • Автоматическое определение IP-адреса
           • Многопоточная обработка задач
           • Логирование в файл slave_gui.log

        © 2025
        """
        info_label = ttk.Label(frame, text=info_text, justify=tk.LEFT, font=("Consolas", 10))
        info_label.pack(anchor=tk.W)

    # =========================================================================
    # Логика управления Slave
    # =========================================================================

    def start_slave(self):
        """Запускает фоновые потоки Slave."""
        if self.running:
            self.logger.warning("Slave уже запущен")
            return

        self.running = True
        local_ip = get_local_ip()

        # Создаём и запускаем потоки
        self.discovery_thread = DiscoveryListener(local_ip)
        self.heartbeat_thread = HeartbeatListener()
        self.task_server_thread = TaskServer(local_ip)

        self.discovery_thread.start()
        self.heartbeat_thread.start()
        self.task_server_thread.start()

        self.logger.info("🚀 Slave успешно запущен на IP %s", local_ip)
        self.logger.info("📡 UDP discovery порт: %d", UDP_BROADCAST_PORT)
        self.logger.info("🔌 TCP task порт: %d", TCP_PORT)
        self.logger.info("💓 UDP heartbeat порт: %d", HEARTBEAT_UDP_PORT)
        self.logger.info("📁 Рабочая директория: %s", os.path.abspath(SLAVE_WORK_DIR))

        self.status_label.config(text="РАБОТАЕТ", foreground="green")
        self.status_bar.config(text="Активен. Ожидание задач от Master...")
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.progress.start(10)

    def stop_slave(self):
        """Останавливает все фоновые потоки."""
        if not self.running:
            return

        self.logger.info("🛑 Остановка Slave...")
        self.status_label.config(text="Остановка...", foreground="orange")

        if self.discovery_thread:
            self.discovery_thread.stop()
        if self.heartbeat_thread:
            self.heartbeat_thread.stop()
        if self.task_server_thread:
            self.task_server_thread.stop()

        # Даём время потокам на завершение
        time.sleep(0.5)

        self.running = False
        self.status_label.config(text="ОСТАНОВЛЕН", foreground="red")
        self.status_bar.config(text="Slave остановлен.")
        self.logger.info("✅ Slave остановлен.")
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.progress.stop()

    def clear_logs(self):
        """Очищает текстовое поле с логами."""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        self.logger.info("Логи очищены")

    def reset_stats(self):
        """Сбрасывает статистику."""
        self.stats = {'tasks_received': 0, 'tasks_succeeded': 0, 'tasks_failed': 0}
        self.update_stats_display()
        self.logger.info("Статистика сброшена")

    # =========================================================================
    # Обновление интерфейса
    # =========================================================================

    def poll_log_queue(self):
        """Периодически забирает сообщения из очереди и выводит их в лог."""
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_text.config(state=tk.NORMAL)
                self.log_text.insert(tk.END, msg + "\n")
                self.log_text.see(tk.END)
                self.log_text.config(state=tk.DISABLED)

                # Обновляем статистику на основе сообщений
                if "Принимаем скрипт от Master" in msg:
                    self.stats['tasks_received'] += 1
                    self.update_stats_display()
                elif "Скрипт выполнен успешно" in msg:
                    self.stats['tasks_succeeded'] += 1
                    self.update_stats_display()
                elif "Скрипт завершился с ошибкой" in msg or "ошибка" in msg.lower():
                    self.stats['tasks_failed'] += 1
                    self.update_stats_display()
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.poll_log_queue)

    def update_stats_display(self):
        """Обновляет значения на вкладке статистики."""
        self.tasks_received_var.set(str(self.stats['tasks_received']))
        self.tasks_succeeded_var.set(str(self.stats['tasks_succeeded']))
        self.tasks_failed_var.set(str(self.stats['tasks_failed']))

    def update_status_display(self):
        """Периодически обновляет статус (можно добавить проверку потоков)."""
        if self.running:
            # Простая проверка: если потоки мертвы, но флаг running=True – восстанавливаем?
            if (self.discovery_thread and not self.discovery_thread.is_alive()) or \
               (self.heartbeat_thread and not self.heartbeat_thread.is_alive()) or \
               (self.task_server_thread and not self.task_server_thread.is_alive()):
                self.logger.warning("Один из потоков завершился неожиданно")
                self.status_label.config(text="НЕСТАБИЛЕН", foreground="orange")
            else:
                self.status_label.config(text="РАБОТАЕТ", foreground="green")
        self.root.after(3000, self.update_status_display)

    def show_about(self):
        """Показывает окно 'О программе'."""
        about_text = (
            "Slave Agent v2.0\n"
            "Графический интерфейс для подчинённого узла\n"
            "в распределённой системе Master-Slave.\n\n"
            "Разработан с использованием Python и Tkinter."
        )
        messagebox.showinfo("О программе", about_text)

    def on_closing(self):
        """Обработчик закрытия окна."""
        if self.running:
            if messagebox.askokcancel("Выход", "Slave всё ещё работает. Остановить и выйти?"):
                self.stop_slave()
                self.root.destroy()
        else:
            self.root.destroy()

    def run(self):
        """Запускает главный цикл GUI."""
        self.root.mainloop()


# =============================================================================
# Точка входа
# =============================================================================

if __name__ == "__main__":
    # Убедимся, что рабочие директории существуют
    os.makedirs(SLAVE_WORK_DIR, exist_ok=True)
    os.makedirs(MASTER_RESULTS_DIR, exist_ok=True)

    app = SlaveGUI()
    app.run()