#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# master_gui.py — Графический интерфейс для Master-агента
# =============================================================================
#
# Запуск: python master_gui.py
#
# Особенности:
#   - Использование ttk для современного вида
#   - Вкладки: Слейвы, Задачи, Логи, Результаты
#   - Обнаружение слейвов (Discovery)
#   - Создание задач (из CSV/JSON или вручную)
#   - Запуск распределения задач с прогрессом
#   - Отображение статуса слейвов и результатов
# =============================================================================

import sys
import os
import threading
import queue
import logging
import time
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog

# Добавляем путь к текущей директории для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from master import Master, SlaveInfo, TaskInfo
from config import (
    TASK_STATUS_DONE, TASK_STATUS_LOST, TASK_STATUS_PENDING, TASK_STATUS_SENT,
    SLAVE_STATUS_ALIVE, SLAVE_STATUS_BUSY, SLAVE_STATUS_DEAD,
    MASTER_RESULTS_DIR
)

# =============================================================================
# Настройка логирования с перенаправлением в GUI
# =============================================================================

class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(self.format(record))

def configure_gui_logging(log_queue, level=logging.INFO):
    handler = QueueHandler(log_queue)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s",
                                  datefmt="%H:%M:%S")
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(level)

    # Также пишем в файл
    file_handler = logging.FileHandler("master_gui.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logging.root.addHandler(file_handler)


# =============================================================================
# GUI приложения
# =============================================================================

class MasterGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Master Agent — Управление распределёнными задачами")
        self.root.geometry("1000x700")
        self.root.minsize(900, 600)

        # Стилизация
        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.style.configure('TNotebook.Tab', padding=[10, 5])
        self.style.configure('Accent.TButton', foreground='white', background='#2c3e50')
        self.style.map('Accent.TButton', background=[('active', '#1a252f')])

        # Переменные
        self.master = None
        self.running = False          # выполняется ли распределение задач
        self.stop_requested = False   # флаг для прерывания выполнения
        self.log_queue = queue.Queue()
        configure_gui_logging(self.log_queue, level=logging.INFO)
        self.logger = logging.getLogger("master_gui")

        # Данные
        self.slaves_dict = {}          # ip -> SlaveInfo
        self.tasks_dict = {}           # task_id -> TaskInfo
        self.script_path = tk.StringVar(value="program.py")
        self.tasks_list = []           # список словарей {"params": ...}

        # Построение интерфейса
        self.create_menu()
        self.create_widgets()

        # Запуск опроса очереди логов и обновления статусов
        self.poll_log_queue()
        self.refresh_slaves_display()  # периодическое обновление

        # Экземпляр Master создадим при первом discover
        self.master = Master()

    def create_menu(self):
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Файл", menu=file_menu)
        file_menu.add_command(label="Очистить логи", command=self.clear_logs)
        file_menu.add_separator()
        file_menu.add_command(label="Выход", command=self.on_closing)

        control_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Управление", menu=control_menu)
        control_menu.add_command(label="Обнаружить слейвы", command=self.discover_slaves)
        control_menu.add_command(label="Загрузить задачи из CSV", command=self.load_tasks_csv)
        control_menu.add_command(label="Загрузить задачи из JSON", command=self.load_tasks_json)
        control_menu.add_command(label="Запустить задачи", command=self.run_tasks)
        control_menu.add_separator()
        control_menu.add_command(label="Остановить выполнение", command=self.stop_tasks)

        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Справка", menu=help_menu)
        help_menu.add_command(label="О программе", command=self.show_about)

    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Панель управления
        control_frame = ttk.LabelFrame(main_frame, text="Управление", padding="5")
        control_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Button(control_frame, text="🔍 Обнаружить слейвы", command=self.discover_slaves).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="📂 Загрузить задачи", command=self.load_tasks_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="▶ Запустить задачи", command=self.run_tasks, width=15).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="⏹ Остановить", command=self.stop_tasks, width=10).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="🗑 Очистить логи", command=self.clear_logs).pack(side=tk.LEFT, padx=5)

        # Путь к скрипту
        script_frame = ttk.Frame(control_frame)
        script_frame.pack(side=tk.LEFT, padx=(20, 5))
        ttk.Label(script_frame, text="Скрипт:").pack(side=tk.LEFT)
        self.script_entry = ttk.Entry(script_frame, textvariable=self.script_path, width=30)
        self.script_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(script_frame, text="Обзор", command=self.browse_script).pack(side=tk.LEFT)

        # Прогресс-бар
        self.progress = ttk.Progressbar(control_frame, mode='indeterminate', length=150)
        self.progress.pack(side=tk.RIGHT, padx=5)

        # Вкладки
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Вкладка "Слейвы"
        self.slaves_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.slaves_frame, text="🖥️ Слейвы")
        self.create_slaves_tab()

        # Вкладка "Задачи"
        self.tasks_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tasks_frame, text="📋 Задачи")
        self.create_tasks_tab()

        # Вкладка "Логи"
        self.log_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.log_frame, text="📄 Логи")
        self.create_log_tab()

        # Вкладка "Результаты"
        self.results_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.results_frame, text="✅ Результаты")
        self.create_results_tab()

        # Строка состояния
        self.status_bar = ttk.Label(self.root, text="Готов", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    # =========================================================================
    # Вкладка "Слейвы"
    # =========================================================================
    def create_slaves_tab(self):
        frame = ttk.Frame(self.slaves_frame, padding="5")
        frame.pack(fill=tk.BOTH, expand=True)

        # Дерево для отображения слейвов
        columns = ("IP", "Статус", "Текущая задача")
        self.slaves_tree = ttk.Treeview(frame, columns=columns, show="headings", height=10)
        self.slaves_tree.heading("IP", text="IP адрес")
        self.slaves_tree.heading("Статус", text="Статус")
        self.slaves_tree.heading("Текущая задача", text="Текущая задача")
        self.slaves_tree.column("IP", width=150)
        self.slaves_tree.column("Статус", width=100)
        self.slaves_tree.column("Текущая задача", width=200)

        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.slaves_tree.yview)
        self.slaves_tree.configure(yscrollcommand=scrollbar.set)
        self.slaves_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        ttk.Button(frame, text="Обновить список", command=self.refresh_slaves_display).pack(pady=5)

    def refresh_slaves_display(self):
        """Обновляет таблицу слейвов из self.master.slaves."""
        if not self.master or not hasattr(self.master, 'slaves'):
            return
        for item in self.slaves_tree.get_children():
            self.slaves_tree.delete(item)

        for ip, slave in self.master.slaves.items():
            status_text = slave.status
            if slave.status == SLAVE_STATUS_ALIVE:
                status_text = "🟢 Жив"
            elif slave.status == SLAVE_STATUS_BUSY:
                status_text = "🟡 Занят"
            elif slave.status == SLAVE_STATUS_DEAD:
                status_text = "🔴 Мёртв"
            task_id_short = slave.task_id[:8] if slave.task_id else ""
            self.slaves_tree.insert("", tk.END, values=(ip, status_text, task_id_short))

        # Периодическое обновление
        self.root.after(3000, self.refresh_slaves_display)

    # =========================================================================
    # Вкладка "Задачи"
    # =========================================================================
    def create_tasks_tab(self):
        frame = ttk.Frame(self.tasks_frame, padding="5")
        frame.pack(fill=tk.BOTH, expand=True)

        # Таблица задач
        columns = ("ID", "Слейв", "Параметры", "Статус", "Результат")
        self.tasks_tree = ttk.Treeview(frame, columns=columns, show="headings", height=15)
        self.tasks_tree.heading("ID", text="ID задачи")
        self.tasks_tree.heading("Слейв", text="Слейв")
        self.tasks_tree.heading("Параметры", text="Параметры")
        self.tasks_tree.heading("Статус", text="Статус")
        self.tasks_tree.heading("Результат", text="Результат")
        self.tasks_tree.column("ID", width=120)
        self.tasks_tree.column("Слейв", width=120)
        self.tasks_tree.column("Параметры", width=250)
        self.tasks_tree.column("Статус", width=100)
        self.tasks_tree.column("Результат", width=200)

        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.tasks_tree.yview)
        self.tasks_tree.configure(yscrollcommand=scrollbar.set)
        self.tasks_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Панель для добавления задачи вручную
        manual_frame = ttk.LabelFrame(frame, text="Добавить задачу вручную", padding="5")
        manual_frame.pack(fill=tk.X, pady=5)
        ttk.Label(manual_frame, text="Параметры:").pack(side=tk.LEFT)
        self.manual_params = ttk.Entry(manual_frame, width=50)
        self.manual_params.pack(side=tk.LEFT, padx=5)
        ttk.Button(manual_frame, text="➕ Добавить", command=self.add_manual_task).pack(side=tk.LEFT, padx=5)
        ttk.Button(manual_frame, text="🗑 Очистить все задачи", command=self.clear_all_tasks).pack(side=tk.LEFT, padx=5)

        # Отображение количества задач
        self.tasks_count_label = ttk.Label(frame, text="Задач: 0")
        self.tasks_count_label.pack(pady=5)

    def update_tasks_display(self):
        """Обновляет таблицу задач из self.tasks_dict."""
        for item in self.tasks_tree.get_children():
            self.tasks_tree.delete(item)

        for task_id, task in self.tasks_dict.items():
            status_display = task.status
            if task.status == TASK_STATUS_DONE:
                status_display = "✅ Выполнена"
            elif task.status == TASK_STATUS_LOST:
                status_display = "❌ Потеряна"
            elif task.status == TASK_STATUS_SENT:
                status_display = "📤 Отправлена"
            elif task.status == TASK_STATUS_PENDING:
                status_display = "⏳ Ожидает"
            result_path = os.path.basename(task.result_path) if task.result_path else ""
            self.tasks_tree.insert("", tk.END, values=(
                task_id[:8], task.slave_ip, task.params, status_display, result_path
            ))

        self.tasks_count_label.config(text=f"Задач: {len(self.tasks_dict)}")
        self.root.after(2000, self.update_tasks_display)

    def add_manual_task(self):
        params = self.manual_params.get().strip()
        if not params:
            messagebox.showwarning("Предупреждение", "Введите параметры задачи")
            return
        self.tasks_list.append({"params": params})
        # Временно сохраним задачи в словарь без task_id (они будут созданы при run)
        # Пока просто покажем в таблице
        temp_id = f"new_{len(self.tasks_list)}"
        # Создаём временный объект для отображения
        task_info = TaskInfo(task_id=temp_id, slave_ip="", params=params)
        task_info.status = TASK_STATUS_PENDING
        self.tasks_dict[temp_id] = task_info
        self.manual_params.delete(0, tk.END)
        self.update_tasks_display()
        self.logger.info(f"Добавлена задача: {params}")

    def clear_all_tasks(self):
        self.tasks_list.clear()
        self.tasks_dict.clear()
        self.update_tasks_display()
        self.logger.info("Все задачи удалены")

    # =========================================================================
    # Вкладка "Логи"
    # =========================================================================
    def create_log_tab(self):
        self.log_text = scrolledtext.ScrolledText(self.log_frame, wrap=tk.WORD, font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.log_text.config(state=tk.DISABLED)

    def clear_logs(self):
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        self.logger.info("Логи очищены")

    # =========================================================================
    # Вкладка "Результаты"
    # =========================================================================
    def create_results_tab(self):
        frame = ttk.Frame(self.results_frame, padding="5")
        frame.pack(fill=tk.BOTH, expand=True)

        # Текстовое поле для отображения отчёта
        self.results_text = scrolledtext.ScrolledText(frame, wrap=tk.WORD, font=("Consolas", 10))
        self.results_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.results_text.config(state=tk.DISABLED)

        ttk.Button(frame, text="Обновить отчёт", command=self.generate_report).pack(pady=5)

    def generate_report(self):
        """Формирует отчёт по выполненным задачам."""
        self.results_text.config(state=tk.NORMAL)
        self.results_text.delete(1.0, tk.END)

        if not self.tasks_dict:
            self.results_text.insert(tk.END, "Нет задач для отображения.\n")
            self.results_text.config(state=tk.DISABLED)
            return

        done = [t for t in self.tasks_dict.values() if t.status == TASK_STATUS_DONE]
        lost = [t for t in self.tasks_dict.values() if t.status == TASK_STATUS_LOST]

        report = f"""
{'='*60}
ИТОГОВЫЙ ОТЧЁТ
{'='*60}
Всего задач: {len(self.tasks_dict)}
✅ Выполнено: {len(done)}
❌ Потеряно: {len(lost)}
{'='*60}

Выполненные задачи:
"""
        for t in done:
            report += f"  [{t.task_id[:8]}] slave={t.slave_ip}  результат={t.result_path}\n"

        if lost:
            report += "\nПотерянные задачи:\n"
            for t in lost:
                report += f"  [{t.task_id[:8]}] slave={t.slave_ip}  причина={t.error}\n"

        self.results_text.insert(tk.END, report)
        self.results_text.config(state=tk.DISABLED)

    # =========================================================================
    # Логика работы
    # =========================================================================
    def browse_script(self):
        filename = filedialog.askopenfilename(
            title="Выберите скрипт для выполнения",
            filetypes=[("Python files", "*.py"), ("All files", "*.*")]
        )
        if filename:
            self.script_path.set(filename)

    def discover_slaves(self):
        """Запускает discovery в отдельном потоке."""
        def discover():
            self.status_bar.config(text="Обнаружение слейвов...")
            self.logger.info("Запуск Discovery...")
            try:
                slaves = self.master.discover()
                self.logger.info(f"Обнаружено слейвов: {len(slaves)}")
                self.status_bar.config(text=f"Обнаружено {len(slaves)} слейвов")
                self.refresh_slaves_display()
            except Exception as e:
                self.logger.error(f"Ошибка discovery: {e}")
                self.status_bar.config(text="Ошибка discovery")

        threading.Thread(target=discover, daemon=True).start()

    def load_tasks_dialog(self):
        """Диалог выбора формата загрузки задач."""
        choice = messagebox.askyesno("Загрузка задач", "Загрузить из CSV? (Нет — из JSON)")
        if choice:
            self.load_tasks_csv()
        else:
            self.load_tasks_json()

    def load_tasks_csv(self):
        filename = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not filename:
            return
        try:
            import csv
            self.tasks_list.clear()
            self.tasks_dict.clear()
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row:
                        params = row[0].strip()
                        self.tasks_list.append({"params": params})
            self.logger.info(f"Загружено {len(self.tasks_list)} задач из CSV")
            # Создаём временные записи для отображения
            for i, task in enumerate(self.tasks_list):
                temp_id = f"csv_{i+1}"
                task_info = TaskInfo(task_id=temp_id, slave_ip="", params=task["params"])
                task_info.status = TASK_STATUS_PENDING
                self.tasks_dict[temp_id] = task_info
            self.update_tasks_display()
            self.status_bar.config(text=f"Загружено {len(self.tasks_list)} задач")
        except Exception as e:
            self.logger.error(f"Ошибка загрузки CSV: {e}")
            messagebox.showerror("Ошибка", f"Не удалось загрузить CSV:\n{e}")

    def load_tasks_json(self):
        filename = filedialog.askopenfilename(filetypes=[("JSON files", "*.json")])
        if not filename:
            return
        try:
            import json
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list):
                self.tasks_list = data  # ожидаем список объектов с ключом "params"
            else:
                self.tasks_list = data.get("tasks", [])
            self.logger.info(f"Загружено {len(self.tasks_list)} задач из JSON")
            for i, task in enumerate(self.tasks_list):
                temp_id = f"json_{i+1}"
                task_info = TaskInfo(task_id=temp_id, slave_ip="", params=task["params"])
                task_info.status = TASK_STATUS_PENDING
                self.tasks_dict[temp_id] = task_info
            self.update_tasks_display()
            self.status_bar.config(text=f"Загружено {len(self.tasks_list)} задач")
        except Exception as e:
            self.logger.error(f"Ошибка загрузки JSON: {e}")
            messagebox.showerror("Ошибка", f"Не удалось загрузить JSON:\n{e}")

    def run_tasks(self):
        """Запускает распределение задач в отдельном потоке."""
        if self.running:
            messagebox.showwarning("Внимание", "Задачи уже выполняются")
            return
        if not self.tasks_list:
            messagebox.showwarning("Внимание", "Нет загруженных задач")
            return
        if not self.master.slaves:
            messagebox.showwarning("Внимание", "Нет обнаруженных слейвов. Сначала выполните Discovery.")
            return
        if not os.path.isfile(self.script_path.get()):
            messagebox.showerror("Ошибка", f"Скрипт не найден: {self.script_path.get()}")
            return

        self.running = True
        self.stop_requested = False
        self.progress.start(10)
        self.status_bar.config(text="Выполнение задач...")

        # Запускаем в потоке
        def worker():
            try:
                # Копируем список задач (Master создаст TaskInfo внутри run)
                # Очистим временные записи в tasks_dict, чтобы run создал новые
                self.tasks_dict.clear()
                self.update_tasks_display()

                # Вызов master.run
                results = self.master.run(
                    script_path=self.script_path.get(),
                    tasks=self.tasks_list
                )
                # Обновляем словарь задач
                self.tasks_dict = results
                self.update_tasks_display()
                self.generate_report()
                self.logger.info("Распределение задач завершено")
                self.status_bar.config(text="Выполнение завершено")
            except Exception as e:
                self.logger.error(f"Ошибка при выполнении: {e}")
                self.status_bar.config(text="Ошибка выполнения")
            finally:
                self.running = False
                self.progress.stop()
                self.stop_requested = False

        threading.Thread(target=worker, daemon=True).start()

    def stop_tasks(self):
        """Попытка остановить выполнение задач (устанавливает флаг)."""
        if not self.running:
            return
        self.stop_requested = True
        self.logger.warning("Запрошена остановка выполнения (не гарантируется мгновенная остановка)")
        self.status_bar.config(text="Остановка задач...")

    def show_about(self):
        about_text = (
            "Master Agent v2.0\n"
            "Графический интерфейс для управляющего узла\n"
            "в распределённой системе Master-Slave.\n\n"
            "Разработан с использованием Python и Tkinter."
        )
        messagebox.showinfo("О программе", about_text)

    def poll_log_queue(self):
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_text.config(state=tk.NORMAL)
                self.log_text.insert(tk.END, msg + "\n")
                self.log_text.see(tk.END)
                self.log_text.config(state=tk.DISABLED)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.poll_log_queue)

    def on_closing(self):
        if self.running:
            if messagebox.askokcancel("Выход", "Задачи ещё выполняются. Прервать и выйти?"):
                self.stop_tasks()
                self.root.destroy()
        else:
            self.root.destroy()

    def run(self):
        self.root.mainloop()


# =============================================================================
# Точка входа
# =============================================================================
if __name__ == "__main__":
    # Создаём директорию для результатов, если её нет
    os.makedirs(MASTER_RESULTS_DIR, exist_ok=True)

    app = MasterGUI()
    app.run()