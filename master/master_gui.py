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
#   - Вкладки: Слейвы, Задачи и прогресс, Логи, Результаты
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from master import Master, SlaveInfo, TaskInfo
from config import (
    TASK_STATUS_DONE, TASK_STATUS_LOST, TASK_STATUS_PENDING, TASK_STATUS_SENT,
    SLAVE_STATUS_ALIVE, SLAVE_STATUS_BUSY, SLAVE_STATUS_DEAD,
    MASTER_RESULTS_DIR
)

# =============================================================================
# Логирование → GUI
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
    file_handler = logging.FileHandler("master_gui.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logging.root.addHandler(file_handler)


# =============================================================================
# Виджет одной задачи с прогресс-баром
# =============================================================================

class TaskProgressWidget:
    """
    Один "ряд" на вкладке Прогресс:
      [ID задачи] [Slave IP] [Параметры] [Прогресс-бар] [% / статус]
    """

    def __init__(self, parent: tk.Widget, task: TaskInfo, row: int):
        self.task    = task
        self.frame   = ttk.Frame(parent, padding=(0, 2))
        self.frame.grid(row=row, column=0, sticky="ew", padx=8, pady=2)
        parent.columnconfigure(0, weight=1)

        # ID задачи (короткий)
        ttk.Label(
            self.frame,
            text=task.task_id[:8],
            font=("Consolas", 8),
            foreground="#888",
            width=10,
        ).grid(row=0, column=0, sticky="w")

        # Slave IP
        self.slave_label = ttk.Label(self.frame, text=task.slave_ip or "—", width=14)
        self.slave_label.grid(row=0, column=1, sticky="w", padx=(4, 0))

        # Параметры (коротко)
        params_short = task.params[:30] + "…" if len(task.params) > 30 else task.params
        ttk.Label(self.frame, text=params_short, width=32, foreground="#555").grid(
            row=0, column=2, sticky="w", padx=(4, 0)
        )

        # Прогресс-бар
        self.progress_var = tk.IntVar(value=0)
        self.progressbar  = ttk.Progressbar(
            self.frame,
            variable=self.progress_var,
            maximum=100,
            length=220,
            mode="determinate",
        )
        self.progressbar.grid(row=0, column=3, sticky="ew", padx=(8, 4))
        self.frame.columnconfigure(3, weight=1)

        # Процент / статус
        self.pct_label = ttk.Label(self.frame, text="0%", width=12, anchor="e")
        self.pct_label.grid(row=0, column=4, sticky="e")

    def update(self, task: TaskInfo):
        """Вызывается из главного потока Tkinter."""
        self.slave_label.config(text=task.slave_ip or "—")

        if task.status == TASK_STATUS_DONE:
            self.progress_var.set(100)
            self.pct_label.config(text="✅ Готово", foreground="green")
        elif task.status == TASK_STATUS_LOST:
            self.pct_label.config(text="❌ Ошибка", foreground="red")
        elif task.status == TASK_STATUS_SENT:
            pct = int(task.percent)
            self.progress_var.set(pct)
            label = f"{pct}%  ({task.done}/{task.total})"
            self.pct_label.config(text=label, foreground="#333")
        else:
            self.pct_label.config(text="⏳ Ожидание", foreground="#999")


# =============================================================================
# GUI приложения
# =============================================================================

class MasterGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Master Agent — Управление распределёнными задачами")
        self.root.geometry("1100x750")
        self.root.minsize(950, 620)

        self.style = ttk.Style()
        self.style.theme_use("clam")
        self.style.configure("TNotebook.Tab", padding=[10, 5])

        # Состояние
        self.master         = None
        self.running        = False
        self.stop_requested = False
        self.log_queue      = queue.Queue()
        configure_gui_logging(self.log_queue, level=logging.INFO)
        self.logger = logging.getLogger("master_gui")

        # Данные
        self.slaves_dict: dict[str, SlaveInfo] = {}
        self.tasks_dict:  dict[str, TaskInfo]  = {}
        self.script_path = tk.StringVar(value="program.py")
        self.tasks_list: list[dict] = []

        # Виджеты прогресс-баров: task_id → TaskProgressWidget
        self._progress_widgets: dict[str, TaskProgressWidget] = {}

        self.create_menu()
        self.create_widgets()
        self.poll_log_queue()
        self.refresh_slaves_display()
        self._poll_tasks_display()   # периодическое обновление таблицы задач

        self.master = Master()
        # Подключаем колбэк прогресса
        self.master.on_progress = self._on_progress_callback

    # -------------------------------------------------------------------------
    # Колбэк прогресса (вызывается из фонового потока Master)
    # -------------------------------------------------------------------------

    def _on_progress_callback(self, task: TaskInfo) -> None:
        """
        Вызывается из потока Master при каждом PROGRESS-сообщении.
        Потокобезопасно планирует обновление GUI через root.after().
        """
        self.root.after(0, self._update_progress_widget, task.task_id)

    def _update_progress_widget(self, task_id: str) -> None:
        """Обновить виджет прогресса задачи. Вызывается в главном потоке."""
        task = self.tasks_dict.get(task_id)
        widget = self._progress_widgets.get(task_id)
        if task and widget:
            widget.update(task)

        # Также обновить строку в таблице задач
        self._refresh_task_row(task_id)

    def _refresh_task_row(self, task_id: str) -> None:
        """Обновить одну строку в таблице задач."""
        task = self.tasks_dict.get(task_id)
        if not task:
            return
        for item in self.tasks_tree.get_children():
            vals = self.tasks_tree.item(item, "values")
            if vals and vals[0] == task_id[:8]:
                status_display = self._task_status_text(task)
                result_path    = os.path.basename(task.result_path) if task.result_path else ""
                progress_text  = f"{task.percent:.0f}%"
                self.tasks_tree.item(item, values=(
                    task_id[:8], task.slave_ip, task.params,
                    status_display, progress_text, result_path
                ))
                return

    @staticmethod
    def _task_status_text(task: TaskInfo) -> str:
        if task.status == TASK_STATUS_DONE:    return "✅ Выполнена"
        if task.status == TASK_STATUS_LOST:    return "❌ Потеряна"
        if task.status == TASK_STATUS_SENT:    return "📤 Выполняется"
        return "⏳ Ожидает"

    # -------------------------------------------------------------------------
    # Меню
    # -------------------------------------------------------------------------

    def create_menu(self):
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Файл", menu=file_menu)
        file_menu.add_command(label="Очистить логи",     command=self.clear_logs)
        file_menu.add_separator()
        file_menu.add_command(label="Выход",             command=self.on_closing)

        control_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Управление", menu=control_menu)
        control_menu.add_command(label="Обнаружить слейвы",      command=self.discover_slaves)
        control_menu.add_command(label="Загрузить задачи из CSV", command=self.load_tasks_csv)
        control_menu.add_command(label="Загрузить задачи из JSON",command=self.load_tasks_json)
        control_menu.add_command(label="Запустить задачи",        command=self.run_tasks)
        control_menu.add_separator()
        control_menu.add_command(label="Остановить выполнение",   command=self.stop_tasks)

        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Справка", menu=help_menu)
        help_menu.add_command(label="О программе", command=self.show_about)

    # -------------------------------------------------------------------------
    # Основные виджеты
    # -------------------------------------------------------------------------

    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Панель управления
        control_frame = ttk.LabelFrame(main_frame, text="Управление", padding="5")
        control_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Button(control_frame, text="🔍 Обнаружить слейвы",  command=self.discover_slaves).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="📂 Загрузить задачи",   command=self.load_tasks_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="▶ Запустить задачи",    command=self.run_tasks, width=15).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="⏹ Остановить",          command=self.stop_tasks, width=10).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="🗑 Очистить логи",       command=self.clear_logs).pack(side=tk.LEFT, padx=5)

        script_frame = ttk.Frame(control_frame)
        script_frame.pack(side=tk.LEFT, padx=(20, 5))
        ttk.Label(script_frame, text="Скрипт:").pack(side=tk.LEFT)
        ttk.Entry(script_frame, textvariable=self.script_path, width=30).pack(side=tk.LEFT, padx=5)
        ttk.Button(script_frame, text="Обзор", command=self.browse_script).pack(side=tk.LEFT)

        self.progress = ttk.Progressbar(control_frame, mode="indeterminate", length=150)
        self.progress.pack(side=tk.RIGHT, padx=5)

        # Вкладки
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self.slaves_frame  = ttk.Frame(self.notebook)
        self.tasks_frame   = ttk.Frame(self.notebook)
        self.log_frame     = ttk.Frame(self.notebook)
        self.results_frame = ttk.Frame(self.notebook)

        self.notebook.add(self.slaves_frame,   text="🖥️ Слейвы")
        self.notebook.add(self.tasks_frame,    text="📋 Задачи и прогресс")
        self.notebook.add(self.log_frame,      text="📄 Логи")
        self.notebook.add(self.results_frame,  text="✅ Результаты")

        self.create_slaves_tab()
        self.create_tasks_tab()
        self.create_log_tab()
        self.create_results_tab()

        self.status_bar = ttk.Label(self.root, text="Готов", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    # -------------------------------------------------------------------------
    # Вкладка "Слейвы"
    # -------------------------------------------------------------------------

    def create_slaves_tab(self):
        frame = ttk.Frame(self.slaves_frame, padding="5")
        frame.pack(fill=tk.BOTH, expand=True)

        columns = ("IP", "Статус", "Текущая задача")
        self.slaves_tree = ttk.Treeview(frame, columns=columns, show="headings", height=10)
        self.slaves_tree.heading("IP",            text="IP адрес")
        self.slaves_tree.heading("Статус",        text="Статус")
        self.slaves_tree.heading("Текущая задача",text="Текущая задача")
        self.slaves_tree.column("IP",             width=150)
        self.slaves_tree.column("Статус",         width=100)
        self.slaves_tree.column("Текущая задача", width=200)

        sb = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.slaves_tree.yview)
        self.slaves_tree.configure(yscrollcommand=sb.set)
        self.slaves_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

    def refresh_slaves_display(self):
        if not self.master:
            self.root.after(3000, self.refresh_slaves_display)
            return
        for item in self.slaves_tree.get_children():
            self.slaves_tree.delete(item)
        for ip, slave in self.master.slaves.items():
            if slave.status == SLAVE_STATUS_ALIVE: st = "🟢 Жив"
            elif slave.status == SLAVE_STATUS_BUSY: st = "🟡 Занят"
            else: st = "🔴 Мёртв"
            task_short = slave.task_id[:8] if slave.task_id else ""
            self.slaves_tree.insert("", tk.END, values=(ip, st, task_short))
        self.root.after(3000, self.refresh_slaves_display)

    # -------------------------------------------------------------------------
    # Вкладка "Задачи"
    # -------------------------------------------------------------------------

    def create_tasks_tab(self):
        """
        Объединённая вкладка: таблица задач вверху (статус, slave, прогресс %),
        прогресс-бары по каждой задаче внизу.
        Разделены через PanedWindow — можно тянуть разделитель.
        """
        outer = ttk.Frame(self.tasks_frame, padding="5")
        outer.pack(fill=tk.BOTH, expand=True)

        paned = tk.PanedWindow(outer, orient=tk.VERTICAL, sashrelief=tk.RAISED, sashwidth=5)
        paned.pack(fill=tk.BOTH, expand=True)

        # ── Верхняя панель: таблица задач ──────────────────────────────
        top_frame = ttk.Frame(paned)
        paned.add(top_frame, height=220)

        columns = ("ID", "Слейв", "Параметры", "Статус", "Прогресс", "Результат")
        self.tasks_tree = ttk.Treeview(top_frame, columns=columns, show="headings")
        widths = {"ID": 85, "Слейв": 115, "Параметры": 220, "Статус": 115, "Прогресс": 75, "Результат": 155}
        for col in columns:
            self.tasks_tree.heading(col, text=col)
            self.tasks_tree.column(col, width=widths[col])

        sb_top = ttk.Scrollbar(top_frame, orient=tk.VERTICAL, command=self.tasks_tree.yview)
        self.tasks_tree.configure(yscrollcommand=sb_top.set)
        self.tasks_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb_top.pack(side=tk.RIGHT, fill=tk.Y)

        # ── Нижняя панель: прогресс-бары ───────────────────────────────
        bot_frame = ttk.Frame(paned)
        paned.add(bot_frame)

        # Строка управления + счётчик
        ctrl = ttk.Frame(bot_frame)
        ctrl.pack(fill=tk.X, pady=(4, 0))

        manual_frame = ttk.LabelFrame(ctrl, text="Добавить задачу вручную", padding="4")
        manual_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 8))
        ttk.Label(manual_frame, text="Параметры:").pack(side=tk.LEFT)
        self.manual_params = ttk.Entry(manual_frame, width=40)
        self.manual_params.pack(side=tk.LEFT, padx=5)
        ttk.Button(manual_frame, text="➕ Добавить",           command=self.add_manual_task).pack(side=tk.LEFT, padx=3)
        ttk.Button(manual_frame, text="🗑 Очистить",           command=self.clear_all_tasks).pack(side=tk.LEFT, padx=3)

        self.tasks_count_label = ttk.Label(ctrl, text="Задач: 0", foreground="#555")
        self.tasks_count_label.pack(side=tk.RIGHT, padx=8)

        # Счётчик статусов
        self._progress_summary = ttk.Label(
            bot_frame,
            text="Задач: 0  |  Выполнено: 0  |  В процессе: 0",
            foreground="#555",
            padding=(4, 2),
        )
        self._progress_summary.pack(anchor="w")

        ttk.Separator(bot_frame, orient="horizontal").pack(fill=tk.X, pady=2)

        # Canvas со скроллом для прогресс-баров
        canvas_frame = ttk.Frame(bot_frame)
        canvas_frame.pack(fill=tk.BOTH, expand=True)

        self._progress_canvas = tk.Canvas(canvas_frame, highlightthickness=0)
        sb_bot = ttk.Scrollbar(canvas_frame, orient=tk.VERTICAL,
                               command=self._progress_canvas.yview)
        self._progress_canvas.configure(yscrollcommand=sb_bot.set)
        sb_bot.pack(side=tk.RIGHT, fill=tk.Y)
        self._progress_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self._progress_inner = ttk.Frame(self._progress_canvas)
        self._progress_canvas_window = self._progress_canvas.create_window(
            (0, 0), window=self._progress_inner, anchor="nw"
        )
        self._progress_inner.bind(
            "<Configure>",
            lambda e: self._progress_canvas.configure(
                scrollregion=self._progress_canvas.bbox("all")
            )
        )
        self._progress_canvas.bind(
            "<Configure>",
            lambda e: self._progress_canvas.itemconfig(
                self._progress_canvas_window, width=e.width
            )
        )

        # Заголовок колонок прогресс-баров
        hdr = ttk.Frame(self._progress_inner, padding=(0, 2, 0, 2))
        hdr.grid(row=0, column=0, sticky="ew")
        self._progress_inner.columnconfigure(0, weight=1)
        for col, w, txt in [(0,10,"ID"), (1,14,"Slave"), (2,32,"Параметры"), (3,0,"Прогресс"), (4,14,"")]:
            ttk.Label(hdr, text=txt, font=("TkDefaultFont", 9, "bold"),
                      foreground="#555", width=w if w else None
                      ).grid(row=0, column=col, sticky="w", padx=(8 if col==0 else 4, 0))
        ttk.Separator(self._progress_inner, orient="horizontal").grid(
            row=1, column=0, sticky="ew", padx=8
        )

        # Запустить обновление счётчика
        self._update_progress_summary()

    def update_tasks_display(self):
        """Полная перерисовка таблицы задач. Вызывается вручную при загрузке задач."""
        for item in self.tasks_tree.get_children():
            self.tasks_tree.delete(item)
        for task_id, task in self.tasks_dict.items():
            status_display = self._task_status_text(task)
            pct_text       = f"{task.percent:.0f}%" if task.percent > 0 else "—"
            result_path    = os.path.basename(task.result_path) if task.result_path else ""
            self.tasks_tree.insert("", tk.END, values=(
                task_id[:8], task.slave_ip, task.params,
                status_display, pct_text, result_path
            ))
        self.tasks_count_label.config(text=f"Задач: {len(self.tasks_dict)}")

    def _poll_tasks_display(self):
        """
        Периодически обновляет строки таблицы задач во время выполнения.
        Обновляет только изменившиеся ячейки — не перестраивает всю таблицу.
        """
        if self.tasks_dict:
            items = self.tasks_tree.get_children()
            task_list = list(self.tasks_dict.items())
            for i, (task_id, task) in enumerate(task_list):
                if i >= len(items):
                    break
                status_display = self._task_status_text(task)
                pct_text = f"{task.percent:.0f}%" if task.percent > 0 else "—"
                result_path = os.path.basename(task.result_path) if task.result_path else ""
                self.tasks_tree.item(items[i], values=(
                    task_id[:8], task.slave_ip, task.params,
                    status_display, pct_text, result_path
                ))
        self.root.after(500, self._poll_tasks_display)

    def add_manual_task(self):
        params = self.manual_params.get().strip()
        if not params:
            messagebox.showwarning("Предупреждение", "Введите параметры задачи")
            return
        self.tasks_list.append({"params": params})
        temp_id   = f"new_{len(self.tasks_list)}"
        task_info = TaskInfo(task_id=temp_id, slave_ip="", params=params)
        self.tasks_dict[temp_id] = task_info
        self.manual_params.delete(0, tk.END)
        self.update_tasks_display()

    def clear_all_tasks(self):
        self.tasks_list.clear()
        self.tasks_dict.clear()
        self._progress_widgets.clear()
        self.update_tasks_display()
        self._rebuild_progress_tab()


    def _rebuild_progress_tab(self):
        """Пересоздать виджеты прогресса под текущий tasks_dict."""
        # Удалить старые виджеты (кроме строки 0 — заголовок, строки 1 — сепаратор)
        for widget in self._progress_widgets.values():
            widget.frame.destroy()
        self._progress_widgets.clear()

        for i, (task_id, task) in enumerate(self.tasks_dict.items()):
            w = TaskProgressWidget(self._progress_inner, task, row=i + 2)
            self._progress_widgets[task_id] = w

    def _update_progress_summary(self):
        """Периодически обновляет строку счётчика внизу вкладки Прогресс."""
        total    = len(self.tasks_dict)
        done     = sum(1 for t in self.tasks_dict.values() if t.status == TASK_STATUS_DONE)
        active   = sum(1 for t in self.tasks_dict.values() if t.status == TASK_STATUS_SENT)
        lost     = sum(1 for t in self.tasks_dict.values() if t.status == TASK_STATUS_LOST)
        pending  = total - done - active - lost

        self._progress_summary.config(
            text=(
                f"Задач: {total}  |  ✅ Выполнено: {done}  |"
                f"  📤 В процессе: {active}  |  ⏳ Ожидание: {pending}  |  ❌ Ошибки: {lost}"
            )
        )
        self.root.after(1000, self._update_progress_summary)

    # -------------------------------------------------------------------------
    # Вкладка "Логи"
    # -------------------------------------------------------------------------

    def create_log_tab(self):
        self.log_text = scrolledtext.ScrolledText(
            self.log_frame, wrap=tk.WORD, font=("Consolas", 9)
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.log_text.config(state=tk.DISABLED)

    def clear_logs(self):
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)

    # -------------------------------------------------------------------------
    # Вкладка "Результаты"
    # -------------------------------------------------------------------------

    def create_results_tab(self):
        frame = ttk.Frame(self.results_frame, padding="5")
        frame.pack(fill=tk.BOTH, expand=True)

        self.results_text = scrolledtext.ScrolledText(
            frame, wrap=tk.WORD, font=("Consolas", 10)
        )
        self.results_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.results_text.config(state=tk.DISABLED)
        ttk.Button(frame, text="Обновить отчёт", command=self.generate_report).pack(pady=5)

    def generate_report(self):
        self.results_text.config(state=tk.NORMAL)
        self.results_text.delete(1.0, tk.END)

        if not self.tasks_dict:
            self.results_text.insert(tk.END, "Нет задач для отображения.\n")
            self.results_text.config(state=tk.DISABLED)
            return

        done = [t for t in self.tasks_dict.values() if t.status == TASK_STATUS_DONE]
        lost = [t for t in self.tasks_dict.values() if t.status == TASK_STATUS_LOST]

        report = (
            f"{'='*60}\nИТОГОВЫЙ ОТЧЁТ\n{'='*60}\n"
            f"Всего задач: {len(self.tasks_dict)}\n"
            f"✅ Выполнено: {len(done)}\n"
            f"❌ Потеряно:  {len(lost)}\n"
            f"{'='*60}\n\nВыполненные задачи:\n"
        )
        for t in done:
            report += f"  [{t.task_id[:8]}] slave={t.slave_ip}  результат={t.result_path}\n"
        if lost:
            report += "\nПотерянные задачи:\n"
            for t in lost:
                report += f"  [{t.task_id[:8]}] slave={t.slave_ip}  причина={t.error}\n"

        self.results_text.insert(tk.END, report)
        self.results_text.config(state=tk.DISABLED)

    # -------------------------------------------------------------------------
    # Логика
    # -------------------------------------------------------------------------

    def browse_script(self):
        filename = filedialog.askopenfilename(
            title="Выберите скрипт",
            filetypes=[("Python files", "*.py"), ("All files", "*.*")]
        )
        if filename:
            self.script_path.set(filename)

    def discover_slaves(self):
        def worker():
            self.status_bar.config(text="Обнаружение слейвов...")
            try:
                slaves = self.master.discover()
                self.logger.info("Обнаружено слейвов: %d", len(slaves))
                self.status_bar.config(text=f"Обнаружено {len(slaves)} слейвов")
                self.root.after(0, self.refresh_slaves_display)
            except Exception as e:
                self.logger.error("Ошибка discovery: %s", e)
                self.status_bar.config(text="Ошибка discovery")
        threading.Thread(target=worker, daemon=True).start()

    def load_tasks_dialog(self):
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
            with open(filename, "r", encoding="utf-8") as f:
                for row in csv.reader(f):
                    if row:
                        self.tasks_list.append({"params": row[0].strip()})
            for i, t in enumerate(self.tasks_list):
                tid = f"csv_{i+1}"
                task_info = TaskInfo(task_id=tid, slave_ip="", params=t["params"])
                self.tasks_dict[tid] = task_info
            self.update_tasks_display()
            self._rebuild_progress_tab()
            self.status_bar.config(text=f"Загружено {len(self.tasks_list)} задач")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить CSV:\n{e}")

    def load_tasks_json(self):
        filename = filedialog.askopenfilename(filetypes=[("JSON files", "*.json")])
        if not filename:
            return
        try:
            import json
            with open(filename, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.tasks_list = data if isinstance(data, list) else data.get("tasks", [])
            self.tasks_dict.clear()
            for i, t in enumerate(self.tasks_list):
                tid = f"json_{i+1}"
                task_info = TaskInfo(task_id=tid, slave_ip="", params=t["params"])
                self.tasks_dict[tid] = task_info
            self.update_tasks_display()
            self._rebuild_progress_tab()
            self.status_bar.config(text=f"Загружено {len(self.tasks_list)} задач")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить JSON:\n{e}")

    def run_tasks(self):
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
        self.progress.start(10)
        self.status_bar.config(text="Выполнение задач...")
        self.notebook.select(self.tasks_frame)

        def worker():
            try:
                # Сбросить старые данные
                self.master.tasks.clear()
                self.master.slaves_dict = {}

                # Зарегистрировать задачи в master ДО запуска, чтобы сразу
                # получить реальные UUID и создать виджеты прогресса
                import uuid as _uuid
                from config import TASK_STATUS_PENDING
                self.tasks_dict.clear()
                self._progress_widgets.clear()

                for t in self.tasks_list:
                    task_id = str(_uuid.uuid4())
                    task = TaskInfo(task_id=task_id, slave_ip="", params=t["params"])
                    self.master.tasks[task_id] = task
                    self.tasks_dict[task_id]   = task
                    self.master.task_queue.put(task)

                # Перестроить таблицу задач и виджеты прогресса с реальными UUID
                self.root.after(0, self.update_tasks_display)
                self.root.after(0, self._rebuild_progress_tab)
                # Дать главному потоку время отрисовать виджеты
                time.sleep(0.2)

                # Запустить только диспетчеризацию (задачи уже в очереди)
                results = self.master._run_dispatch(
                    script_path=self.script_path.get(),
                )
                self.tasks_dict = results

                self.root.after(0, self._on_run_finished)
            except Exception as e:
                self.logger.error("Ошибка при выполнении: %s", e)
                self.root.after(0, lambda: self.status_bar.config(text="Ошибка выполнения"))
            finally:
                self.running = False
                self.root.after(0, self.progress.stop)

        threading.Thread(target=worker, daemon=True).start()

    def _on_run_finished(self):
        """Вызывается в главном потоке после завершения master.run."""
        self.update_tasks_display()
        self._rebuild_progress_tab()
        # Финальное обновление всех виджетов прогресса
        for task_id, task in self.tasks_dict.items():
            widget = self._progress_widgets.get(task_id)
            if widget:
                widget.update(task)
        self.generate_report()
        self.status_bar.config(text="Выполнение завершено")
        self.logger.info("Все задачи завершены")

    def stop_tasks(self):
        if not self.running:
            return
        self.logger.warning("Запрошена остановка выполнения")
        self.status_bar.config(text="Остановка...")

    def show_about(self):
        messagebox.showinfo(
            "О программе",
            "Master Agent v2.1\n"
            "Новое: прогресс-бары в реальном времени,\n"
            "retry с экспоненциальной задержкой,\n"
            "checkpoint для возобновления после падения.\n\n"
            "Python + Tkinter"
        )

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
                self.root.destroy()
        else:
            self.root.destroy()

    def run(self):
        self.root.mainloop()


# =============================================================================
# Точка входа
# =============================================================================

if __name__ == "__main__":
    os.makedirs(MASTER_RESULTS_DIR, exist_ok=True)
    app = MasterGUI()
    app.run()