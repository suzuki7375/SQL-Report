# -*- coding: utf-8 -*-

import argparse
import calendar
import datetime
import os
import runpy
import subprocess
import sys
import tkinter as tk
from tkinter import ttk


SCRIPT_NAME = "800G_TRX_TEST.py"
FIXED_BER_SCRIPT_NAME = "800G_Fixed_BER_Test.py"
BER_SYMBOL_ERROR_SCRIPT_NAME = "BER_Symbol_Error_Test.py"
COMBINED_REPORT_SCRIPT_NAME = "Combined Test Report.py"
BUTTON_LABEL = os.path.splitext(SCRIPT_NAME)[0]
FIXED_BER_BUTTON_LABEL = os.path.splitext(FIXED_BER_SCRIPT_NAME)[0]
BER_SYMBOL_ERROR_BUTTON_LABEL = os.path.splitext(BER_SYMBOL_ERROR_SCRIPT_NAME)[0]
COMBINED_REPORT_BUTTON_LABEL = os.path.splitext(COMBINED_REPORT_SCRIPT_NAME)[0]
BUTTON_COUNT = 6


def is_frozen() -> bool:
    return getattr(sys, "frozen", False)


def get_base_dir() -> str:
    if is_frozen():
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def resolve_script_path(script_name: str) -> str:
    if is_frozen() and hasattr(sys, "_MEIPASS"):
        candidate = os.path.join(sys._MEIPASS, script_name)
        if os.path.exists(candidate):
            return candidate
    return os.path.join(get_base_dir(), script_name)


def run_script_from_cli(script_name: str, forwarded_args: list[str]) -> None:
    script_path = resolve_script_path(script_name)
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"找不到腳本檔案: {script_name}")
    sys.argv = [script_path, *forwarded_args]
    runpy.run_path(script_path, run_name="__main__")


def parse_launcher_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--run-script")
    return parser.parse_known_args()


class DatePicker(ttk.Frame):
    def __init__(self, parent: ttk.Frame, value: datetime.date) -> None:
        super().__init__(parent)
        self._value = tk.StringVar(value=value.isoformat())
        self._entry = ttk.Entry(self, textvariable=self._value, width=12, state="readonly")
        self._entry.pack(side="left", fill="x", expand=True)
        self._entry.bind("<Button-1>", self._open_picker)
        self._picker_window: tk.Toplevel | None = None

    @property
    def value(self) -> str:
        return self._value.get()

    def _open_picker(self, _event: tk.Event) -> None:
        if self._picker_window and self._picker_window.winfo_exists():
            self._picker_window.lift()
            return

        today = datetime.date.fromisoformat(self._value.get())
        self._picker_window = tk.Toplevel(self)
        self._picker_window.title("選擇日期")
        self._picker_window.transient(self)
        self._picker_window.resizable(False, False)

        header = ttk.Frame(self._picker_window, padding=(12, 12, 12, 0))
        header.pack(fill="x")

        month_var = tk.IntVar(value=today.month)
        year_var = tk.IntVar(value=today.year)

        def update_calendar() -> None:
            for child in days_frame.winfo_children():
                child.destroy()

            month = month_var.get()
            year = year_var.get()
            month_label.config(text=f"{year} / {month:02d}")
            month_matrix = calendar.monthcalendar(year, month)

            for row_index, week in enumerate(month_matrix):
                for col_index, day in enumerate(week):
                    if day == 0:
                        ttk.Label(days_frame, text=" ").grid(row=row_index, column=col_index, padx=2, pady=2)
                        continue

                    def on_select(selected_day: int = day) -> None:
                        selected_date = datetime.date(year, month, selected_day)
                        self._value.set(selected_date.isoformat())
                        if self._picker_window:
                            self._picker_window.destroy()

                    ttk.Button(
                        days_frame,
                        text=f"{day:02d}",
                        style="Date.TButton",
                        command=on_select,
                    ).grid(row=row_index, column=col_index, padx=2, pady=2, sticky="nsew")

        def go_prev_month() -> None:
            month = month_var.get() - 1
            year = year_var.get()
            if month == 0:
                month = 12
                year -= 1
            month_var.set(month)
            year_var.set(year)
            update_calendar()

        def go_next_month() -> None:
            month = month_var.get() + 1
            year = year_var.get()
            if month == 13:
                month = 1
                year += 1
            month_var.set(month)
            year_var.set(year)
            update_calendar()

        ttk.Button(header, text="◀", width=3, command=go_prev_month).pack(side="left")
        month_label = ttk.Label(header, text="")
        month_label.pack(side="left", expand=True)
        ttk.Button(header, text="▶", width=3, command=go_next_month).pack(side="right")

        weekday_frame = ttk.Frame(self._picker_window, padding=(12, 8, 12, 0))
        weekday_frame.pack(fill="x")
        for index, day_name in enumerate(["日", "一", "二", "三", "四", "五", "六"]):
            ttk.Label(weekday_frame, text=day_name).grid(row=0, column=index, padx=2, pady=2)

        days_frame = ttk.Frame(self._picker_window, padding=(12, 0, 12, 12))
        days_frame.pack()
        for index in range(7):
            days_frame.columnconfigure(index, weight=1)

        update_calendar()

        self._picker_window.grab_set()


def build_ui() -> tk.Tk:
    root = tk.Tk()
    root.title("800G 2FR4 SQL DATA")
    root.geometry("700x420")
    root.configure(bg="#f7f7fb")

    style = ttk.Style(root)
    style.theme_use("clam")
    style.configure("Main.TFrame", background="#f7f7fb")
    style.configure("Card.TFrame", background="#ffffff", relief="ridge", borderwidth=1)
    style.configure(
        "Primary.TButton",
        font=("Segoe UI", 9, "bold"),
        padding=(12, 10),
        relief="raised",
        borderwidth=2,
        background="#f0f2f6",
        foreground="#1f2933",
        bordercolor="#9ca3af",
        lightcolor="#ffffff",
        darkcolor="#a8b0bf",
    )
    style.configure(
        "Secondary.TButton",
        font=("Segoe UI", 9),
        padding=(12, 10),
        relief="raised",
        borderwidth=2,
        background="#f8f9fb",
        foreground="#4b5563",
        bordercolor="#b8c1d1",
        lightcolor="#ffffff",
        darkcolor="#b0b7c4",
    )
    style.map(
        "Primary.TButton",
        background=[("pressed", "#d9dee8"), ("active", "#f7f9fc")],
        relief=[("pressed", "sunken"), ("!pressed", "raised")],
    )
    style.map(
        "Secondary.TButton",
        background=[("pressed", "#e2e7f1"), ("active", "#f3f5f9")],
        relief=[("pressed", "sunken"), ("!pressed", "raised")],
    )
    style.configure("Date.TButton", padding=4)
    style.configure("Title.TLabel", font=("Segoe UI", 12, "bold"), background="#f7f7fb")
    style.configure("Sub.TLabel", font=("Segoe UI", 9), foreground="#555555", background="#f7f7fb")
    style.configure("Status.TLabel", font=("Segoe UI", 9, "italic"), background="#f7f7fb")
    style.configure("Panel.TFrame", background="#ffffff", relief="groove", borderwidth=1)

    main_frame = ttk.Frame(root, padding=24, style="Main.TFrame")
    main_frame.pack(fill="both", expand=True)

    header_frame = ttk.Frame(main_frame, style="Main.TFrame")
    header_frame.pack(fill="x")

    ttk.Label(header_frame, text="800G 2FR4 資料查詢", style="Title.TLabel").pack(anchor="w")
    ttk.Label(header_frame, text="請選擇日期區間後開始查詢", style="Sub.TLabel").pack(anchor="w", pady=(4, 16))

    content_frame = ttk.Frame(main_frame, style="Panel.TFrame", padding=16)
    content_frame.pack(fill="both", expand=True)

    date_frame = ttk.Frame(content_frame, style="Card.TFrame", padding=(12, 10))
    date_frame.pack(fill="x", pady=(0, 12))

    ttk.Label(date_frame, text="日期", style="Title.TLabel").pack(side="left")

    today = datetime.date.today()
    start_picker = DatePicker(date_frame, today)
    start_picker.pack(side="left", padx=(12, 8))

    ttk.Label(date_frame, text="~", style="Title.TLabel").pack(side="left")
    end_picker = DatePicker(date_frame, today)
    end_picker.pack(side="left", padx=(8, 0))

    status_frame = ttk.Frame(content_frame, style="Card.TFrame", padding=(12, 10))
    status_frame.pack(fill="x")

    status_var = tk.StringVar(value="待機中")
    status_label = ttk.Label(status_frame, textvariable=status_var, style="Status.TLabel")
    status_label.pack(side="left")

    progress = ttk.Progressbar(status_frame, mode="indeterminate")
    progress.pack(side="right", fill="x", expand=True, padx=(16, 0))

    buttons_frame = ttk.Frame(content_frame, style="Card.TFrame", padding=(10, 6))
    buttons_frame.pack(fill="both", expand=True, pady=(16, 0))

    buttons_frame.columnconfigure((0, 1, 2), weight=1, uniform="button")
    buttons_frame.rowconfigure((0, 1), weight=1)

    button_refs: list[ttk.Button] = []
    running_process: subprocess.Popen | None = None

    def set_loading(is_loading: bool) -> None:
        state = "disabled" if is_loading else "normal"
        for btn in button_refs:
            btn.configure(state=state)
        if is_loading:
            status_var.set("查詢中，請稍候…")
            progress.start(10)
        else:
            status_var.set("待機中")
            progress.stop()

    def check_process() -> None:
        nonlocal running_process
        if running_process is None:
            return
        if running_process.poll() is None:
            root.after(500, check_process)
            return
        running_process = None
        set_loading(False)

    def run_report(script_name: str) -> None:
        nonlocal running_process
        if running_process is not None:
            return
        base_dir = get_base_dir()
        args = ["--start-date", start_picker.value, "--end-date", end_picker.value]
        if is_frozen():
            running_process = subprocess.Popen(
                [sys.executable, "--run-script", script_name, *args],
                cwd=base_dir,
            )
        else:
            script_path = os.path.join(base_dir, script_name)
            running_process = subprocess.Popen(
                [sys.executable, script_path, *args],
                cwd=base_dir,
            )
        set_loading(True)
        check_process()

    def run_trx_test() -> None:
        run_report(SCRIPT_NAME)

    def run_fixed_ber_test() -> None:
        run_report(FIXED_BER_SCRIPT_NAME)

    def run_ber_symbol_error_test() -> None:
        run_report(BER_SYMBOL_ERROR_SCRIPT_NAME)

    def run_combined_report() -> None:
        run_report(COMBINED_REPORT_SCRIPT_NAME)

    main_button = ttk.Button(
        buttons_frame,
        text=BUTTON_LABEL,
        command=run_trx_test,
        style="Primary.TButton",
    )
    main_button.grid(row=0, column=0, padx=12, pady=12, sticky="nsew")
    button_refs.append(main_button)

    fixed_ber_button = ttk.Button(
        buttons_frame,
        text=FIXED_BER_BUTTON_LABEL,
        command=run_fixed_ber_test,
        style="Primary.TButton",
    )
    fixed_ber_button.grid(row=0, column=1, padx=12, pady=12, sticky="nsew")
    button_refs.append(fixed_ber_button)

    symbol_error_button = ttk.Button(
        buttons_frame,
        text=BER_SYMBOL_ERROR_BUTTON_LABEL,
        command=run_ber_symbol_error_test,
        style="Primary.TButton",
    )
    symbol_error_button.grid(row=0, column=2, padx=12, pady=12, sticky="nsew")
    button_refs.append(symbol_error_button)

    combined_report_button = ttk.Button(
        buttons_frame,
        text=COMBINED_REPORT_BUTTON_LABEL,
        command=run_combined_report,
        style="Primary.TButton",
    )
    combined_report_button.grid(row=1, column=0, padx=12, pady=12, sticky="nsew")
    button_refs.append(combined_report_button)

    for index in range(4, BUTTON_COUNT):
        button = ttk.Button(buttons_frame, text="待新增", style="Secondary.TButton")
        row = index // 3
        col = index % 3
        button.grid(row=row, column=col, padx=12, pady=12, sticky="nsew")
        button_refs.append(button)

    return root


if __name__ == "__main__":
    launch_args, remaining_args = parse_launcher_args()
    if launch_args.run_script:
        run_script_from_cli(launch_args.run_script, remaining_args)
    else:
        app = build_ui()
        app.mainloop()
