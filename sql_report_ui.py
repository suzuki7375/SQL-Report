# -*- coding: utf-8 -*-

import datetime
import os
import subprocess
import sys
import tkinter as tk
from tkinter import ttk


SCRIPT_NAME = "800G_TRX_TEST.py"


def run_trx_test() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(base_dir, SCRIPT_NAME)
    subprocess.Popen([sys.executable, script_path], cwd=base_dir)


def build_ui() -> tk.Tk:
    root = tk.Tk()
    root.title("800G 2FR4 SQL DATA")
    root.geometry("520x260")

    main_frame = ttk.Frame(root, padding=16)
    main_frame.pack(fill="both", expand=True)

    date_frame = ttk.Frame(main_frame)
    date_frame.pack(fill="x")

    ttk.Label(date_frame, text="日期").pack(side="left")

    date_var = tk.StringVar(value=datetime.date.today().isoformat())
    date_entry = ttk.Entry(date_frame, textvariable=date_var, width=16)
    date_entry.pack(side="left", padx=(8, 0))

    ttk.Label(date_frame, text="(YYYY-MM-DD)").pack(side="left", padx=(8, 0))

    buttons_frame = ttk.Frame(main_frame)
    buttons_frame.pack(fill="both", expand=True, pady=20)

    buttons_frame.columnconfigure((0, 1, 2), weight=1)
    buttons_frame.rowconfigure((0, 1), weight=1)

    main_button = ttk.Button(buttons_frame, text=SCRIPT_NAME, command=run_trx_test)
    main_button.grid(row=0, column=0, padx=8, pady=8, sticky="nsew")

    for index in range(1, 6):
        button = ttk.Button(buttons_frame, text=" ")
        row = index // 3
        col = index % 3
        button.grid(row=row, column=col, padx=8, pady=8, sticky="nsew")

    return root


if __name__ == "__main__":
    app = build_ui()
    app.mainloop()
