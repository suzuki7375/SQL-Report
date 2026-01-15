# -*- coding: utf-8 -*-

import argparse
import os
from typing import Final

import pandas as pd
import pyodbc


SERVER: Final = "omddb"
USERNAME: Final = "PE_ReadOnlyUser"
PASSWORD: Final = "pe@0505"
DRIVER: Final = "ODBC Driver 17 for SQL Server"
DATABASE: Final = "MEQueryManufacturingDatabase"

SQL_FILE: Final = "MASTER.sql"
OUTPUT_EXTENSION: Final = ".xlsx"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default=".",
        help="輸出資料夾（預設為目前資料夾）",
    )
    return parser.parse_args()


def conn_str_with_db(db: str) -> str:
    return (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={db};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "TrustServerCertificate=yes;"
    )


def load_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as file:
        lines = file.read().splitlines()
    cleaned_lines = [line for line in lines if line.strip().upper() != "GO"]
    return "\n".join(cleaned_lines).strip()


def build_output_path(base_dir: str) -> str:
    base_name = os.path.splitext(os.path.basename(__file__))[0]
    filename = f"{base_name}{OUTPUT_EXTENSION}"
    return os.path.join(base_dir, filename)


def fetch_master_data(sql_text: str) -> pd.DataFrame:
    with pyodbc.connect(conn_str_with_db(DATABASE), timeout=10) as conn:
        return pd.read_sql_query(sql_text, conn)


def main() -> None:
    args = parse_args()
    sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), SQL_FILE)
    sql_text = load_sql(sql_path)
    df = fetch_master_data(sql_text)
    output_path = build_output_path(args.output_dir)
    df.to_excel(output_path, index=False)
    print(f"✅ 匯出完成：{output_path}")


if __name__ == "__main__":
    main()
