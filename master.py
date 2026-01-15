# -*- coding: utf-8 -*-

import argparse
import datetime
import os
import re
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
    parser.add_argument("--start-date", help="YYYY-MM-DD")
    parser.add_argument("--end-date", help="YYYY-MM-DD")
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


def parse_date(value: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("日期格式需為 YYYY-MM-DD") from exc


def validate_date_args(start_date: str | None, end_date: str | None) -> tuple[str | None, str | None]:
    if start_date is None and end_date is None:
        return None, None
    if not start_date or not end_date:
        raise ValueError("需同時提供 --start-date 與 --end-date。")
    parsed_start = parse_date(start_date).isoformat()
    parsed_end = parse_date(end_date).isoformat()
    return parsed_start, parsed_end


def load_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as file:
        lines = file.read().splitlines()
    cleaned_lines = [line for line in lines if line.strip().upper() != "GO"]
    return "\n".join(cleaned_lines).strip()


def build_output_path(base_dir: str) -> str:
    base_name = os.path.splitext(os.path.basename(__file__))[0]
    filename = f"{base_name}{OUTPUT_EXTENSION}"
    return os.path.join(base_dir, filename)


def build_date_filtered_sql(sql_text: str, start_date: str | None, end_date: str | None) -> tuple[str, list[str]]:
    if start_date and end_date:
        date_clause = "TRY_CONVERT(date, SUBSTRING(TESTNUMBER, 2, 8)) BETWEEN ? AND ?"
        if re.search(r"\bWHERE\b", sql_text, re.IGNORECASE):
            sql_text = f"{sql_text}\nAND {date_clause}"
        else:
            sql_text = f"{sql_text}\nWHERE {date_clause}"
        return sql_text, [start_date, end_date]
    return sql_text, []


def fetch_master_data(sql_text: str, params: list[str] | None = None) -> pd.DataFrame:
    with pyodbc.connect(conn_str_with_db(DATABASE), timeout=30) as conn:
        return pd.read_sql_query(sql_text, conn, params=params or [])


def log_connection_info() -> None:
    print("1. 登入確認資訊是否正確")
    print(f"   SERVER={SERVER}")
    print(f"   DATABASE={DATABASE}")
    print(f"   USERNAME={USERNAME}")
    print(f"   DRIVER={DRIVER}")


def extract_report_location(sql_text: str) -> str:
    match = re.search(r"FROM\s+([^\s;]+)", sql_text, re.IGNORECASE)
    return match.group(1) if match else "未知"


def log_report_location(sql_text: str) -> None:
    location = extract_report_location(sql_text)
    print("2. 報表位置")
    print(f"   {location}")


def log_report_header(df: pd.DataFrame) -> None:
    print("3. 是否看到報表header")
    if df.columns.empty:
        raise ValueError("未取得報表header，請確認 SQL 查詢結果。")
    headers = ", ".join(str(column) for column in df.columns)
    print(f"   報表欄位：{headers}")


def fetch_master_dataframe(start_date: str | None, end_date: str | None) -> pd.DataFrame:
    sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), SQL_FILE)
    sql_text = load_sql(sql_path)
    filtered_sql, params = build_date_filtered_sql(sql_text, start_date, end_date)
    return fetch_master_data(filtered_sql, params)


def main() -> None:
    args = parse_args()
    start_date, end_date = validate_date_args(args.start_date, args.end_date)
    sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), SQL_FILE)
    sql_text = load_sql(sql_path)
    filtered_sql, params = build_date_filtered_sql(sql_text, start_date, end_date)
    log_connection_info()
    log_report_location(sql_text)
    df = fetch_master_data(filtered_sql, params)
    log_report_header(df)
    output_path = build_output_path(args.output_dir)
    df.to_excel(output_path, index=False, sheet_name="master")
    print(f"✅ 匯出完成：{output_path}")


if __name__ == "__main__":
    main()
