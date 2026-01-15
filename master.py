# -*- coding: utf-8 -*-

import argparse
import datetime
import os
import re
from typing import Final
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine


SERVER: Final = "omddb"
USERNAME: Final = "PE_ReadOnlyUser"
PASSWORD: Final = "pe@0505"
DRIVER: Final = "ODBC Driver 17 for SQL Server"
DATABASE: Final = "MEQueryManufacturingDatabase"

SQL_FILE: Final = "MASTER.sql"
OUTPUT_EXTENSION: Final = ".xlsx"
TESTNUMBER_HEADER: Final = "TESTNUMBER"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--output-dir",
        default=".",
        help="輸出資料夾（預設為目前資料夾）",
    )
    return parser.parse_args()


def parse_date(value: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("日期格式需為 YYYY-MM-DD") from exc


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
    odbc_params = quote_plus(conn_str_with_db(DATABASE))
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_params}")
    with engine.connect() as conn:
        return pd.read_sql_query(sql_text, conn)


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


def extract_testnumber_date(series: pd.Series) -> pd.Series:
    extracted = series.astype(str).str.extract(r"[A-Za-z](\d{8})", expand=False)
    return pd.to_datetime(extracted, format="%Y%m%d", errors="coerce").dt.date


def filter_by_date_range(df: pd.DataFrame, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    if TESTNUMBER_HEADER not in df.columns:
        raise ValueError(f"找不到欄位：{TESTNUMBER_HEADER}")
    test_dates = extract_testnumber_date(df[TESTNUMBER_HEADER])
    mask = (test_dates >= start_date) & (test_dates <= end_date)
    return df.loc[mask].copy()


def main() -> None:
    args = parse_args()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), SQL_FILE)
    sql_text = load_sql(sql_path)
    log_connection_info()
    log_report_location(sql_text)
    df = fetch_master_data(sql_text)
    log_report_header(df)
    df = filter_by_date_range(df, start_date, end_date)
    output_path = build_output_path(args.output_dir)
    df.to_excel(output_path, index=False, sheet_name="Master")
    print(f"✅ 匯出完成：{output_path}")


if __name__ == "__main__":
    main()
