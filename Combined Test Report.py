# -*- coding: utf-8 -*-

import argparse
import datetime
import importlib.util
import os
import sys
import time

import pandas as pd
import pyodbc
from openpyxl import Workbook, load_workbook

OUTPUT_EXTENSION = ".xlsx"
FUNCTION_TEMPLATE = "Function.xlsx"
DATA_ANALYSIS_SHEET = "Data Analysis"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    return parser.parse_args()


def parse_date(value: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("日期格式需為 YYYY-MM-DD") from exc


def format_date_range(start_date: str, end_date: str) -> str:
    if start_date == end_date:
        return start_date
    return f"{start_date}_{end_date}"


def build_output_path(base_dir: str, start_date: str, end_date: str) -> str:
    base_name = os.path.splitext(os.path.basename(__file__))[0]
    date_range = format_date_range(start_date, end_date)
    filename = f"{base_name}_{date_range}{OUTPUT_EXTENSION}"
    return os.path.join(base_dir, filename)


def load_module(module_path: str, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"無法載入模組：{module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_template_workbook(base_dir: str) -> Workbook:
    template_path = os.path.join(base_dir, FUNCTION_TEMPLATE)
    if os.path.exists(template_path):
        return load_workbook(template_path)
    workbook = Workbook()
    if workbook.active:
        workbook.remove(workbook.active)
    workbook.create_sheet(DATA_ANALYSIS_SHEET)
    return workbook


def ensure_data_analysis_template(workbook: Workbook) -> object:
    if DATA_ANALYSIS_SHEET in workbook.sheetnames:
        return workbook[DATA_ANALYSIS_SHEET]
    return workbook.create_sheet(DATA_ANALYSIS_SHEET)


def create_data_analysis_sheet(workbook: Workbook, template_ws, sheet_name: str) -> None:
    ws = workbook.copy_worksheet(template_ws)
    ws.title = sheet_name


def export_report_dataframe(module, start_date: str, end_date: str) -> pd.DataFrame:
    with pyodbc.connect(module.conn_str_with_db(module.DATABASE), timeout=30) as conn:
        if hasattr(module, "build_columns_query"):
            use_openquery = False
            cur = conn.cursor()
            try:
                cur.execute(module.build_columns_query(False))
            except pyodbc.Error:
                use_openquery = True
                cur.execute(module.build_columns_query(True))

            if use_openquery and hasattr(module, "build_sorted_query_openquery"):
                export_sql = module.build_sorted_query_openquery(module.EXPORT_N, start_date, end_date)
                df = pd.read_sql_query(export_sql, conn)
            else:
                export_sql = module.build_sorted_query(module.EXPORT_N)
                df = pd.read_sql_query(export_sql, conn, params=[start_date, end_date])
        else:
            export_sql = module.build_sorted_query(module.EXPORT_N)
            df = pd.read_sql_query(export_sql, conn, params=[start_date, end_date])

    return module.apply_header(df)


def build_report(
    module,
    sheet_prefix: str,
    categories: list[str],
    category_builder,
    workbook: Workbook,
    template_ws,
    start_date: str,
    end_date: str,
    populate_data_analysis,
) -> None:
    print(f"\n🚀 開始整合：{sheet_prefix}")
    t0 = time.time()

    df = export_report_dataframe(module, start_date, end_date)
    print(f"✅ export rows={len(df)} time={time.time()-t0:.1f}s")

    if module.CH_NUMBER_HEADER not in df.columns:
        raise KeyError(f"查無欄位 {module.CH_NUMBER_HEADER}")

    df["_category"] = category_builder(df, module)
    df = module.apply_error_codes(df)
    metrics = module.build_data_analysis_metrics(df)
    failed_devices = module.build_failed_devices(df)
    failed_components = module.build_failed_component_records(df)

    for category in categories:
        sheet_df = df[df["_category"] == category].drop(columns=["_category"])
        if sheet_df.empty:
            sheet_df = df.head(0).drop(columns=["_category"])
        module.write_dataframe_to_sheet(workbook, f"{sheet_prefix} {category}", sheet_df)

    module.write_dataframe_to_sheet(workbook, f"{sheet_prefix} Failed Device", failed_devices)

    data_analysis_sheet_name = f"{sheet_prefix} {module.DATA_ANALYSIS_SHEET}"
    create_data_analysis_sheet(workbook, template_ws, data_analysis_sheet_name)
    original_sheet_name = module.DATA_ANALYSIS_SHEET
    module.DATA_ANALYSIS_SHEET = data_analysis_sheet_name
    populate_data_analysis(module, workbook, metrics, failed_devices, failed_components)
    module.DATA_ANALYSIS_SHEET = original_sheet_name


def populate_trx_data_analysis(
    module,
    workbook: Workbook,
    metrics: dict[str, dict[str, float]],
    _failed_devices: pd.DataFrame,
    failed_components: pd.DataFrame,
) -> None:
    module.populate_data_analysis_sheet(workbook, metrics, failed_components)


def populate_standard_data_analysis(
    module,
    workbook: Workbook,
    metrics: dict[str, dict[str, float]],
    failed_devices: pd.DataFrame,
    failed_components: pd.DataFrame,
) -> None:
    module.populate_data_analysis_sheet(workbook, metrics, failed_devices, failed_components)


def main() -> None:
    args = parse_args()
    start_date = parse_date(args.start_date).isoformat()
    end_date = parse_date(args.end_date).isoformat()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_path = build_output_path(base_dir, args.start_date, args.end_date)

    reports = [
        {
            "sheet_prefix": "800G_TRX",
            "module_file": "800G_TRX_TEST.py",
            "module_name": "report_trx_test",
            "categories": ["ATS", "DDMI", "LT", "HT", "RT", "其他"],
            "category_builder": lambda df, module: module.classify_ch_number_series(df[module.CH_NUMBER_HEADER]),
            "populate_data_analysis": populate_trx_data_analysis,
        },
        {
            "sheet_prefix": "800G_Fixed_BER",
            "module_file": "800G_Fixed_BER_Test.py",
            "module_name": "report_fixed_ber_test",
            "categories": ["3T_BER", "其他"],
            "category_builder": lambda df, module: df[module.CH_NUMBER_HEADER].apply(module.classify_ch_number),
            "populate_data_analysis": populate_standard_data_analysis,
        },
        {
            "sheet_prefix": "BER_Symbol_Error",
            "module_file": "BER_Symbol_Error_Test.py",
            "module_name": "report_symbol_error_test",
            "categories": ["TC_BER", "其他"],
            "category_builder": lambda df, module: df[module.CH_NUMBER_HEADER].apply(module.classify_ch_number),
            "populate_data_analysis": populate_standard_data_analysis,
        },
    ]

    workbook = load_template_workbook(base_dir)
    template_ws = ensure_data_analysis_template(workbook)

    try:
        for report in reports:
            module_path = os.path.join(base_dir, report["module_file"])
            module = load_module(module_path, report["module_name"])
            build_report(
                module,
                report["sheet_prefix"],
                report["categories"],
                report["category_builder"],
                workbook,
                template_ws,
                start_date,
                end_date,
                report["populate_data_analysis"],
            )

        if DATA_ANALYSIS_SHEET in workbook.sheetnames:
            workbook.remove(workbook[DATA_ANALYSIS_SHEET])

        workbook.save(out_path)
        print("📁 Combined Excel 已輸出：", out_path)

    except Exception as exc:
        print("❌ 查詢或匯出失敗：")
        print(exc)
        sys.exit(2)


if __name__ == "__main__":
    main()
