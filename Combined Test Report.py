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
from openpyxl.cell.cell import MergedCell

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


def set_cell_value(ws, row: int, column: int, value: object) -> None:
    cell = ws.cell(row=row, column=column)
    if not isinstance(cell, MergedCell):
        cell.value = value
        return
    for merged_range in ws.merged_cells.ranges:
        if cell.coordinate in merged_range:
            if row == merged_range.min_row and column == merged_range.min_col:
                ws.cell(row=merged_range.min_row, column=merged_range.min_col, value=value)
            return


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


def resolve_equipment_warning(sheet_prefix: str) -> str:
    if sheet_prefix == "800G_TRX":
        return "⚠️ TRX 資料查不到 TESTNUMBER 欄位，Equipment 會留空"
    if sheet_prefix == "800G_Fixed_BER":
        return "⚠️ 3T BER 資料查不到 TESTNUMBER 欄位，Equipment 會留空"
    if sheet_prefix == "BER_Symbol_Error":
        return "⚠️ BER 資料查不到 TESTNUMBER 欄位，Equipment 會留空"
    return "⚠️ 查不到 TESTNUMBER 欄位，Equipment 會留空"


def fetch_equipment_map(module, df: pd.DataFrame, base_dir: str, sheet_prefix: str) -> dict[str, str]:
    if not hasattr(module, "fetch_master_equipment_map"):
        return {}
    testnumber_finder = getattr(module, "find_testnumber_column", None)
    if not callable(testnumber_finder):
        return {}
    testnumber_column = testnumber_finder(list(df.columns))
    if not testnumber_column:
        print(resolve_equipment_warning(sheet_prefix))
        return {}
    master_sql_file = getattr(module, "MASTER_SQL_FILE", "MASTER.sql")
    master_sql_path = os.path.join(base_dir, master_sql_file)
    if not os.path.exists(master_sql_path):
        print("⚠️ 找不到 MASTER.sql，Equipment 會留空")
        return {}
    load_sql = getattr(module, "load_sql", None)
    if callable(load_sql):
        master_sql = load_sql(master_sql_path)
    else:
        with open(master_sql_path, "r", encoding="utf-8") as file:
            master_sql = file.read()
    testnumbers = (
        df[testnumber_column]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )
    if not testnumbers:
        return {}
    with pyodbc.connect(module.conn_str_with_db(module.DATABASE), timeout=30) as conn:
        return module.fetch_master_equipment_map(conn, master_sql, testnumbers)


def should_add_equipment(sheet_prefix: str, category: str) -> bool:
    if sheet_prefix == "800G_TRX":
        return category != "其他"
    if sheet_prefix == "800G_Fixed_BER":
        return category == "3T_BER"
    if sheet_prefix == "BER_Symbol_Error":
        return category == "TC_BER"
    return False


def build_report(
    module,
    sheet_prefix: str,
    categories: list[str],
    category_builder,
    workbook: Workbook,
    start_date: str,
    end_date: str,
    base_dir: str,
) -> dict[str, object]:
    print(f"\n🚀 開始整合：{sheet_prefix}")
    t0 = time.time()

    df = export_report_dataframe(module, start_date, end_date)
    print(f"✅ export rows={len(df)} time={time.time()-t0:.1f}s")

    if module.CH_NUMBER_HEADER not in df.columns:
        raise KeyError(f"查無欄位 {module.CH_NUMBER_HEADER}")

    df["_category"] = category_builder(df, module)
    equipment_map = fetch_equipment_map(module, df, base_dir, sheet_prefix)
    df = module.apply_error_codes(df)
    metrics = module.build_data_analysis_metrics(df)
    failed_devices = module.build_failed_devices(df)
    if equipment_map and hasattr(module, "add_equipment_column"):
        failed_devices = module.add_equipment_column(failed_devices, equipment_map)
    failed_components = module.build_failed_component_records(df)

    for category in categories:
        sheet_df = df[df["_category"] == category].drop(columns=["_category"])
        if sheet_df.empty:
            sheet_df = df.head(0).drop(columns=["_category"])
        if equipment_map and hasattr(module, "add_equipment_column") and should_add_equipment(sheet_prefix, category):
            sheet_df = module.add_equipment_column(sheet_df, equipment_map)
        module.write_dataframe_to_sheet(workbook, f"{sheet_prefix} {category}", sheet_df)

    module.write_dataframe_to_sheet(workbook, f"{sheet_prefix} Failed Device", failed_devices)
    return {
        "module": module,
        "metrics": metrics,
        "failed_devices": failed_devices,
        "failed_components": failed_components,
    }


def populate_combined_data_analysis(workbook: Workbook, report_results: dict[str, dict[str, object]]) -> None:
    ws = ensure_data_analysis_template(workbook)

    trx_result = report_results.get("800G_TRX")
    fixed_result = report_results.get("800G_Fixed_BER")
    symbol_result = report_results.get("BER_Symbol_Error")

    combined_metrics: dict[str, dict[str, float]] = {}
    if trx_result:
        combined_metrics.update(trx_result["metrics"])
    if fixed_result:
        combined_metrics["3T BER"] = fixed_result["metrics"].get("3T BER", {})
    if symbol_result:
        combined_metrics["TC BER"] = symbol_result["metrics"].get("TC BER", {})

    fpy_row_map = {
        "DDMI": 3,
        "RT": 4,
        "LT": 5,
        "HT": 6,
        "Burn In": 7,
        "3T BER": 8,
        "TC BER": 9,
        "ATS": 10,
        "Switch": 11,
    }
    retest_row_map = {
        "DDMI": 16,
        "RT": 17,
        "LT": 18,
        "HT": 19,
        "Burn In": 20,
        "3T BER": 21,
        "TC BER": 22,
        "ATS": 23,
        "Switch": 24,
    }

    for station, row in fpy_row_map.items():
        data = combined_metrics.get(station, {})
        set_cell_value(ws, row, 2, data.get("fpy_input", 0))
        set_cell_value(ws, row, 3, data.get("fpy_output", 0))
        set_cell_value(ws, row, 4, data.get("fpy_rate", 0))

    for station, row in retest_row_map.items():
        data = combined_metrics.get(station, {})
        set_cell_value(ws, row, 2, data.get("retest_input", 0))
        set_cell_value(ws, row, 3, data.get("retest_output", 0))
        set_cell_value(ws, row, 4, data.get("retest_rate", 0))

    if trx_result:
        trx_module = trx_result["module"]
        pareto_configs = [
            ("DDMI", 28, 41),
            ("RT", 43, 56),
            ("LT", 58, 71),
            ("HT", 73, 86),
            ("ATS", 88, 100),
        ]
        for station, start_row, clear_until_row in pareto_configs:
            input_total = trx_result["metrics"].get(station, {}).get("fpy_input", 0)
            pareto_table = trx_module.build_pareto_table(
                trx_result["failed_components"],
                station,
                input_total,
            )
            trx_module.write_pareto_table(
                ws,
                start_row=start_row,
                table=pareto_table,
                clear_until_row=clear_until_row,
            )

    if fixed_result:
        fixed_module = fixed_result["module"]
        input_total = fixed_result["metrics"].get("3T BER", {}).get("fpy_input", 0)
        pareto_table = fixed_module.build_failed_device_pareto_table(
            fixed_result["failed_devices"],
            input_total,
        )
        fixed_module.write_pareto_table(
            ws,
            start_row=103,
            table=pareto_table,
            clear_until_row=115,
        )

    if symbol_result:
        symbol_module = symbol_result["module"]
        input_total = symbol_result["metrics"].get("TC BER", {}).get("fpy_input", 0)
        pareto_table = symbol_module.build_pareto_table(
            symbol_result["failed_components"],
            "TC BER",
            input_total,
        )
        symbol_module.write_pareto_table(
            ws,
            start_row=118,
            table=pareto_table,
            clear_until_row=130,
        )


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
        },
        {
            "sheet_prefix": "800G_Fixed_BER",
            "module_file": "800G_Fixed_BER_Test.py",
            "module_name": "report_fixed_ber_test",
            "categories": ["3T_BER", "其他"],
            "category_builder": lambda df, module: df[module.CH_NUMBER_HEADER].apply(module.classify_ch_number),
        },
        {
            "sheet_prefix": "BER_Symbol_Error",
            "module_file": "BER_Symbol_Error_Test.py",
            "module_name": "report_symbol_error_test",
            "categories": ["TC_BER", "其他"],
            "category_builder": lambda df, module: df[module.CH_NUMBER_HEADER].apply(module.classify_ch_number),
        },
    ]

    workbook = load_template_workbook(base_dir)
    ensure_data_analysis_template(workbook)
    report_results: dict[str, dict[str, object]] = {}

    try:
        for report in reports:
            module_path = os.path.join(base_dir, report["module_file"])
            module = load_module(module_path, report["module_name"])
            result = build_report(
                module,
                report["sheet_prefix"],
                report["categories"],
                report["category_builder"],
                workbook,
                start_date,
                end_date,
                base_dir,
            )
            report_results[report["sheet_prefix"]] = result

        populate_combined_data_analysis(workbook, report_results)

        workbook.save(out_path)
        print("📁 Combined Excel 已輸出：", out_path)

    except Exception as exc:
        print("❌ 查詢或匯出失敗：")
        print(exc)
        sys.exit(2)


if __name__ == "__main__":
    main()
