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
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils.dataframe import dataframe_to_rows

OUTPUT_EXTENSION = ".xlsx"
FUNCTION_TEMPLATE = "Function.xlsx"
DATA_ANALYSIS_SHEET = "Data Analysis"
EQUIPMENT_STATUS_SHEET = "equiment status"
ERROR_CODE_HEADER_DEFAULT = "Error code"
STATION_NAME_HEADER = "STATION NAME"
ERROR_CODE_CANONICAL_HEADER = "Error Code"
EQUIPMENT_STATUS_CATEGORY_HEADER = "分類方式"
STATION_NAME_MAP = {
    "ATS": {
        "T157100002205_1": "ATS1_L",
        "T157100002205_2": "ATS1_R",
        "T157100002022_1": "ATS2_L",
        "T157100002022_2": "ATS2_R",
        "T157100002072_1": "ATS3_L",
        "T157100002072_2": "ATS3_R",
        "T157100002201_1": "ATS4_L",
        "T157100002201_2": "ATS4_R",
        "T157100002402_1": "ATS5_L",
        "T157100002402_2": "ATS5_R",
        "T157100002535_1": "ATS6_L",
        "T157100002535_2": "ATS6_R",
        "T157100002533_1": "ATS7_L",
        "T157100002533_2": "ATS7_R",
        "T157100002329_1": "OQC_L",
        "T157100002329_2": "OQC_R",
    },
    "RT": {
        "T157100002534": "RT2",
        "T157100002113": "RT1",
    },
}


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


def find_location_column(columns: list[str]) -> str | None:
    candidates = [
        "location",
        "testlocation",
        "test_location",
        "stationlocation",
        "station_location",
        "site",
        "testsite",
        "test_site",
        "line",
        "testline",
        "test_line",
    ]
    return next((col for col in columns if col.lower() in candidates), None)


def resolve_station_name(category: object, equipment: object) -> str:
    category_key = str(category).strip()
    equipment_key = str(equipment).strip()
    if not category_key or not equipment_key:
        return ""
    return STATION_NAME_MAP.get(category_key, {}).get(equipment_key, "")


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


def normalize_excel_value(value: object) -> object:
    if isinstance(value, (tuple, list, set, dict)):
        return str(value)
    return value


def write_dataframe_to_sheet(workbook: Workbook, sheet_name: str, df: pd.DataFrame) -> None:
    if sheet_name in workbook.sheetnames:
        workbook.remove(workbook[sheet_name])
    ws = workbook.create_sheet(title=sheet_name)
    safe_df = df.applymap(normalize_excel_value)
    for row in dataframe_to_rows(safe_df, index=False, header=True):
        ws.append(row)


def _measure_cell_length(value: object) -> int:
    if value is None:
        return 0
    text = str(value)
    return max((len(line) for line in text.splitlines()), default=0)


def format_equipment_status_sheet(ws) -> None:
    header_fill = PatternFill(fill_type="solid", fgColor="BDD7EE")
    border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )
    header_font = Font(name="Calibri", size=9, bold=True)
    body_font = Font(name="Calibri", size=11)
    header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for row in ws.iter_rows():
        for cell in row:
            cell.font = body_font

    for cell in ws[1]:
        cell.fill = header_fill
        cell.border = border
        cell.font = header_font
        cell.alignment = header_alignment

    yield_rate_col = None
    for idx, cell in enumerate(ws[1], start=1):
        if cell.value == "Yield rate":
            yield_rate_col = idx
            break

    if yield_rate_col:
        for row_idx in range(2, ws.max_row + 1):
            ws.cell(row=row_idx, column=yield_rate_col).number_format = "0.0%"

    for column_cells in ws.columns:
        column_letter = column_cells[0].column_letter
        max_length = max((_measure_cell_length(cell.value) for cell in column_cells), default=0)
        ws.column_dimensions[column_letter].width = min(max(max_length + 2, 12), 60)


def reorder_error_code_column(
    df: pd.DataFrame,
    error_code_header: str,
    insert_after_index: int = 8,
) -> pd.DataFrame:
    if error_code_header not in df.columns:
        return df
    columns = list(df.columns)
    if columns.index(error_code_header) == insert_after_index:
        return df
    columns.remove(error_code_header)
    insert_position = min(insert_after_index, len(columns))
    columns.insert(insert_position, error_code_header)
    return df[columns]


def move_sheet_after(workbook: Workbook, sheet_name: str, after_sheet_name: str) -> None:
    if sheet_name not in workbook.sheetnames or after_sheet_name not in workbook.sheetnames:
        return
    sheet = workbook[sheet_name]
    after_sheet = workbook[after_sheet_name]
    workbook._sheets.remove(sheet)
    insert_index = workbook._sheets.index(after_sheet) + 1
    workbook._sheets.insert(insert_index, sheet)


def hide_sheet(workbook: Workbook, sheet_name: str) -> None:
    if sheet_name in workbook.sheetnames:
        workbook[sheet_name].sheet_state = "hidden"


def _add_equipment_column(df: pd.DataFrame, module, equipment_map: dict[str, str]) -> pd.DataFrame:
    if equipment_map and hasattr(module, "add_equipment_column"):
        return module.add_equipment_column(df, equipment_map)
    if "Equipment" not in df.columns:
        df = df.copy()
        df["Equipment"] = ""
    return df


def _add_location_column(df: pd.DataFrame, location_column: str | None) -> pd.DataFrame:
    df = df.copy()
    if location_column and location_column in df.columns:
        df["Location"] = df[location_column].fillna("")
    else:
        df["Location"] = ""
    return df


def _compute_group_fpy(
    df: pd.DataFrame,
    group_fields: list[str],
    module,
    expected_count: int,
    use_ch_pass_fail: bool = False,
) -> list[dict[str, object]]:
    component_column = module.find_component_column(list(df.columns))
    result_column = module.find_result_column(list(df.columns))
    ch_pass_fail_columns = module.find_ch_pass_fail_columns(list(df.columns))
    sort_columns = module.determine_sort_columns(df)

    if use_ch_pass_fail and not ch_pass_fail_columns:
        print("⚠️ 找不到 CH_Pass_Fail 欄位，良率將以 0 計算")
    if not use_ch_pass_fail and not result_column:
        print("⚠️ 找不到結果欄位，良率將以 0 計算")

    rows: list[dict[str, object]] = []
    grouped = df.groupby(group_fields, dropna=False) if group_fields else [((), df)]
    for keys, group_df in grouped:
        fpy_input = 0
        fpy_output = 0
        for _, component_group in group_df.groupby(component_column):
            tests = module.split_into_tests(component_group, expected_count, sort_columns)
            if not tests:
                continue
            fpy_input += 1
            passed = False
            if use_ch_pass_fail:
                if ch_pass_fail_columns:
                    passed = all(
                        module.is_pass(value)
                        for column in ch_pass_fail_columns
                        for value in tests[0][column]
                    )
            else:
                if result_column:
                    passed = all(module.is_pass(val) for val in tests[0][result_column])
            if passed:
                fpy_output += 1
        fpy_rate = fpy_output / fpy_input if fpy_input else 0
        if group_fields:
            if len(group_fields) == 1:
                key_map = {group_fields[0]: keys}
            else:
                key_map = dict(zip(group_fields, keys))
        else:
            key_map = {}
        rows.append(
            {
                **key_map,
                "fpy_input": fpy_input,
                "fpy_output": fpy_output,
                "fpy_rate": fpy_rate,
            }
        )
    return rows


def build_error_code_summary(report_results: dict[str, dict[str, object]]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    report_name_map = {
        "800G_TRX": "800G_TRX_TEST",
        "800G_Fixed_BER": "800G_Fixed_BER_Test",
        "BER_Symbol_Error": "BER_Symbol_Error_Test",
    }

    for sheet_prefix, result in report_results.items():
        failed_devices = result.get("failed_devices")
        if failed_devices is None or failed_devices.empty:
            continue
        module = result["module"]
        equipment_map = result.get("equipment_map", {})
        df = failed_devices.copy()
        if "Equipment" not in df.columns:
            df = _add_equipment_column(df, module, equipment_map)
        location_column = find_location_column(list(df.columns))
        df = _add_location_column(df, location_column)
        error_code_header = getattr(module, "ERROR_CODE_HEADER", ERROR_CODE_HEADER_DEFAULT)
        if error_code_header not in df.columns:
            continue
        df[ERROR_CODE_CANONICAL_HEADER] = df[error_code_header].fillna("").astype(str).str.strip()
        df = df[df[ERROR_CODE_CANONICAL_HEADER] != ""]
        if df.empty:
            continue
        component_column = module.find_component_column(list(df.columns))
        if component_column:
            df["_component_id"] = df[component_column].fillna("").astype(str)
        if "Category" not in df.columns:
            df["Category"] = ""
        df["Report"] = report_name_map.get(sheet_prefix, sheet_prefix)
        frame_columns = ["Report", "Category", "Equipment", "Location", ERROR_CODE_CANONICAL_HEADER]
        if "_component_id" in df.columns:
            frame_columns.append("_component_id")
        frames.append(df[frame_columns])

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    group_columns = ["Report", "Category", "Equipment", "Location", ERROR_CODE_CANONICAL_HEADER]
    if "_component_id" in combined.columns:
        counts = (
            combined.groupby(group_columns, dropna=False)["_component_id"]
            .nunique()
            .reset_index(name="count")
        )
    else:
        counts = (
            combined.groupby(group_columns, dropna=False)
            .size()
            .reset_index(name="count")
        )
    summary = counts.pivot_table(
        index=["Report", "Category", "Equipment", "Location"],
        columns=ERROR_CODE_CANONICAL_HEADER,
        values="count",
        fill_value=0,
        aggfunc="sum",
    ).reset_index()
    return summary


def build_equipment_status_table(report_results: dict[str, dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    trx_result = report_results.get("800G_TRX")
    if trx_result:
        module = trx_result["module"]
        equipment_map = trx_result.get("equipment_map", {})
        df = trx_result["df"].copy()
        df = _add_equipment_column(df, module, equipment_map)
        location_column = find_location_column(list(df.columns))
        if not location_column:
            print("⚠️ TRX 資料查不到 Location 欄位，Location 會留空")
        df = _add_location_column(df, location_column)
        for category in ["ATS", "DDMI", "RT", "LT", "HT"]:
            category_df = df[df["_category"] == category]
            if category_df.empty:
                continue
            expected_count = 24 if category == "ATS" else 8
            group_fields = ["Equipment", "Location"]
            for row in _compute_group_fpy(category_df, group_fields, module, expected_count):
                row.update(
                    {
                        "Report": "800G_TRX_TEST",
                        "Category": category,
                    }
                )
                if "Location" not in row:
                    row["Location"] = ""
                row[STATION_NAME_HEADER] = resolve_station_name(row.get("Category", ""), row.get("Equipment", ""))
                row["Yield rate"] = row.pop("fpy_rate")
                rows.append(row)

    fixed_result = report_results.get("800G_Fixed_BER")
    if fixed_result:
        module = fixed_result["module"]
        equipment_map = fixed_result.get("equipment_map", {})
        df = fixed_result["df"].copy()
        df = _add_equipment_column(df, module, equipment_map)
        location_column = find_location_column(list(df.columns))
        if not location_column:
            print("⚠️ 3T BER 資料查不到 Location 欄位，Location 會留空")
        df = _add_location_column(df, location_column)
        category_df = df[df["_category"] == "3T_BER"]
        if not category_df.empty:
            for row in _compute_group_fpy(category_df, ["Equipment", "Location"], module, 32, use_ch_pass_fail=True):
                row.update(
                    {
                        "Report": "800G_Fixed_BER_Test",
                        "Category": "3T_BER",
                    }
                )
                row[STATION_NAME_HEADER] = resolve_station_name(row.get("Category", ""), row.get("Equipment", ""))
                row["Yield rate"] = row.pop("fpy_rate")
                rows.append(row)

    symbol_result = report_results.get("BER_Symbol_Error")
    if symbol_result:
        module = symbol_result["module"]
        equipment_map = symbol_result.get("equipment_map", {})
        df = symbol_result["df"].copy()
        df = _add_equipment_column(df, module, equipment_map)
        location_column = find_location_column(list(df.columns))
        if not location_column:
            print("⚠️ BER 資料查不到 Location 欄位，Location 會留空")
        df = _add_location_column(df, location_column)
        category_df = df[df["_category"] == "TC_BER"]
        if not category_df.empty:
            for row in _compute_group_fpy(category_df, ["Equipment", "Location"], module, 32, use_ch_pass_fail=True):
                row.update(
                    {
                        "Report": "BER_Symbol_Error_Test",
                        "Category": "TC_BER",
                    }
                )
                row[STATION_NAME_HEADER] = resolve_station_name(row.get("Category", ""), row.get("Equipment", ""))
                row["Yield rate"] = row.pop("fpy_rate")
                rows.append(row)

    if not rows:
        return pd.DataFrame(
            columns=[
                "Report",
                EQUIPMENT_STATUS_CATEGORY_HEADER,
                "Equipment",
                STATION_NAME_HEADER,
                "Location",
                "fpy_input",
                "fpy_output",
                "Yield rate",
            ]
        )

    table = pd.DataFrame(rows)
    error_code_summary = build_error_code_summary(report_results)
    if not error_code_summary.empty:
        table = table.merge(
            error_code_summary,
            on=["Report", "Category", "Equipment", "Location"],
            how="left",
        )

    report_order = ["800G_TRX_TEST", "800G_Fixed_BER_Test", "BER_Symbol_Error_Test"]
    category_order = ["ATS", "DDMI", "RT", "LT", "HT", "3T_BER", "TC_BER"]
    table["_report_order"] = pd.Categorical(table["Report"], categories=report_order, ordered=True)
    table["_category_order"] = pd.Categorical(table["Category"], categories=category_order, ordered=True)
    table = table.sort_values(
        by=["_report_order", "_category_order", "Equipment", "Location"],
        kind="stable",
        na_position="last",
    ).drop(columns=["_report_order", "_category_order"])

    column_order = [
        "Report",
        EQUIPMENT_STATUS_CATEGORY_HEADER,
        "Equipment",
        STATION_NAME_HEADER,
        "Location",
        "fpy_input",
        "fpy_output",
        "Yield rate",
    ]
    table = table.rename(columns={"Category": EQUIPMENT_STATUS_CATEGORY_HEADER})
    for column in column_order:
        if column not in table.columns:
            table[column] = ""
    error_code_columns = [
        column
        for column in table.columns
        if column not in column_order
    ]
    if error_code_columns:
        table[error_code_columns] = table[error_code_columns].fillna(0).astype(int)
    return table[column_order + error_code_columns]


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
        error_code_header = getattr(module, "ERROR_CODE_HEADER", ERROR_CODE_HEADER_DEFAULT)
        sheet_df = reorder_error_code_column(sheet_df, error_code_header)
        module.write_dataframe_to_sheet(workbook, f"{sheet_prefix} {category}", sheet_df)

    error_code_header = getattr(module, "ERROR_CODE_HEADER", ERROR_CODE_HEADER_DEFAULT)
    failed_devices = reorder_error_code_column(failed_devices, error_code_header)
    module.write_dataframe_to_sheet(workbook, f"{sheet_prefix} Failed Device", failed_devices)
    return {
        "df": df,
        "equipment_map": equipment_map,
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
        equipment_status = build_equipment_status_table(report_results)
        write_dataframe_to_sheet(workbook, EQUIPMENT_STATUS_SHEET, equipment_status)
        format_equipment_status_sheet(workbook[EQUIPMENT_STATUS_SHEET])
        move_sheet_after(workbook, EQUIPMENT_STATUS_SHEET, DATA_ANALYSIS_SHEET)
        hide_sheet(workbook, "Error Code")
        hide_sheet(workbook, "800G_TRX 其他")
        hide_sheet(workbook, "800G_Fixed_BER 其他")
        hide_sheet(workbook, "BER_Symbol_Error 其他")

        workbook.save(out_path)
        print("📁 Combined Excel 已輸出：", out_path)

    except Exception as exc:
        print("❌ 查詢或匯出失敗：")
        print(exc)
        sys.exit(2)


if __name__ == "__main__":
    main()
