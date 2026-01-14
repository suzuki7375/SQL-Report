# -*- coding: utf-8 -*-

import argparse
import os
import sys
import time

import pandas as pd
import pyodbc
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

SERVER = "omddb"
USERNAME = "PE_ReadOnlyUser"
PASSWORD = "pe@0505"
DRIVER = "ODBC Driver 17 for SQL Server"
DATABASE = "MEQueryManufacturingDatabase"

TARGET_OBJECT = "[工八_PEREADONLY].[LK2MES-DB-REAL].[dbo].[V_PE_PRD_TestResult_800G_TRX_TEST]"

# 先看資料用
PREVIEW_N = 20

# 匯出用（先小量，確定 OK 再加大）
EXPORT_N = 2000

OUTPUT_EXTENSION = ".xlsx"
TEST_ITEM_HEADER = "測試項目"
CH_NUMBER_HEADER = "CHNumber"
COMPONENT_ID_HEADER = "COMPONENTID"
DATA_ANALYSIS_SHEET = "Data Analysis"
ERROR_CODE_SHEET = "Error Code"
FUNCTION_TEMPLATE = "Function.xlsx"
ERROR_CODE_HEADER = "Error code"
FAILURE_CODE_HEADER = "FailureCodeID"
STATION_ORDER = [
    "DDMI",
    "RT",
    "LT",
    "HT",
    "Burn In",
    "3T BER",
    "TC BER",
    "ATS",
    "Switch",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    return parser.parse_args()


def conn_str_no_db() -> str:
    return (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "TrustServerCertificate=yes;"
    )


def conn_str_with_db(db: str) -> str:
    return (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={db};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "TrustServerCertificate=yes;"
    )


def test_login() -> None:
    conn = pyodbc.connect(conn_str_no_db(), timeout=5)
    cur = conn.cursor()
    cur.execute("SELECT SYSTEM_USER, SUSER_SNAME(), @@SERVERNAME")
    print("✅ 登入成功（未指定 DATABASE）")
    print("🔎 Login info:", cur.fetchone())
    conn.close()


def build_sorted_query(limit: int) -> str:
    return f"""
WITH base AS (
    SELECT *,
        TRY_CONVERT(date, SUBSTRING(TESTNUMBER, 2, 8)) AS test_date,
        DATETIMEFROMPARTS(
            TRY_CONVERT(int, SUBSTRING(TESTNUMBER, 2, 4)),
            TRY_CONVERT(int, SUBSTRING(TESTNUMBER, 6, 2)),
            TRY_CONVERT(int, SUBSTRING(TESTNUMBER, 8, 2)),
            TRY_CONVERT(int, SUBSTRING(TESTNUMBER, 10, 2)),
            TRY_CONVERT(int, SUBSTRING(TESTNUMBER, 12, 2)),
            0,
            0
        ) AS test_datetime
    FROM {TARGET_OBJECT}
)
SELECT TOP {limit} *
FROM base
WHERE test_date BETWEEN ? AND ?
ORDER BY test_datetime;
""".strip()


def apply_header(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    columns = list(df.columns)
    columns[0] = TEST_ITEM_HEADER
    df.columns = columns
    return df


def format_date_range(start_date: str, end_date: str) -> str:
    if start_date == end_date:
        return start_date
    return f"{start_date}_{end_date}"


def build_output_path(base_dir: str, start_date: str, end_date: str) -> str:
    base_name = os.path.splitext(os.path.basename(__file__))[0]
    date_range = format_date_range(start_date, end_date)
    filename = f"{base_name}_{date_range}{OUTPUT_EXTENSION}"
    return os.path.join(base_dir, filename)


def classify_ch_number(value: str) -> str:
    text = str(value) if value is not None else ""
    if "ATS" in text:
        return "ATS"
    if "DDMI" in text:
        return "DDMI"
    if "TP2TP3_LT" in text or "_LT" in text:
        return "LT"
    if "TP2TP3_HT" in text or "_HT" in text:
        return "HT"
    if "TP2TP3_RT" in text or "_RT" in text:
        return "RT"
    return "其他"


def find_column(columns: list[str], candidates: list[str]) -> str | None:
    lowered = {col.lower(): col for col in columns}
    for name in candidates:
        match = lowered.get(name.lower())
        if match:
            return match
    for col in columns:
        lowered_col = col.lower()
        if any(token in lowered_col for token in candidates):
            return col
    return None


def find_component_column(columns: list[str]) -> str:
    column = find_column(
        columns,
        [COMPONENT_ID_HEADER.lower(), "component_id", "component"],
    )
    if not column:
        raise KeyError(f"查無欄位 {COMPONENT_ID_HEADER}")
    return column


def find_failure_code_column(columns: list[str]) -> str | None:
    return find_column(
        columns,
        [
            FAILURE_CODE_HEADER.lower(),
            "failure_code_id",
            "failure_code",
            "failurecodeid",
            "failurecode",
        ],
    )


def find_station_column(columns: list[str]) -> str | None:
    return find_column(columns, ["station", "teststation", "test_station"])


def find_result_column(columns: list[str]) -> str | None:
    return find_column(
        columns,
        [
            "result",
            "testresult",
            "test_result",
            "pass_fail",
            "passfail",
            "pf",
            "status",
        ],
    )


def find_ch_pass_fail_columns(columns: list[str]) -> list[str]:
    return [col for col in columns if "ch_pass_fail" in col.lower()]


def normalize_station(text: str) -> str | None:
    if not text:
        return None
    upper = text.upper()
    if "DDMI" in upper:
        return "DDMI"
    if "3T" in upper and "BER" in upper:
        return "3T BER"
    if "TC" in upper and "BER" in upper:
        return "TC BER"
    if "ATS" in upper:
        return "ATS"
    if "BURN" in upper:
        return "Burn In"
    if "SWITCH" in upper:
        return "Switch"
    if "TP2TP3_LT" in upper or "_LT" in upper or upper.endswith("LT") or upper.startswith("LT"):
        return "LT"
    if "TP2TP3_HT" in upper or "_HT" in upper or upper.endswith("HT") or upper.startswith("HT"):
        return "HT"
    if "TP2TP3_RT" in upper or "_RT" in upper or upper.endswith("RT") or upper.startswith("RT"):
        return "RT"
    return None


def determine_station(row: pd.Series, station_column: str | None) -> str | None:
    if station_column:
        value = row.get(station_column)
        station = normalize_station(str(value)) if value is not None else None
        if station:
            return station
    value = row.get(CH_NUMBER_HEADER)
    return normalize_station(str(value)) if value is not None else None


def is_pass(value: object) -> bool:
    if value is None:
        return False
    text = str(value).strip().upper()
    return text in {"PASS", "P", "OK", "TRUE", "1", "Y", "YES"}


def determine_sort_columns(df: pd.DataFrame) -> list[str]:
    candidates = [
        "test_datetime",
        "TEST_DATETIME",
        "TestDateTime",
        "TESTNUMBER",
        "TestNumber",
        "TEST_TIME",
        "TESTDATE",
    ]
    columns = [col for col in candidates if col in df.columns]
    return columns if columns else []


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def has_value(value: object) -> bool:
    text = normalize_text(value)
    return text not in {"", "nan", "none", "null"}


def split_into_tests(group_df: pd.DataFrame, expected_count: int, sort_columns: list[str]) -> list[pd.DataFrame]:
    if sort_columns:
        group_df = group_df.sort_values(sort_columns)
    total_rows = len(group_df)
    if total_rows == 0:
        return []
    tests = []
    for start in range(0, total_rows, expected_count):
        tests.append(group_df.iloc[start : start + expected_count])
    return tests


def load_error_code_mapping(workbook: Workbook) -> list[tuple[str, str]]:
    if ERROR_CODE_SHEET not in workbook.sheetnames:
        return []
    ws = workbook[ERROR_CODE_SHEET]
    mapping: list[tuple[str, str]] = []
    for error_code, failure_code in ws.iter_rows(min_row=2, max_col=2, values_only=True):
        if not error_code or not failure_code:
            continue
        mapping.append((normalize_text(error_code), normalize_text(failure_code)))
    return mapping


def match_error_code(value: object, mapping: list[tuple[str, str]]) -> str:
    text = normalize_text(value)
    if not text:
        return ""

    prefix = text.split(":", 1)[0].strip()
    if prefix:
        for error_code, _ in mapping:
            if error_code.lower() == prefix.lower():
                return error_code
        return prefix

    lowered = text.lower()
    for error_code, pattern in mapping:
        pattern_text = pattern.lower()
        if pattern_text in lowered or lowered in pattern_text:
            return error_code
    return ""


def build_component_error_codes(df: pd.DataFrame, mapping: list[tuple[str, str]]) -> dict[str, str]:
    if not mapping:
        return {}
    component_column = find_component_column(list(df.columns))
    failure_code_column = find_failure_code_column(list(df.columns))
    if not failure_code_column:
        print("⚠️ 找不到 FailureCodeID 欄位，Error code 將為空白")
        return {}

    target_categories = {"ATS", "DDMI", "RT", "LT", "HT"}
    if "_category" in df.columns:
        source_df = df[df["_category"].isin(target_categories)]
    else:
        source_df = df

    error_codes: dict[str, str] = {}
    for component_id, group in source_df.groupby(component_column):
        failure_values = [value for value in group[failure_code_column] if has_value(value)]
        if not failure_values:
            continue
        selected_code = ""
        for value in failure_values:
            selected_code = match_error_code(value, mapping)
            if selected_code:
                break
        if selected_code:
            error_codes[component_id] = selected_code
    return error_codes


def apply_error_codes(df: pd.DataFrame, mapping: list[tuple[str, str]]) -> pd.DataFrame:
    if df.empty:
        df[ERROR_CODE_HEADER] = ""
        return df
    component_column = find_component_column(list(df.columns))
    error_code_map = build_component_error_codes(df, mapping)
    df = df.copy()
    df[ERROR_CODE_HEADER] = df[component_column].map(error_code_map).fillna("")
    return df


def build_data_analysis_metrics(df: pd.DataFrame) -> dict[str, dict[str, float]]:
    component_column = find_component_column(list(df.columns))
    result_column = find_result_column(list(df.columns))
    station_column = find_station_column(list(df.columns))

    df = df.copy()
    df["_station"] = df.apply(lambda row: determine_station(row, station_column), axis=1)
    df = df[df["_station"].isin(STATION_ORDER)]

    if not result_column:
        print("⚠️ 找不到結果欄位，良率將以 0 計算")

    sort_columns = determine_sort_columns(df)
    metrics: dict[str, dict[str, float]] = {}
    for station in STATION_ORDER:
        station_df = df[df["_station"] == station]
        expected_count = 8 if station in {"DDMI", "RT", "LT", "HT"} else 24
        fpy_input = 0
        fpy_output = 0
        retest_input = 0
        retest_output = 0

        for _, group in station_df.groupby(component_column):
            tests = split_into_tests(group, expected_count, sort_columns)
            if not tests:
                continue
            fpy_input += 1
            if result_column and all(is_pass(val) for val in tests[0][result_column]):
                fpy_output += 1
            if len(tests) > 1:
                retest_input += len(tests) - 1
                if result_column:
                    retest_output += sum(
                        1 for test_df in tests[1:] if all(is_pass(val) for val in test_df[result_column])
                    )

        fpy_rate = fpy_output / fpy_input if fpy_input else 0
        retest_rate = retest_output / retest_input if retest_input else 0
        metrics[station] = {
            "fpy_input": fpy_input,
            "fpy_output": fpy_output,
            "fpy_rate": fpy_rate,
            "retest_input": retest_input,
            "retest_output": retest_output,
            "retest_rate": retest_rate,
        }
    return metrics


def build_failed_devices(df: pd.DataFrame) -> pd.DataFrame:
    component_column = find_component_column(list(df.columns))
    ch_pass_fail_columns = find_ch_pass_fail_columns(list(df.columns))

    if not ch_pass_fail_columns:
        print("⚠️ 找不到 CH_Pass_Fail 欄位，Failed Device sheet 將為空")
        return df.head(0).drop(columns=["_category"], errors="ignore")

    sort_columns = determine_sort_columns(df)
    failed_tests: list[pd.DataFrame] = []

    for category in ["ATS", "DDMI", "RT", "LT", "HT"]:
        category_df = df[df["_category"] == category]
        if category_df.empty:
            continue
        expected_count = 24 if category == "ATS" else 8
        for _, group in category_df.groupby(component_column):
            tests = split_into_tests(group, expected_count, sort_columns)
            for test_df in tests:
                if any(
                    not is_pass(value)
                    for column in ch_pass_fail_columns
                    for value in test_df[column]
                ):
                    failed_tests.append(test_df)

    if failed_tests:
        failed_df = pd.concat(failed_tests, ignore_index=True)
    else:
        failed_df = df.head(0)

    return failed_df.drop(columns=["_category"], errors="ignore")


def load_output_workbook(base_dir: str) -> Workbook:
    template_path = os.path.join(base_dir, FUNCTION_TEMPLATE)
    if os.path.exists(template_path):
        return load_workbook(template_path)
    workbook = Workbook()
    if workbook.active:
        workbook.remove(workbook.active)
    workbook.create_sheet(DATA_ANALYSIS_SHEET)
    return workbook


def write_dataframe_to_sheet(workbook: Workbook, sheet_name: str, df: pd.DataFrame) -> None:
    if sheet_name in workbook.sheetnames:
        del workbook[sheet_name]
    ws = workbook.create_sheet(title=sheet_name)
    for row in dataframe_to_rows(df, index=False, header=True):
        ws.append(row)


def populate_data_analysis_sheet(workbook: Workbook, metrics: dict[str, dict[str, float]]) -> None:
    if DATA_ANALYSIS_SHEET not in workbook.sheetnames:
        workbook.create_sheet(DATA_ANALYSIS_SHEET)
    ws = workbook[DATA_ANALYSIS_SHEET]

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
        data = metrics.get(station, {})
        ws[f"B{row}"] = data.get("fpy_input", 0)
        ws[f"C{row}"] = data.get("fpy_output", 0)
        ws[f"D{row}"] = data.get("fpy_rate", 0)

    for station, row in retest_row_map.items():
        data = metrics.get(station, {})
        ws[f"B{row}"] = data.get("retest_input", 0)
        ws[f"C{row}"] = data.get("retest_output", 0)
        ws[f"D{row}"] = data.get("retest_rate", 0)


def main():
    args = parse_args()
    test_login()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_path = build_output_path(base_dir, args.start_date, args.end_date)

    try:
        print(f"🚀 連線 DB：{DATABASE}")
        with pyodbc.connect(conn_str_with_db(DATABASE), timeout=30) as conn:

            # 1) 先抓 TOP 0 取得欄位（確認你已「進入報表/view」）
            cur = conn.cursor()
            cur.execute(f"SELECT TOP 0 * FROM {TARGET_OBJECT};")
            cols = [d[0] for d in cur.description]
            print(f"✅ 欄位數：{len(cols)}（已連到該報表 view）")

            # 2) 匯出資料（先小量）
            export_sql = build_sorted_query(EXPORT_N)
            print(f"\n📤 匯出 TOP {EXPORT_N} 到 Excel：{out_path}")
            t0 = time.time()
            df = pd.read_sql_query(
                export_sql,
                conn,
                params=[args.start_date, args.end_date],
            )
            df = apply_header(df)
            print(f"✅ export rows={len(df)} time={time.time()-t0:.1f}s")
            if PREVIEW_N > 0:
                print(f"\n👀 預覽資料 TOP {PREVIEW_N}：")
                with pd.option_context("display.max_columns", 20, "display.width", 180):
                    print(df.head(min(PREVIEW_N, 5)))

            if CH_NUMBER_HEADER not in df.columns:
                raise KeyError(f"查無欄位 {CH_NUMBER_HEADER}")

            categories = ["ATS", "DDMI", "LT", "HT", "RT", "其他"]
            df["_category"] = df[CH_NUMBER_HEADER].apply(classify_ch_number)
            workbook = load_output_workbook(base_dir)
            error_code_mapping = load_error_code_mapping(workbook)
            df = apply_error_codes(df, error_code_mapping)
            metrics = build_data_analysis_metrics(df)
            failed_devices = build_failed_devices(df)
            for category in categories:
                sheet_df = df[df["_category"] == category].drop(columns=["_category"])
                if sheet_df.empty:
                    sheet_df = df.head(0).drop(columns=["_category"])
                write_dataframe_to_sheet(workbook, category, sheet_df)

            write_dataframe_to_sheet(workbook, "Failed Device", failed_devices)
            populate_data_analysis_sheet(workbook, metrics)
            workbook.save(out_path)

            print("📁 Excel 已輸出：", out_path)

    except Exception as e:
        print("❌ 查詢或匯出失敗：")
        print(e)
        sys.exit(2)


if __name__ == "__main__":
    main()
