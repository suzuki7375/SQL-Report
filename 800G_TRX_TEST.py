# -*- coding: utf-8 -*-

import os
import sys
import time
import pandas as pd
import pyodbc

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

OUTPUT_CSV = "raw_from_sql.csv"


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


def main():
    test_login()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_path = os.path.join(base_dir, OUTPUT_CSV)

    try:
        print(f"🚀 連線 DB：{DATABASE}")
        with pyodbc.connect(conn_str_with_db(DATABASE), timeout=30) as conn:

            # 1) 先抓 TOP 0 取得欄位（確認你已「進入報表/view」）
            cur = conn.cursor()
            cur.execute(f"SELECT TOP 0 * FROM {TARGET_OBJECT};")
            cols = [d[0] for d in cur.description]
            print(f"✅ 欄位數：{len(cols)}（已連到該報表 view）")

            # 2) 預覽資料（讓你實際「看到資料」）
            preview_sql = f"SELECT TOP {PREVIEW_N} * FROM {TARGET_OBJECT};"
            print(f"\n👀 預覽資料 TOP {PREVIEW_N}：")
            t0 = time.time()
            df_preview = pd.read_sql_query(preview_sql, conn)
            print(f"✅ preview rows={len(df_preview)} time={time.time()-t0:.1f}s")
            with pd.option_context("display.max_columns", 20, "display.width", 180):
                print(df_preview.head(min(PREVIEW_N, 5)))

            # 3) 匯出 CSV（先小量）
            export_sql = f"SELECT TOP {EXPORT_N} * FROM {TARGET_OBJECT};"
            print(f"\n📤 匯出 TOP {EXPORT_N} 到 CSV：{OUTPUT_CSV}")
            t1 = time.time()
            df = pd.read_sql_query(export_sql, conn)
            df.to_csv(out_path, index=False, encoding="utf-8-sig")
            print(f"✅ export rows={len(df)} time={time.time()-t1:.1f}s")
            print("📁 CSV 已輸出：", out_path)

    except Exception as e:
        print("❌ 查詢或匯出失敗：")
        print(e)
        sys.exit(2)


if __name__ == "__main__":
    main()
