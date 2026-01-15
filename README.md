# SQL Report UI 打包說明

這個專案使用 `sql_report_ui.py` 做為 UI 入口，按鈕會依序執行下列腳本：

- `800G_TRX_TEST.py`
- `800G_Fixed_BER_Test.py`
- `BER_Symbol_Error_Test.py`
- `Combined Test Report.py`

因為腳本檔名包含數字與空白，PyInstaller 無法把它們視為可直接 import 的模組，
所以必須「以資料檔」方式一起打包，避免執行時找不到檔案。

## 開發環境相依套件

執行 `master.py` 需要安裝資料庫連線與資料處理套件，請先安裝：

```bash
python -m pip install sqlalchemy pyodbc pandas
```

> 若出現 `No module named 'sqlalchemy'`，代表尚未安裝上述套件。

## 方式一：使用提供的 spec 檔案

```bash
pyinstaller sql_report_ui.spec
```

產出檔案會在 `dist/sql_report_ui/`（或 `dist/sql_report_ui.exe`）中。

## 方式二：用指令手動打包（onefile 範例）

> Windows 指令請留意分號 `;` 的資料路徑分隔符號。

```bash
pyinstaller \
  --name sql_report_ui \
  --onefile \
  --windowed \
  --add-data "800G_TRX_TEST.py;." \
  --add-data "800G_Fixed_BER_Test.py;." \
  --add-data "BER_Symbol_Error_Test.py;." \
  --add-data "Combined Test Report.py;." \
  --add-data "Function.xlsx;." \
  sql_report_ui.py
```

## 執行方式

打包完成後直接執行 `sql_report_ui.exe`，即可在沒有 Python 的環境中使用。
若仍出現「找不到腳本檔案」的錯誤，請確認所有 `.py` 與 `Function.xlsx` 都已被打包。
