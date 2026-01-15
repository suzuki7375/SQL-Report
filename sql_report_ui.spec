# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_submodules

block_cipher = None

analysis = Analysis(
    ["sql_report_ui.py"],
    pathex=[],
    binaries=[],
    datas=[
        ("800G_TRX_TEST.py", "."),
        ("800G_Fixed_BER_Test.py", "."),
        ("BER_Symbol_Error_Test.py", "."),
        ("Combined Test Report.py", "."),
        ("master.py", "."),
        ("Function.xlsx", "."),
    ],
    hiddenimports=collect_submodules("tkinter"),
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
)

pyz = PYZ(analysis.pure, analysis.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    analysis.scripts,
    analysis.binaries,
    analysis.zipfiles,
    analysis.datas,
    [],
    name="sql_report_ui",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
)
