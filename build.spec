# -*- mode: python -*-
from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

hidden_imports = [
    'pandas._libs.interval',
    'pandas._libs.tslibs',
    'openpyxl.styles.stylesheet'
]

datas = collect_data_files('pandas') + collect_data_files('openpyxl')

a = Analysis(
    ['Account opening software v5.0.py'],
    pathex=[],
    binaries=[],
    datas=datas + [('homophones.json', '.')],
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['_bz2', '_lzma'],
    win_no_prefer_redirects=False,
    # win_private_assemblies=True,  # 已删除
    cipher=block_cipher,
    noarchive=False
)

# 手动排除DLL
a.binaries = [
    x for x in a.binaries
    if not x[0].startswith(('api-ms-win', 'winsxs'))
]

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='TableSearchSystem',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    icon='app_icon.ico'
)