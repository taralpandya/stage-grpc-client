# client.spec
import os
from PyInstaller.building.api import PYZ, EXE
from PyInstaller.building.build_main import Analysis

block_cipher = None

a = Analysis(
    ['main.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        # Icons
        ('assets/icon_connected.png',    'assets'),
        ('assets/icon_disconnected.png', 'assets'),
        ('assets/icon_syncing_0.png',    'assets'),
        ('assets/icon_syncing_1.png',    'assets'),
        ('assets/icon_syncing_2.png',    'assets'),
        ('assets/icon_syncing_3.png',    'assets'),
        ('assets/icon_syncing_4.png',    'assets'),
        ('assets/icon_syncing_5.png',    'assets'),
        ('assets/icon_syncing_6.png',    'assets'),
        ('assets/icon_syncing_7.png',    'assets'),

        # Certs — bundled per client build
        ('certs/ca/ca.crt',   'certs/ca'),
        ('certs/client.crt',  'certs'),
        ('certs/client.key',  'certs'),

        # Proto generated files
        ('service_pb2.py',      '.'),
        ('service_pb2_grpc.py', '.'),

        # Env file
        ('.env', '.'),
    ],
    hiddenimports=[
        'grpc',
        'grpc.aio',
        'grpc._cython.cygrpc',
        'google.protobuf',
        'mysql.connector',
        'mysql.connector.plugins',
        'mysql.connector.plugins.mysql_native_password',
        'pystray',
        'pystray._win32',
        'PIL',
        'PIL.Image',
        'wmi',
        'win32api',
        'win32con',
        'win32gui',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter',
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='phlserv-client',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,      # ← no console window, runs silently
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # icon='assets/icon_connected.ico',  # uncomment if you have an .ico file
)