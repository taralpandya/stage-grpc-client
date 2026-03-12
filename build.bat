@echo off
echo Cleaning previous build...
rmdir /s /q build 2>nul
rmdir /s /q dist 2>nul

echo Compiling proto...
python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/service.proto

echo Building exe...
pyinstaller client.spec --clean

echo Done! Exe is at dist\phlserv-client.exe
pause