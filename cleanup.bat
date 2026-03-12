for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"
del /s /q *.pyc 2>nul
del /q *_pb2.py *_pb2_grpc.py 2>nul
rmdir /s /q build 2>nul
rmdir /s /q dist 2>nul
del /s /q ._* 2>nul
