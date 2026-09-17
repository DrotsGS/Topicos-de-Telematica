@echo off
REM Terminal 1: el NameNode
call venv\Scripts\activate.bat
set NODE_ID=nn-1
set PORT=50051
python namenode\server.py
