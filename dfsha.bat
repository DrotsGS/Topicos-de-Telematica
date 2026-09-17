@echo off
REM Terminal 3: el cliente.  Uso:  dfsha ping  |  dfsha mkdir /docs  |  dfsha ls /
call venv\Scripts\activate.bat
set NAMENODE_ADDR=localhost:50051
python client\cli.py %*
