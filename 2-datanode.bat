@echo off
REM Terminal 2: el primer DataNode
REM Para un segundo DataNode local usa 3-datanode2.bat (puerto distinto).
call venv\Scripts\activate.bat
set NODE_ID=dn-1
set PORT=50060
set ADVERTISE_ADDR=localhost:50060
set NAMENODE_ADDR=localhost:50051
python datanode\server.py
