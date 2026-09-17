@echo off
REM Terminal 4 (opcional): un segundo DataNode, en otro puerto.
REM Cada uno guarda sus bloques en data\<NODE_ID>, no se pisan.
call venv\Scripts\activate.bat
set NODE_ID=dn-2
set PORT=50061
set ADVERTISE_ADDR=localhost:50061
set NAMENODE_ADDR=localhost:50051
python datanode\server.py
