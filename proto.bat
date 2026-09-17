@echo off
REM Regenera los stubs. Ejecutalo cada vez que edites proto\dfsha.proto.
call venv\Scripts\activate.bat
python scripts\gen_proto.py
