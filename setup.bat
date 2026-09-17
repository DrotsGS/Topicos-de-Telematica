@echo off
REM Prepara el entorno. Ejecutar UNA vez al clonar el repositorio.

echo == Creando entorno virtual ==
python -m venv venv
if errorlevel 1 goto error

echo == Instalando dependencias ==
call venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
if errorlevel 1 goto error

echo == Generando stubs de gRPC ==
python scripts\gen_proto.py
if errorlevel 1 goto error

echo.
echo Listo. Ahora abre tres terminales y ejecuta:
echo   1-namenode.bat
echo   2-datanode.bat
echo   dfsha.bat ping
goto :eof

:error
echo.
echo Fallo la preparacion del entorno.
exit /b 1
