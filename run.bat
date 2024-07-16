@echo off
setlocal enabledelayedexpansion

:: Captura todos los argumentos pasados a run.bat
set ARGUMENTOS=%*

:: Usa %~dp0 para obtener el directorio completo del script actual
set VOLUMEN_MONTAR=%~dp0

:: Ejecuta el comando docker con los argumentos capturados y luego fuerza la eliminación del contenedor
docker run -it --rm -v "%VOLUMEN_MONTAR%:/app" sist-dist:v0 /bin/bash -c "python src/__init__.py %ARGUMENTOS% && exec bash || true"