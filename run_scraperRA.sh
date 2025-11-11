#!/bin/bash

# Activar log de debug para cron (aparte del log principal por fecha)
exec > >(tee -a "/home/ubuntu/tsousa/scraper-reclameaqui-backend/cron-debug.log") 2>&1
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ==="
echo "Inicio de ejecución del script desde: $0"

# Obtener el directorio donde se encuentra este script .sh
SHELL_DIR="$( cd "$( dirname "$0" )" && pwd )"
echo "SHELL_DIR: $SHELL_DIR"

# Definir las variables de ruta
SCRIPT_PATH="$SHELL_DIR/src/ReclameAqui/run_scraper.py"
LOG_PATH="$SHELL_DIR/logs/scraperReclameAqui_log_$(date +%Y%m%d).txt"

echo "SCRIPT_PATH: $SCRIPT_PATH"
echo "LOG_PATH: $LOG_PATH"

# Crear el directorio de logs si no existe
mkdir -p "$SHELL_DIR/logs"

# Activar el entorno virtual
source "$SHELL_DIR/env/bin/activate"
echo "Entorno virtual activado"

# cargando variables de entorno
if [ -f "$SHELL_DIR/.env" ]; then
    echo "Cargando variables de entorno desde $SHELL_DIR/.env"
    export $(grep -v '^#' "$SHELL_DIR/.env" | xargs)
else
    echo "No se encontró el archivo .env en $SHELL_DIR. Asegúrate de que existe."
fi

# Ejecutar el script Python y redirigir toda la salida al archivo de log
echo "Ejecutando el script Python..."
cd "$SHELL_DIR/src"

{
    echo "=== Inicio de ejecución del script Python (módulo) ==="
    python3 -m ReclameAqui.run_scraper 2>&1
    echo "=== Fin de ejecución del script Python ==="
    echo "Código de salida: $?"
} | tee -a "$LOG_PATH"

# Añadir una línea al final para saber que el script terminó
echo "Script completado el $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_PATH"
echo "=== Fin del script ==="
