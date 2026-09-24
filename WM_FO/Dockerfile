# Imagen base: Python 3.11 "slim" (ligera).
FROM python:3.11-slim

# Carpeta de trabajo dentro del contenedor.
WORKDIR /app

# Instalamos dependencias primero: Docker cachea este paso y no lo repite
# mientras requirements.txt no cambie.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo el proyecto (necesitamos utils/ y WM_FO/).
COPY . .

# Hace que los print() salgan al instante en los logs.
ENV PYTHONUNBUFFERED=1

# Se ejecuta con `docker compose run` (necesita consola interactiva -it
# para el menu). kafka_broker, id_operador y fichero via variables de entorno.
# Se usa "sh -c" para que el shell expanda las variables $KAFKA_BROKER, etc.
CMD ["sh", "-c", "python WM_FO/WM_FO.py \"$KAFKA_BROKER\" \"$OPERADOR_ID\" \"$ACTIVACIONES_FILE\""]