FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

# Se ejecuta con `docker compose run` (necesita consola interactiva -it
# para el menu). kafka_broker, id_operador y fichero via variables de entorno.
CMD ["sh", "-c", "python WM_FO/WM_FO.py \"$KAFKA_BROKER\" \"$OPERADOR_ID\" \"$ACTIVACIONES_FILE\""]
