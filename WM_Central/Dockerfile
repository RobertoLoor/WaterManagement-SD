FROM python:3.11-slim

WORKDIR /app

# Copiar el archivo de dependencias e instalarlas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el proyecto para disponer de 'utils' y '.env'
COPY . .

# Puerto para Sockets
EXPOSE 5000

# Comando de ejecución pasando puerto de sockets y broker de Kafka como variables de entorno
CMD ["python", "WM_Central/WM_Central.py", "5000"]