FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo el proyecto para disponer de utils/ y .env
COPY . .

EXPOSE 5000
EXPOSE 8080

ENV PYTHONUNBUFFERED=1

CMD ["python", "WM_Central/WM_Central.py"]
