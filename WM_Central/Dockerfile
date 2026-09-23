FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo el proyecto para disponer de utils/
COPY . .

# .env esta en .gitignore y no llega a GitHub; el codigo solo lo usa
# como marcador para localizar la raiz del proyecto, asi que basta uno vacio.
RUN touch .env

EXPOSE 5000
EXPOSE 8080

ENV PYTHONUNBUFFERED=1

CMD ["python", "WM_Central/WM_Central.py"]