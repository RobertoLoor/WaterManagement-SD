# Imagen base: Python 3.11 en su version "slim" (Debian minimo, imagen ligera).
FROM python:3.11-slim
 
# Carpeta de trabajo dentro del contenedor; los comandos siguientes se ejecutan aqui.
WORKDIR /app
 
# Copiamos primero SOLO requirements.txt e instalamos dependencias. Docker guarda cada
# paso en cache: si el codigo cambia pero requirements.txt no, este paso no se repite
# y el build es mucho mas rapido.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
# Copiamos todo el proyecto para disponer de utils/
# (el contexto de build es la raiz del repo, por eso hay acceso a utils/ y a WM_Central/).
COPY . .
 
# .env esta en .gitignore y no llega a GitHub; el codigo solo lo usa
# como marcador para localizar la raiz del proyecto, asi que basta uno vacio.
RUN touch .env
 
# EXPOSE es documentacion: indica que la app usa estos puertos.
# 5000 = sockets de los Monitores; 8080 = dashboard web.
EXPOSE 5000
EXPOSE 8080
 
# Fuerza que los print() salgan inmediatamente en los logs (sin buffer).
ENV PYTHONUNBUFFERED=1
 
# Comando que se ejecuta al arrancar el contenedor.
CMD ["python", "WM_Central/WM_Central.py"]