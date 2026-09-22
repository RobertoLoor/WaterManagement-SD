# WM_Central/WM_Central.py
import socket
import threading
import sys
import os

# Buscar dinámicamente la raíz del proyecto (.env + utils/)
dir_cursor = os.path.dirname(os.path.abspath(__file__))

while True:
    utils_dir = os.path.join(dir_cursor, "utils")
    env_file = os.path.join(dir_cursor, ".env")

    if os.path.isdir(utils_dir) and os.path.isfile(env_file):
        if dir_cursor not in sys.path:
            sys.path.insert(0, dir_cursor)
        break

    parent = os.path.dirname(dir_cursor)

    if parent == dir_cursor:
        raise RuntimeError(
            "No se encontró la raíz del proyecto. Asegúrate de incluir la carpeta 'utils' y el archivo '.env'."
        )

    dir_cursor = parent


from utils.protocol import (
    empaquetar_trama,
    desempaquetar_trama,
    ENQ,
    ACK,
    NACK,
)


def atender_monitor(conn, addr):
    """Atiende a un Monitor (WM_WS_M) en un hilo dedicado."""
    print(f"[NUEVA CONEXIÓN] Monitor conectado desde {addr}")
    try:
        while True:
            datos = conn.recv(1024)
            if not datos:
                print(f"[DESCONEXIÓN] Monitor {addr} desconectado.")
                break

            # Solicitar canal
            if datos == ENQ:
                conn.sendall(ACK)
                continue

            # Validar la trama
            es_valido, mensaje = desempaquetar_trama(datos)
            if not es_valido:
                print(f"[ERROR] Trama corrupta recibida de {addr}")
                conn.sendall(NACK)
                continue

            # Confirmar recepción correcta
            conn.sendall(ACK)
            print(f"[MENSAJE RECIBIDO de {addr}]: {mensaje}")

            partes = mensaje.split('#')
            comando = partes[0]

            if comando == "REGISTRO":
                id_ws = partes[1] if len(partes) > 1 else "WS-DESCONOCIDO"
                ubicacion = partes[2] if len(partes) > 2 else "DESCONOCIDA"

                print(f"[REGISTRO ÉXITO] Estación '{id_ws}' en '{ubicacion}' dada de alta.")

                # Confirmación a nivel de aplicación
                respuesta = empaquetar_trama(f"REGISTRO_OK#{id_ws}")
                conn.sendall(respuesta)

    except Exception as e:
        print(f"[ERROR SOCKET {addr}]: {e}")
    finally:
        conn.close()


def iniciar_servidor(puerto):
    """Inicia el servidor de sockets de la Central."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('0.0.0.0', puerto))
    server_socket.listen()
    print(f"[WM_CENTRAL] Servidor de Sockets escuchando en el puerto {puerto}...")

    while True:
        conn, addr = server_socket.accept()
        hilo = threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True)
        hilo.start()


if __name__ == "__main__":
    puerto = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    iniciar_servidor(puerto)