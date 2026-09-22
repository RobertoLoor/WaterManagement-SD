# WM_WS/WM_WS_M.py
import socket
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
)


def registrar_en_central(ip_central, puerto_central, id_ws, ubicacion):
    """Se conecta a WM_Central y envía la trama de registro por sockets."""
    try:
        cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente_socket.connect((ip_central, puerto_central))
        print(f"[MONITOR {id_ws}] Conectado a Central en {ip_central}:{puerto_central}")

        # 1. Enviar ENQ
        cliente_socket.sendall(ENQ)
        respuesta = cliente_socket.recv(1024)

        if respuesta == ACK:
            # 2. Enviar trama de registro
            mensaje = f"REGISTRO#{id_ws}#{ubicacion}"
            trama = empaquetar_trama(mensaje)
            cliente_socket.sendall(trama)

            # Confirmación de recepción (ACK)
            ack_respuesta = cliente_socket.recv(1024)
            if ack_respuesta == ACK:
                # Respuesta de la aplicación
                trama_respuesta = cliente_socket.recv(1024)
                valido, msg_respuesta = desempaquetar_trama(trama_respuesta)
                if valido:
                    print(f"[MONITOR {id_ws}] Respuesta de Central: {msg_respuesta}")
                else:
                    print(f"[MONITOR {id_ws}] Error en trama de respuesta.")

        cliente_socket.close()

    except Exception as e:
        print(f"[ERROR MONITOR {id_ws}]: No se pudo conectar con la Central ({e})")


if __name__ == "__main__":
    ip_central = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    puerto_central = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    id_ws = sys.argv[3] if len(sys.argv) > 3 else "WS-01"
    ubicacion = sys.argv[4] if len(sys.argv) > 4 else "Parque Canalejas"

    registrar_en_central(ip_central, puerto_central, id_ws, ubicacion)