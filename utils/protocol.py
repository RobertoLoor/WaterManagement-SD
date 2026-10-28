# utils/protocol.py
"""
Protocolo de tramas STX/ETX/LRC sobre sockets TCP para delimitar mensajes.
Convierte texto a tramas con bytes de inicio, fin y comprobación de integridad XOR.
También maneja caracteres ASCII de control (ENQ, ACK, NACK, EOT) para la conexión.
"""

# Caracteres de control ASCII para delimitar tramas y diálogo en el socket
STX  = b'\x02'   # Inicio de trama
ETX  = b'\x03'   # Fin de trama
EOT  = b'\x04'   # Fin de transmisión / desconexión
ENQ  = b'\x05'   # Petición "¿estás listo?" / latido
ACK  = b'\x06'   # Confirmación positiva
NACK = b'\x15'   # Confirmación negativa / error en trama


# Calcula el byte de control LRC usando la operación XOR para verificar que el mensaje no se corrompa.
def calcular_lrc(datos: bytes) -> bytes:
    lrc = 0
    # Operación XOR (^=) bit a bit: acumula los bytes para detectar si algún bit cambió por fallo de red.
    for byte in datos:
        lrc ^= byte
    # Empaqueta el entero resultante en un único byte binario.
    return bytes([lrc])


# Empaqueta un texto en una trama binaria con formato: STX + texto + ETX + LRC.
def empaquetar_trama(mensaje: str) -> bytes:
    # Los sockets TCP solo transmiten bytes, por eso convertimos el string con encode('utf-8').
    payload_bytes = mensaje.encode('utf-8')
    lrc = calcular_lrc(payload_bytes)
    # Concatenamos el delimitador de inicio (STX), el contenido, el delimitador de fin (ETX) y el checksum.
    return STX + payload_bytes + ETX + lrc


# Desempaqueta y valida una trama comprobando su formato y su código de error LRC.
def desempaquetar_trama(trama: bytes):
    # La trama mínima debe tener al menos 3 bytes: STX + ETX + LRC.
    if len(trama) < 3 or trama[0:1] != STX:
        return False, ""
    try:
        # Busca la posición del byte ETX para saber dónde termina el texto útil.
        indice_etx = trama.index(ETX)
    except ValueError:
        return False, ""

    # Separa el contenido del mensaje y el byte LRC recibido (que va justo después del ETX).
    payload_bytes = trama[1:indice_etx]
    lrc_recibido = trama[indice_etx + 1: indice_etx + 2]
    lrc_calculado = calcular_lrc(payload_bytes)

    # Si el LRC calculado coincide con el recibido, el mensaje no ha sufrido alteraciones en el cable.
    if lrc_recibido == lrc_calculado:
        try:
            # Convierte los bytes de nuevo a string legible de Python.
            return True, payload_bytes.decode('utf-8')
        except UnicodeDecodeError:
            return False, ""
    return False, ""


# Lee exactamente un mensaje del socket TCP, ya sea un byte de control o una trama completa.
def recv_exact_frame(sock, timeout=None) -> bytes:
    # settimeout evita que el programa se quede congelado para siempre si el otro extremo no responde.
    if timeout is not None:
        sock.settimeout(timeout)

    # En TCP no hay concepto de 'mensaje completo', es un flujo continuo: leemos byte a byte (recv(1)).
    primero = sock.recv(1)
    if not primero:
        return b""

    # Si es un byte de control suelto (como ENQ o ACK), se devuelve inmediatamente sin esperar más.
    if primero in (ENQ, ACK, NACK, EOT):
        return primero

    # Si no empieza por STX ni es de control, es basura o ruido de la conexión y lo devolvemos tal cual.
    if primero != STX:
        return primero

    # Si empieza por STX, seguimos leyendo byte a byte hasta encontrar el byte de fin ETX.
    buf = primero
    while ETX not in buf:
        trozo = sock.recv(1)
        if not trozo:
            return buf
        buf += trozo

    # Leemos el último byte que acompaña a la trama, que es el código de verificación LRC.
    lrc = sock.recv(1)
    return buf + lrc
