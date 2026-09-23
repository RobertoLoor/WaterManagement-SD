# utils/protocol.py
"""
Protocolo estandar de sockets <STX><DATA><ETX><LRC> usado entre
WM_WS_M <-> WM_Central y WM_WS_M <-> WM_WS_E.
"""

STX = b'\x02'
ETX = b'\x03'
EOT = b'\x04'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'


def calcular_lrc(datos: bytes) -> bytes:
    """XOR byte a byte de todo el contenido para validar integridad."""
    lrc = 0
    for byte in datos:
        lrc ^= byte
    return bytes([lrc])


def empaquetar_trama(mensaje: str) -> bytes:
    """<STX><MENSAJE><ETX><LRC>"""
    payload_bytes = mensaje.encode('utf-8')
    lrc = calcular_lrc(payload_bytes)
    return STX + payload_bytes + ETX + lrc


def desempaquetar_trama(trama: bytes):
    """Devuelve (es_valido, mensaje_texto)."""
    if len(trama) < 3 or trama[0:1] != STX:
        return False, ""
    try:
        indice_etx = trama.index(ETX)
    except ValueError:
        return False, ""

    payload_bytes = trama[1:indice_etx]
    lrc_recibido = trama[indice_etx + 1: indice_etx + 2]
    lrc_calculado = calcular_lrc(payload_bytes)

    if lrc_recibido == lrc_calculado:
        return True, payload_bytes.decode('utf-8')
    return False, ""


def recv_exact_frame(sock, timeout=None) -> bytes:
    """
    Lee del socket hasta completar una trama <STX>...<ETX><LRC> o un
    caracter de control suelto (ENQ/ACK/NACK/EOT, 1 byte). Devuelve
    b"" si la conexion se cierra.
    """
    if timeout is not None:
        sock.settimeout(timeout)
    primero = sock.recv(1)
    if not primero:
        return b""
    if primero in (ENQ, ACK, NACK, EOT):
        return primero
    if primero != STX:
        # Byte inesperado: se descarta
        return primero
    buf = primero
    while ETX not in buf:
        trozo = sock.recv(1)
        if not trozo:
            return buf
        buf += trozo
    # falta el byte de LRC
    lrc = sock.recv(1)
    return buf + lrc
