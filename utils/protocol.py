# utils/protocol.py
"""
Protocolo estandar de sockets <STX><DATA><ETX><LRC> usado entre
WM_WS_M <-> WM_Central y WM_WS_M <-> WM_WS_E.
"""
 
# ---------------------------------------------------------------------------
# RESUMEN PARA ESTUDIAR
# ---------------------------------------------------------------------------
# Los sockets TCP solo transportan bytes, sin saber donde empieza o acaba un
# mensaje. Este modulo define un "protocolo de trama" para delimitarlos y
# comprobar que no se han corrompido:
#
#     STX  |  mensaje en texto  |  ETX  |  LRC
#     0x02 |  "REGISTRO#WS-01"  |  0x03 |  1 byte de control
#
#  - STX (Start of Text): marca el inicio de la trama.
#  - ETX (End of Text)  : marca el fin del mensaje.
#  - LRC (Longitudinal Redundancy Check): 1 byte para detectar errores.
#
# Ademas hay caracteres de control sueltos (1 byte) para el "dialogo":
#  - ENQ: "¿estas ahi?" (enquiry)  -> se responde con ACK
#  - ACK: "recibido correctamente"
#  - NACK: "recibido con errores, reenvia"
#  - EOT: "fin de la transmision", cierra la conversacion
# ---------------------------------------------------------------------------
 
# Caracteres de control del codigo ASCII, escritos como bytes (b'...').
STX = b'\x02'   # Inicio de trama
ETX = b'\x03'   # Fin del mensaje
EOT = b'\x04'   # Fin de transmision
ENQ = b'\x05'   # Peticion de "¿estas listo?"
ACK = b'\x06'   # Confirmacion positiva
NACK = b'\x15'  # Confirmacion negativa (error, hay que reenviar)
 
 
def calcular_lrc(datos: bytes) -> bytes:
    """XOR byte a byte de todo el contenido para validar integridad."""
    # Empezamos con 0 y vamos haciendo XOR (^) con cada byte del mensaje.
    # Si el receptor repite el calculo y obtiene el mismo valor, es muy
    # probable que el mensaje no se haya alterado por el camino.
    lrc = 0
    for byte in datos:
        lrc ^= byte
    # Devolvemos el resultado como un unico byte.
    return bytes([lrc])
 
 
def empaquetar_trama(mensaje: str) -> bytes:
    """<STX><MENSAJE><ETX><LRC>"""
    # 1) Pasamos el texto a bytes (UTF-8, para admitir acentos, etc.).
    payload_bytes = mensaje.encode('utf-8')
    # 2) El LRC se calcula solo sobre el mensaje (sin STX ni ETX).
    lrc = calcular_lrc(payload_bytes)
    # 3) Montamos la trama completa concatenando las partes.
    return STX + payload_bytes + ETX + lrc
 
 
def desempaquetar_trama(trama: bytes):
    """Devuelve (es_valido, mensaje_texto)."""
    # Una trama valida tiene como minimo STX + ETX + LRC (3 bytes) y debe
    # empezar por STX. Si no, se descarta.
    if len(trama) < 3 or trama[0:1] != STX:
        return False, ""
    try:
        # Buscamos la posicion del ETX: lo que hay entre STX y ETX es el mensaje.
        indice_etx = trama.index(ETX)
    except ValueError:
        # No hay ETX: la trama esta incompleta o corrupta.
        return False, ""
 
    # Separamos el mensaje y el LRC que envio el emisor (el byte tras ETX).
    payload_bytes = trama[1:indice_etx]
    lrc_recibido = trama[indice_etx + 1: indice_etx + 2]
    # Recalculamos el LRC nosotros a partir del mensaje recibido.
    lrc_calculado = calcular_lrc(payload_bytes)
 
    # Si coinciden, la trama es integra: devolvemos el mensaje ya como texto.
    if lrc_recibido == lrc_calculado:
        return True, payload_bytes.decode('utf-8')
    # Si no coinciden, hubo corrupcion: quien llame responde NACK.
    return False, ""
 
 
def recv_exact_frame(sock, timeout=None) -> bytes:
    """
    Lee del socket hasta completar una trama <STX>...<ETX><LRC> o un
    caracter de control suelto (ENQ/ACK/NACK/EOT, 1 byte). Devuelve
    b"" si la conexion se cierra.
    """
    # Si se indica timeout, el recv lanzara socket.timeout si el otro lado
    # no manda nada en ese tiempo (WM_Central lo usa con 120 s).
    if timeout is not None:
        sock.settimeout(timeout)
 
    # Leemos SOLO 1 byte para decidir que tipo de cosa llega.
    primero = sock.recv(1)
    if not primero:
        # recv devuelve b"" cuando el otro extremo cierra la conexion.
        return b""
 
    # Caso 1: es un caracter de control suelto -> se devuelve tal cual.
    if primero in (ENQ, ACK, NACK, EOT):
        return primero
 
    # Caso 2: byte que no es ni control ni inicio de trama -> ruido, se descarta.
    if primero != STX:
        # Byte inesperado: se descarta
        return primero
 
    # Caso 3: empieza una trama. Vamos leyendo byte a byte hasta ver el ETX.
    buf = primero
    while ETX not in buf:
        trozo = sock.recv(1)
        if not trozo:
            # Conexion cerrada a mitad de trama: devolvemos lo recibido.
            return buf
        buf += trozo
 
    # Tras el ETX llega un ultimo byte: el LRC, que tambien hay que leer.
    # falta el byte de LRC
    lrc = sock.recv(1)
    return buf + lrc
 