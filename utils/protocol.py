# utils/protocol.py
"""
Protocolo de trama STX/ETX/LRC para comunicacion por sockets TCP.

¿Por qué existe este módulo?
-----------------------------
TCP es un protocolo de FLUJO DE BYTES: garantiza que los bytes llegan en orden
y sin duplicados, pero NO sabe nada de "mensajes". Si el emisor manda dos mensajes
seguidos, el receptor puede recibirlos fundidos en uno solo (o partidos en trozos).
Para solucionar esto, este módulo define un "protocolo de trama" que envuelve
cada mensaje entre marcas reconocibles:

    +-------+----------------------+-------+-----+
    |  STX  |  mensaje en texto    |  ETX  | LRC |
    | 0x02  |  "REGISTRO#WS-01"    | 0x03  | 1B  |
    +-------+----------------------+-------+-----+

Cómo funciona cada carácter de control:
  - STX (0x02, Start of Text) : marca el INICIO de una trama de datos.
  - ETX (0x03, End of Text)   : marca el FIN del contenido del mensaje.
  - LRC (1 byte tras ETX)     : checksum XOR para detectar corrupción en tránsito.

Además del formato de trama, el protocolo usa caracteres de "diálogo" de 1 byte
(sin STX/ETX) para el handshake y el control del canal:
  - ENQ (0x05, Enquiry)       : "¿estás ahí?" → se responde con ACK.
  - ACK (0x06, Acknowledge)   : "recibido correctamente".
  - NACK (0x15, Neg. Ack.)    : "hubo un error, por favor reenvía".
  - EOT (0x04, End of Trans.) : "cierro la conversación".
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
# Son valores estandar definidos en ASCII (ISO 646) desde los años 60 para
# controlar terminales y comunicaciones en serie.

# STX = 0x02: señal de inicio de trama. El receptor sabe que a partir de aquí
# empieza el contenido del mensaje y que debe acumularlo hasta ver ETX.
STX = b'\x02'   # Inicio de trama

# ETX = 0x03: señal de fin del contenido. Todo lo que hay entre STX y ETX
# es el mensaje propiamente dicho; el byte que viene justo después es el LRC.
ETX = b'\x03'   # Fin del mensaje

# EOT = 0x04: el emisor indica que ha terminado la sesión y que el receptor
# puede cerrar la conexión de forma ordenada.
EOT = b'\x04'   # Fin de transmision

# ENQ = 0x05: "enquiry" (consulta). Se usa como latido/ping de bajo nivel:
# el Monitor lo manda periódicamente a CENTRAL para demostrar que sigue vivo.
# CENTRAL responde ACK; si no responde, se da por caída.
ENQ = b'\x05'   # Peticion de "¿estas listo?"

# ACK = 0x06: confirmación positiva. Puede responder a un ENQ (estoy vivo)
# o a la recepción de una trama (el LRC es correcto, datos íntegros).
ACK = b'\x06'   # Confirmacion positiva

# NACK = 0x15: confirmación negativa. Se envía cuando el LRC calculado en
# el receptor no coincide con el recibido (la trama llegó corrupta).
# El emisor debería reenviar la trama al recibir un NACK.
NACK = b'\x15'  # Confirmacion negativa (error, hay que reenviar)


def calcular_lrc(datos: bytes) -> bytes:
    """
    Calcula el LRC (Longitudinal Redundancy Check) de un bloque de bytes.

    Algoritmo: XOR byte a byte de todos los bytes del bloque.
    ¿Por qué XOR?
      - Es reversible: aplicar XOR dos veces con el mismo valor devuelve el original.
      - Detecta cualquier bit que cambie en un número impar de bytes.
      - Es extremadamente rápido (una instrucción de CPU).
    Si el receptor recalcula el LRC sobre los datos recibidos y obtiene el mismo
    valor que el LRC que viajó en la trama, es muy probable que los datos no
    se hayan alterado durante la transmisión.

    :param datos: Los bytes del mensaje (SIN incluir STX ni ETX).
    :returns: Un único byte (envuelto en bytes) con el resultado del XOR acumulado.
    """
    # Empezamos con 0 y vamos haciendo XOR (^) con cada byte del mensaje.
    # Si el receptor repite el calculo y obtiene el mismo valor, es muy
    # probable que el mensaje no se haya alterado por el camino.
    lrc = 0
    for byte in datos:
        lrc ^= byte
    # Devolvemos el resultado como un unico byte.
    return bytes([lrc])


def empaquetar_trama(mensaje: str) -> bytes:
    """
    Construye una trama completa lista para enviar por el socket.

    Estructura resultante: <STX><mensaje_en_UTF-8><ETX><LRC>

    Pasos:
      1. Codificar el mensaje a bytes en UTF-8 (admite acentos, ñ, etc.).
      2. Calcular el LRC sobre esos bytes (sin STX ni ETX, que no forman
         parte del "contenido" a proteger).
      3. Concatenar: STX + payload_bytes + ETX + lrc.

    :param mensaje: Cadena de texto Python (str) a enviar.
    :returns: Bytes listos para pasar a socket.sendall().
    """
    # 1) Pasamos el texto a bytes (UTF-8, para admitir acentos, etc.).
    payload_bytes = mensaje.encode('utf-8')
    # 2) El LRC se calcula solo sobre el mensaje (sin STX ni ETX).
    lrc = calcular_lrc(payload_bytes)
    # 3) Montamos la trama completa concatenando las partes.
    return STX + payload_bytes + ETX + lrc


def desempaquetar_trama(trama: bytes):
    """
    Valida e interpreta una trama recibida del socket.

    Validaciones que se realizan:
      1. Longitud mínima: al menos 3 bytes (STX + ETX + LRC).
      2. El primer byte debe ser STX; si no, la trama está mal formada.
      3. Debe existir un byte ETX después del STX.
      4. El LRC recibido (byte tras ETX) debe coincidir con el LRC
         recalculado sobre el payload; si no, los datos llegaron corruptos.

    :param trama: Bytes crudos tal como los devuelve recv_exact_frame().
    :returns: Tupla (es_valido: bool, mensaje_texto: str).
              Si es_valido es False, mensaje_texto es cadena vacía.
    """
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
    Lee del socket hasta obtener una unidad completa de protocolo:
      - Una trama de datos:   <STX>...<ETX><LRC>  (varios bytes)
      - Un carácter de control suelto: ENQ, ACK, NACK o EOT  (1 byte)

    Devuelve b"" si la conexión se ha cerrado en el otro extremo.

    ¿Por qué se lee byte a byte?
    TCP puede fragmentar los datos en trozos arbitrarios; recibir byte a
    byte garantiza que nunca mezclaremos dos tramas ni nos quedaremos a
    mitad de una. El coste de rendimiento es despreciable (los mensajes
    son pequeños y la frecuencia es baja).

    :param sock:    Socket TCP ya conectado y en modo bloqueante.
    :param timeout: Segundos máximos de espera (None = sin límite).
                    Si expira, se lanza socket.timeout (útil en CENTRAL
                    para detectar Monitores mudos).
    :returns: Bytes con la trama o el carácter de control, o b"" si
              la conexión se cerró.
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
    # No hay más bytes que leer para ENQ/ACK/NACK/EOT: son mensajes de 1 byte.
    if primero in (ENQ, ACK, NACK, EOT):
        return primero

    # Caso 2: byte que no es ni control ni inicio de trama -> ruido, se descarta.
    # Devolvemos el byte para que quien llame pueda registrarlo o ignorarlo.
    if primero != STX:
        # Byte inesperado: se descarta
        return primero

    # Caso 3: empieza una trama. Vamos leyendo byte a byte hasta ver el ETX.
    # Acumulamos todo en 'buf', empezando por el STX ya leído.
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
