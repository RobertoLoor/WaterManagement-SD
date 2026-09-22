# protocol.py

# 1. Definición de caracteres de control (en bytes para envío por red)
STX = b'\x02'  # Start of Text
ETX = b'\x03'  # End of Text
EOT = b'\x04'  # End of Transmission
ENQ = b'\x05'  # Enquiry
ACK = b'\x06'  # Acknowledge
NACK = b'\x15' # Negative Acknowledge

# 2. Estructura del Payload (Recordatorio)
# Formato: <CÓDIGO>#<PARAM_1>#<PARAM_2>
# Ejemplo: "REGISTRO#WS-04#River Park"

# 3. Implementación del algoritmo LRC
def calcular_lrc(datos: bytes) -> bytes:
    """Calcula el byte LRC haciendo un XOR iterativo de los datos."""
    lrc = 0
    for byte in datos:
        lrc ^= byte
    return bytes([lrc])

# 4. Función de empaquetado (Constructor de la trama)
def empaquetar_trama(mensaje: str) -> bytes:
    """Convierte un string en una trama de bytes: <STX><MENSAJE><ETX><LRC>"""
    payload_bytes = mensaje.encode('utf-8')
    lrc = calcular_lrc(payload_bytes)
    trama = STX + payload_bytes + ETX + lrc
    return trama

# 5. Función de desempaquetado y validación (Parser)
def desempaquetar_trama(trama: bytes) -> tuple[bool, str]:
    """
    Extrae el mensaje de la trama y valida el LRC.
    Retorna una tupla: (es_valido (booleano), mensaje_texto)
    """
    # Longitud mínima: STX (1) + ETX (1) + LRC (1) = 3 bytes
    if len(trama) < 3 or trama[0:1] != STX:
        return False, ""
        
    try:
        # Encontramos dónde está el ETX para separar el payload del LRC
        indice_etx = trama.index(ETX)
    except ValueError:
        return False, "" # Trama malformada, no hay ETX
        
    # Extraemos las partes
    payload_bytes = trama[1:indice_etx]
    lrc_recibido = trama[indice_etx + 1 : indice_etx + 2]
    
    # Recalculamos el LRC sobre el payload extraído
    lrc_calculado = calcular_lrc(payload_bytes)
    
    if lrc_recibido == lrc_calculado:
        return True, payload_bytes.decode('utf-8')
    else:
        return False, ""