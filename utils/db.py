# utils/db.py
"""
Capa de persistencia con SQLite para almacenar estaciones, operarios y eventos.
Utiliza un Lock global de threading para garantizar que múltiples hilos concurrentes
puedan leer y escribir en la base de datos sin corromperla.
"""
import os
import sqlite3
import threading
import time

# Lock global: evita que dos hilos escriban o lean a la vez en SQLite (condición de carrera).
_lock = threading.Lock()

ESQUEMA = """
CREATE TABLE IF NOT EXISTS estaciones (
    id_ws TEXT PRIMARY KEY,
    ubicacion TEXT,
    estado TEXT DEFAULT 'DESCONECTADA',
    caudal REAL DEFAULT 0,
    volumen REAL DEFAULT 0,
    operario TEXT,
    ultima_actualizacion REAL
);

CREATE TABLE IF NOT EXISTS operarios (
    id_operador TEXT PRIMARY KEY,
    nombre TEXT
);

CREATE TABLE IF NOT EXISTS eventos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL,
    mensaje TEXT
);
"""


class Database:

    # Inicializa la conexión SQLite y crea las tablas si todavía no existen en disco.
    def __init__(self, ruta_bd: str):
        self.ruta_bd = ruta_bd
        # Extrae la ruta de la carpeta y la crea en el sistema operativo si no existe para evitar fallos.
        carpeta = os.path.dirname(os.path.abspath(ruta_bd))
        os.makedirs(carpeta, exist_ok=True)
        # check_same_thread=False permite que varios hilos de Python compartan la misma conexión SQLite.
        self._conn = sqlite3.connect(ruta_bd, check_same_thread=False)
        # row_factory = sqlite3.Row permite acceder a los campos por su nombre de columna: fila['estado'].
        self._conn.row_factory = sqlite3.Row
        with _lock:
            # executescript ejecuta todas las sentencias SQL del bloque ESQUEMA de una vez.
            self._conn.executescript(ESQUEMA)
            self._conn.commit()

    # Registra una estación nueva o actualiza su ubicación y la marca como desconectada.
    def upsert_estacion_registro(self, id_ws: str, ubicacion: str):
        with _lock:
            # ON CONFLICT: si la estación ya existía por id_ws, actualiza sus campos en vez de dar error.
            self._conn.execute(
                """INSERT INTO estaciones (id_ws, ubicacion, estado, ultima_actualizacion)
                   VALUES (?, ?, 'DESCONECTADA', ?)
                   ON CONFLICT(id_ws) DO UPDATE SET
                       ubicacion=excluded.ubicacion,
                       estado='DESCONECTADA', caudal=0, volumen=0, operario=NULL,
                       ultima_actualizacion=excluded.ultima_actualizacion""",
                (id_ws, ubicacion, time.time()),
            )
            self._conn.commit()

    # Modifica el estado actual de una estación y el operario asignado en la base de datos.
    def set_estado(self, id_ws: str, estado: str, operario: str = None):
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET estado=?, operario=?, ultima_actualizacion=?
                   WHERE id_ws=?""",
                (estado, operario, time.time(), id_ws),
            )
            self._conn.commit()

    # Actualiza las lecturas de caudal y volumen acumulado de una estación durante el riego.
    def set_telemetria(self, id_ws: str, caudal: float, volumen: float):
        with _lock:
            # El filtro estado IN ('DISPONIBLE', 'REGANDO') evita que una telemetría vieja reactive una estación en avería.
            self._conn.execute(
                """UPDATE estaciones SET estado='REGANDO', caudal=?, volumen=?,
                   ultima_actualizacion=?
                   WHERE id_ws=? AND estado IN ('DISPONIBLE', 'REGANDO')""",
                (caudal, volumen, time.time(), id_ws),
            )
            self._conn.commit()

    # Da por terminado el riego reseteando caudal y volumen, devolviendo la estación a DISPONIBLE.
    def finalizar_riego(self, id_ws: str):
        with _lock:
            # CASE WHEN: solo pasa a DISPONIBLE si estaba REGANDO; si pasó a FUGA o BLOQUEO se respeta ese estado.
            self._conn.execute(
                """UPDATE estaciones SET
                       estado=CASE WHEN estado='REGANDO' THEN 'DISPONIBLE' ELSE estado END,
                       caudal=0, volumen=0, operario=NULL,
                       ultima_actualizacion=? WHERE id_ws=?""",
                (time.time(), id_ws),
            )
            self._conn.commit()

    # Cambia el estado de una estación concreta a DESCONECTADA.
    def marcar_desconectada(self, id_ws: str):
        self.set_estado(id_ws, 'DESCONECTADA')

    # Pone todas las estaciones en estado DESCONECTADA al arrancar el servidor central.
    def marcar_todas_desconectadas(self):
        with _lock:
            # Al reiniciar Central no sabemos el estado real de las estaciones hasta que vuelvan a conectarse.
            self._conn.execute(
                """UPDATE estaciones SET estado='DESCONECTADA', caudal=0, volumen=0,
                   operario=NULL, ultima_actualizacion=?""",
                (time.time(),),
            )
            self._conn.commit()

    # Consulta una estación por su identificador y devuelve sus datos como diccionario.
    def get_estacion(self, id_ws: str):
        with _lock:
            cur = self._conn.execute("SELECT * FROM estaciones WHERE id_ws=?", (id_ws,))
            row = cur.fetchone()
            # Convierte la fila de SQLite a un diccionario de Python estándar para usarlo fácilmente.
            return dict(row) if row else None

    # Devuelve una lista con todas las estaciones registradas en la base de datos.
    def get_todas_estaciones(self):
        with _lock:
            cur = self._conn.execute("SELECT * FROM estaciones ORDER BY id_ws")
            return [dict(r) for r in cur.fetchall()]

    # Guarda un evento con marca de tiempo en la tabla de historial y lo muestra en consola.
    def log_evento(self, mensaje: str):
        with _lock:
            self._conn.execute(
                "INSERT INTO eventos (timestamp, mensaje) VALUES (?, ?)",
                (time.time(), mensaje),
            )
            self._conn.commit()
        print(f"[EVENTO] {mensaje}")

    # Obtiene los eventos más recientes ordenados del más nuevo al más antiguo.
    def get_ultimos_eventos(self, limite: int = 50):
        with _lock:
            cur = self._conn.execute(
                "SELECT * FROM eventos ORDER BY id DESC LIMIT ?", (limite,)
            )
            return [dict(r) for r in cur.fetchall()]

    # Da de alta a un operario en la tabla si no existía previamente.
    def upsert_operario(self, id_operador: str, nombre: str = None):
        if not id_operador:
            return
        with _lock:
            # ON CONFLICT DO NOTHING: si el operario ya existía en la base de datos, simplemente lo ignora.
            self._conn.execute(
                """INSERT INTO operarios (id_operador, nombre) VALUES (?, ?)
                   ON CONFLICT(id_operador) DO NOTHING""",
                (id_operador, nombre or id_operador),
            )
            self._conn.commit()

    # Cierra de forma segura la conexión con el archivo de base de datos.
    def close(self):
        with _lock:
            if self._conn:
                try:
                    self._conn.close()
                except Exception:
                    pass
