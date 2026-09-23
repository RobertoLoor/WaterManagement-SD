# utils/db.py
"""
Capa de acceso a datos (SQLite) para WM_Central.
Unico responsable de tocar la base de datos; se protege con un lock
porque accede a ella tanto el hilo de sockets, los hilos consumidores
de Kafka como el hilo del dashboard web (Flask).
"""
import os
import sqlite3
import threading
import time
 
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
    def __init__(self, ruta_bd: str):
        self.ruta_bd = ruta_bd
        # Crear la carpeta de la BD si no existe (p. ej. /app/data en Railway/Docker).
        # Sin esto, sqlite3 falla con "unable to open database file".
        carpeta = os.path.dirname(os.path.abspath(ruta_bd))
        os.makedirs(carpeta, exist_ok=True)
        self._conn = sqlite3.connect(ruta_bd, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with _lock:
            self._conn.executescript(ESQUEMA)
            self._conn.commit()
 
    # ---------------------------------------------------------- estaciones
    def upsert_estacion_registro(self, id_ws: str, ubicacion: str):
        """Alta o actualizacion de una estacion cuando su Monitor se registra/conecta."""
        with _lock:
            self._conn.execute(
                """INSERT INTO estaciones (id_ws, ubicacion, estado, ultima_actualizacion)
                   VALUES (?, ?, 'DISPONIBLE', ?)
                   ON CONFLICT(id_ws) DO UPDATE SET
                       ubicacion=excluded.ubicacion,
                       estado='DISPONIBLE',
                       ultima_actualizacion=excluded.ultima_actualizacion""",
                (id_ws, ubicacion, time.time()),
            )
            self._conn.commit()
 
    def set_estado(self, id_ws: str, estado: str, operario: str = None):
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET estado=?, operario=?, ultima_actualizacion=?
                   WHERE id_ws=?""",
                (estado, operario, time.time(), id_ws),
            )
            self._conn.commit()
 
    def set_telemetria(self, id_ws: str, caudal: float, volumen: float):
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET estado='REGANDO', caudal=?, volumen=?,
                   ultima_actualizacion=? WHERE id_ws=?""",
                (caudal, volumen, time.time(), id_ws),
            )
            self._conn.commit()
 
    def finalizar_riego(self, id_ws: str):
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET estado='DISPONIBLE', caudal=0, operario=NULL,
                   ultima_actualizacion=? WHERE id_ws=?""",
                (time.time(), id_ws),
            )
            self._conn.commit()
 
    def marcar_desconectada(self, id_ws: str):
        self.set_estado(id_ws, 'DESCONECTADA')
 
    def get_estacion(self, id_ws: str):
        with _lock:
            cur = self._conn.execute("SELECT * FROM estaciones WHERE id_ws=?", (id_ws,))
            row = cur.fetchone()
            return dict(row) if row else None
 
    def get_todas_estaciones(self):
        with _lock:
            cur = self._conn.execute("SELECT * FROM estaciones ORDER BY id_ws")
            return [dict(r) for r in cur.fetchall()]
 
    # ------------------------------------------------------------ eventos
    def log_evento(self, mensaje: str):
        with _lock:
            self._conn.execute(
                "INSERT INTO eventos (timestamp, mensaje) VALUES (?, ?)",
                (time.time(), mensaje),
            )
            self._conn.commit()
        print(f"[EVENTO] {mensaje}")
 
    def get_ultimos_eventos(self, limite: int = 50):
        with _lock:
            cur = self._conn.execute(
                "SELECT * FROM eventos ORDER BY id DESC LIMIT ?", (limite,)
            )
            return [dict(r) for r in cur.fetchall()]
 
    # ----------------------------------------------------------- operarios
    def upsert_operario(self, id_operador: str, nombre: str = None):
        with _lock:
            self._conn.execute(
                """INSERT INTO operarios (id_operador, nombre) VALUES (?, ?)
                   ON CONFLICT(id_operador) DO NOTHING""",
                (id_operador, nombre or id_operador),
            )
            self._conn.commit()