# utils/db.py
"""
Capa de acceso a datos (SQLite) para WM_Central.
Unico responsable de tocar la base de datos; se protege con un lock
porque accede a ella tanto el hilo de sockets, los hilos consumidores
de Kafka como el hilo del dashboard web (Flask).
"""
import os          # rutas de ficheros y carpetas
import sqlite3     # base de datos SQLite (un fichero, sin servidor aparte)
import threading   # hilos: aqui se usa Lock para evitar accesos simultaneos
import time        # marcas de tiempo (time.time() = segundos desde 1970)
 
# ---------------------------------------------------------------------------
# RESUMEN PARA ESTUDIAR
# ---------------------------------------------------------------------------
# WM_Central tiene MUCHOS hilos trabajando a la vez (sockets, 3 consumidores
# Kafka, broadcast y Flask). Todos usan esta clase Database, y una sola conexion
# SQLite. Para que no se pisen, TODAS las operaciones se hacen dentro de
# "with _lock:", de modo que solo un hilo toca la BD cada vez.
#
# Tablas:
#   estaciones : una fila por estacion de riego (estado, caudal, operario...)
#   operarios  : operarios (usuarios de WM_FO) que han interactuado
#   eventos    : historial de cosas que ocurren (para el panel de eventos)
#
# Estados posibles de una estacion: DISPONIBLE, REGANDO, FUGA,
# FUERA_DE_SERVICIO y DESCONECTADA.
# ---------------------------------------------------------------------------
 
# Lock global compartido por todas las instancias: un unico hilo a la vez.
_lock = threading.Lock()
 
# Script SQL que crea las tablas. "IF NOT EXISTS" hace que sea seguro
# ejecutarlo en cada arranque: solo crea lo que aun no existe.
ESQUEMA = """
CREATE TABLE IF NOT EXISTS estaciones (
    id_ws TEXT PRIMARY KEY,                      -- identificador, p. ej. 'WS-01'
    ubicacion TEXT,                              -- p. ej. 'River Park'
    estado TEXT DEFAULT 'DESCONECTADA',          -- estado actual
    caudal REAL DEFAULT 0,                       -- litros/minuto en este momento
    volumen REAL DEFAULT 0,                      -- litros acumulados en el riego actual
    operario TEXT,                               -- quien ha iniciado el riego
    ultima_actualizacion REAL                    -- timestamp del ultimo cambio
);
 
CREATE TABLE IF NOT EXISTS operarios (
    id_operador TEXT PRIMARY KEY,
    nombre TEXT
);
 
CREATE TABLE IF NOT EXISTS eventos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,        -- numero creciente: ordena los eventos
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
        # check_same_thread=False permite usar esta conexion desde varios hilos
        # (por defecto SQLite lo prohibe); la seguridad la damos nosotros con _lock.
        self._conn = sqlite3.connect(ruta_bd, check_same_thread=False)
        # Con Row, cada fila se puede leer por nombre de columna (fila["estado"]).
        self._conn.row_factory = sqlite3.Row
        # Creamos las tablas si no existen y confirmamos (commit) los cambios.
        with _lock:
            self._conn.executescript(ESQUEMA)
            self._conn.commit()
 
    # ---------------------------------------------------------- estaciones
    def upsert_estacion_registro(self, id_ws: str, ubicacion: str):
        """Alta o actualizacion de una estacion cuando su Monitor se registra/conecta."""
        # "Upsert" = INSERT + UPDATE: si la estacion no existe se inserta; si ya existe
        # (ON CONFLICT sobre la clave primaria id_ws) se actualiza y pasa a DISPONIBLE.
        # Los "?" son parametros: evitan inyeccion SQL (nunca concatenar texto en el SQL).
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
        # Cambia el estado de una estacion (y el operario asociado; None lo limpia).
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET estado=?, operario=?, ultima_actualizacion=?
                   WHERE id_ws=?""",
                (estado, operario, time.time(), id_ws),
            )
            self._conn.commit()
 
    def set_telemetria(self, id_ws: str, caudal: float, volumen: float):
        # Cada mensaje de telemetria del Engine actualiza caudal y volumen, y deja
        # la estacion en REGANDO (si esta llegando telemetria es que esta regando).
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET estado='REGANDO', caudal=?, volumen=?,
                   ultima_actualizacion=? WHERE id_ws=?""",
                (caudal, volumen, time.time(), id_ws),
            )
            self._conn.commit()
 
    def finalizar_riego(self, id_ws: str):
        # Fin de un riego: vuelve a DISPONIBLE, caudal a 0 y se quita el operario.
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET estado='DISPONIBLE', caudal=0, operario=NULL,
                   ultima_actualizacion=? WHERE id_ws=?""",
                (time.time(), id_ws),
            )
            self._conn.commit()
 
    def marcar_desconectada(self, id_ws: str):
        # Atajo: reutiliza set_estado con el estado DESCONECTADA.
        self.set_estado(id_ws, 'DESCONECTADA')
 
    def get_estacion(self, id_ws: str):
        # Devuelve UNA estacion como diccionario, o None si no existe.
        with _lock:
            cur = self._conn.execute("SELECT * FROM estaciones WHERE id_ws=?", (id_ws,))
            row = cur.fetchone()
            return dict(row) if row else None
 
    def get_todas_estaciones(self):
        # Devuelve la lista de todas las estaciones (para el dashboard y el broadcast).
        with _lock:
            cur = self._conn.execute("SELECT * FROM estaciones ORDER BY id_ws")
            return [dict(r) for r in cur.fetchall()]
 
    # ------------------------------------------------------------ eventos
    def log_evento(self, mensaje: str):
        # Guarda un evento en la tabla (lo muestra el panel) y ademas lo imprime en
        # los logs (lo veras en Railway con el prefijo [EVENTO]).
        with _lock:
            self._conn.execute(
                "INSERT INTO eventos (timestamp, mensaje) VALUES (?, ?)",
                (time.time(), mensaje),
            )
            self._conn.commit()
        print(f"[EVENTO] {mensaje}")
 
    def get_ultimos_eventos(self, limite: int = 50):
        # Los N eventos mas recientes (ORDER BY id DESC = del mas nuevo al mas viejo).
        with _lock:
            cur = self._conn.execute(
                "SELECT * FROM eventos ORDER BY id DESC LIMIT ?", (limite,)
            )
            return [dict(r) for r in cur.fetchall()]
 
    # ----------------------------------------------------------- operarios
    def upsert_operario(self, id_operador: str, nombre: str = None):
        # Registra un operario si no existe; si ya existe no hace nada
        # (DO NOTHING). Si no hay nombre, usa el propio id como nombre.
        with _lock:
            self._conn.execute(
                """INSERT INTO operarios (id_operador, nombre) VALUES (?, ?)
                   ON CONFLICT(id_operador) DO NOTHING""",
                (id_operador, nombre or id_operador),
            )
            self._conn.commit()
 