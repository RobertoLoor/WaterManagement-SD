# utils/db.py
"""
Capa de acceso a datos (SQLite) para WM_Central.

¿Por qué SQLite?
----------------
SQLite almacena toda la base de datos en un ÚNICO FICHERO del sistema operativo.
No requiere un proceso servidor separado (a diferencia de PostgreSQL o MySQL),
lo que simplifica el despliegue en Railway/Docker: basta con montar un volumen
persistente y apuntar DB_PATH a él.

Problema de concurrencia y solución:
-------------------------------------
WM_Central tiene MUCHOS hilos trabajando a la vez (servidor de sockets,
3 consumidores Kafka, hilo de broadcast y Flask). Todos usan esta misma clase
Database y comparten una única conexión SQLite. SQLite no es seguro para accesos
concurrentes sin coordinación, por lo que TODAS las operaciones de escritura y
lectura se envuelven en "with _lock:", garantizando que solo un hilo toca la
base de datos en cada instante.

Tablas gestionadas:
  - estaciones : una fila por estación de riego (estado, caudal, operario...).
  - operarios  : registro de los operarios (usuarios de WM_FO) que han interactuado.
  - eventos    : historial cronológico de sucesos para el panel de eventos del dashboard.

Estados posibles de una estación:
  DISPONIBLE, REGANDO, FUGA, FUERA_DE_SERVICIO, DESCONECTADA.
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

# Lock GLOBAL compartido por todas las instancias de Database en el proceso.
# ¿Por qué global y no de instancia?
# Aunque en la práctica solo existe una instancia (la que crea WM_Central.main()),
# se define como global porque la conexión SQLite también es única y global.
# Si fuera un lock de instancia, nada impediría crear dos instancias con dos locks
# distintos que se pisaran entre sí. Al ser global, no importa cuántas instancias
# haya: siempre se usa el mismo candado para la misma conexión.
_lock = threading.Lock()

# Script SQL que crea las tablas. "IF NOT EXISTS" hace que sea seguro
# ejecutarlo en cada arranque: solo crea lo que aun no existe.
ESQUEMA = """
CREATE TABLE IF NOT EXISTS estaciones (
    -- Clave primaria: identificador único de la estación, p. ej. 'WS-01'.
    -- Lo asigna el operador al arrancar el Monitor (argumento de línea de comandos).
    id_ws TEXT PRIMARY KEY,

    -- Descripción legible de dónde está instalada la estación, p. ej. 'River Park'.
    -- Se recibe en la trama de REGISTRO del Monitor.
    ubicacion TEXT,

    -- Estado operativo actual de la estación. Valores posibles:
    --   DISPONIBLE      : conectada y lista para recibir órdenes de riego.
    --   REGANDO         : riego en curso (electroválvula abierta).
    --   FUGA            : el Monitor detectó que el Engine responde KO o no responde.
    --   FUERA_DE_SERVICIO: bloqueada manualmente desde el dashboard de CENTRAL.
    --   DESCONECTADA    : el Monitor no está conectado a CENTRAL (estado inicial y de fallo).
    estado TEXT DEFAULT 'DESCONECTADA',

    -- Caudal instantáneo en litros/minuto, reportado por la telemetría del Engine.
    -- Se pone a 0 cuando el riego termina o la estación se desconecta.
    caudal REAL DEFAULT 0,

    -- Volumen acumulado en litros durante el riego actual.
    -- Se reinicia a 0 al empezar un nuevo riego.
    volumen REAL DEFAULT 0,

    -- Identificador del operario que inició el riego actual (o NULL si no hay riego).
    -- Puede ser un id de WM_FO o la cadena 'CENTRAL' si lo inició el dashboard.
    operario TEXT,

    -- Marca de tiempo UNIX (segundos desde 1970-01-01 00:00:00 UTC) de la última
    -- modificación de este registro. Permite ordenar o filtrar por antigüedad.
    ultima_actualizacion REAL
);

CREATE TABLE IF NOT EXISTS operarios (
    -- Identificador único del operario (p. ej. 'FO-01'). Clave primaria.
    id_operador TEXT PRIMARY KEY,

    -- Nombre legible del operario. Si no se proporciona, se usa el propio id.
    nombre TEXT
);

CREATE TABLE IF NOT EXISTS eventos (
    -- Número correlativo autoincremental: sirve para ordenar eventos cronológicamente
    -- (ORDER BY id DESC = del más reciente al más antiguo).
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Marca de tiempo UNIX del momento en que ocurrió el evento.
    timestamp REAL,

    -- Descripción en texto libre del evento, p. ej.:
    -- "WS-01 registrada en 'River Park' (a la espera de su Engine)."
    -- "Riego AUTORIZADO en WS-02 por FO-01 durante 60s."
    mensaje TEXT
);
"""


class Database:
    def __init__(self, ruta_bd: str):
        self.ruta_bd = ruta_bd
        # Crear la carpeta de la BD si no existe (p. ej. /app/data en Railway/Docker).
        # Sin esto, sqlite3 falla con "unable to open database file".
        # exist_ok=True evita el error si la carpeta ya existe (es idempotente).
        carpeta = os.path.dirname(os.path.abspath(ruta_bd))
        os.makedirs(carpeta, exist_ok=True)
        # check_same_thread=False permite usar esta conexion desde varios hilos
        # (por defecto SQLite lo prohibe); la seguridad la damos nosotros con _lock.
        self._conn = sqlite3.connect(ruta_bd, check_same_thread=False)
        # Con Row, cada fila se puede leer por nombre de columna (fila["estado"]).
        # Sin esto, las filas serían tuplas anónimas y habría que recordar la posición
        # de cada columna, lo que es frágil ante cambios de esquema.
        self._conn.row_factory = sqlite3.Row
        # Creamos las tablas si no existen y confirmamos (commit) los cambios.
        with _lock:
            self._conn.executescript(ESQUEMA)
            self._conn.commit()

    # ---------------------------------------------------------- estaciones
    def upsert_estacion_registro(self, id_ws: str, ubicacion: str):
        """
        Registra o actualiza una estación cuando su Monitor se conecta a CENTRAL.

        "Upsert" = INSERT + UPDATE en una sola operación SQL.
          - Si la estación NO existe: se inserta con estado DESCONECTADA.
          - Si ya existe (ON CONFLICT sobre la clave primaria id_ws): se actualiza
            la ubicación y se reinician estado, caudal, volumen y operario.

        ¿Por qué se pone DESCONECTADA y no DISPONIBLE?
        Porque en este punto solo el Monitor se ha registrado; aún no se sabe si su
        Engine está conectado. El estado pasa a DISPONIBLE solo cuando el Monitor
        notifica el evento CONECTADO a través de Kafka (hilo_estado_ws en CENTRAL).

        Los "?" son parámetros posicionales: SQLite los sustituye de forma segura,
        evitando inyección SQL (nunca concatenar texto en el SQL).
        """
        # "Upsert" = INSERT + UPDATE: si la estacion no existe se inserta; si ya existe
        # (ON CONFLICT sobre la clave primaria id_ws) se actualiza. Queda DESCONECTADA
        # hasta que el Monitor informe de que su Engine esta conectado (evento
        # CONECTADO): solo entonces se sabe que la estacion puede regar de verdad.
        # Los "?" son parametros: evitan inyeccion SQL (nunca concatenar texto en el SQL).
        with _lock:
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

    def set_estado(self, id_ws: str, estado: str, operario: str = None):
        """
        Actualiza el estado y el operario asociado de una estación.

        Se usa para transiciones de estado que no implican cambios de caudal/volumen:
        DISPONIBLE, FUGA, FUERA_DE_SERVICIO, DESCONECTADA. También se usa para
        marcar el inicio de un riego (REGANDO) junto con el operario que lo pidió.
        Si operario=None (valor por defecto), el campo queda a NULL en la BD,
        lo que indica que no hay ningún riego activo asociado a un operario.
        """
        # Cambia el estado de una estacion (y el operario asociado; None lo limpia).
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET estado=?, operario=?, ultima_actualizacion=?
                   WHERE id_ws=?""",
                (estado, operario, time.time(), id_ws),
            )
            self._conn.commit()

    def set_telemetria(self, id_ws: str, caudal: float, volumen: float):
        """
        Actualiza caudal y volumen de una estación con los datos del Engine.

        Esta función se llama cada segundo durante un riego activo, cuando llega
        un mensaje de tipo TELEMETRIA por Kafka desde el Engine.

        ¿Por qué el WHERE filtra por estado IN ('DISPONIBLE', 'REGANDO')?
        Protección frente a mensajes rezagados de Kafka: si CENTRAL acaba de
        poner la estación en FUGA, FUERA_DE_SERVICIO o DESCONECTADA, un mensaje
        de telemetría antiguo no debe "resucitar" la estación ni sobrescribir
        esos estados críticos. Solo se actualiza si la estación estaba en un
        estado "normal" de riego.
        """
        # Cada mensaje de telemetria del Engine actualiza caudal y volumen acumulado, y deja
        # la estacion en REGANDO (si esta llegando telemetria es que esta regando).
        # OJO: solo si estaba DISPONIBLE o ya REGANDO. Un mensaje rezagado no debe
        # "resucitar" una estacion que CENTRAL acaba de bloquear (FUERA_DE_SERVICIO),
        # que esta en FUGA o que esta DESCONECTADA.
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET estado='REGANDO', caudal=?, volumen=?,
                   ultima_actualizacion=?
                   WHERE id_ws=? AND estado IN ('DISPONIBLE', 'REGANDO')""",
                (caudal, volumen, time.time(), id_ws),
            )
            self._conn.commit()

    def finalizar_riego(self, id_ws: str):
        """
        Registra el fin de un riego: pone caudal y volumen a 0 y limpia el operario.

        El CASE WHEN en el UPDATE decide el nuevo estado:
          - Si estaba REGANDO  → pasa a DISPONIBLE (fin normal del riego).
          - Si estaba en otro estado (FUGA, FUERA_DE_SERVICIO, DESCONECTADA) →
            se respeta ese estado. El Engine puede enviar RIEGO_FINALIZADO incluso
            si CENTRAL ya cambió el estado por otra causa (fuga, bloqueo); en ese
            caso la estación no debe volver a DISPONIBLE por error.
        """
        # Fin de un riego: caudal a 0 y se quita el operario. La estacion vuelve a
        # DISPONIBLE solo si seguia REGANDO: si mientras tanto CENTRAL la bloqueo
        # (FUERA_DE_SERVICIO), hubo una FUGA o se desconecto, ese estado se respeta.
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET
                       estado=CASE WHEN estado='REGANDO' THEN 'DISPONIBLE' ELSE estado END,
                       caudal=0, volumen=0, operario=NULL,
                       ultima_actualizacion=? WHERE id_ws=?""",
                (time.time(), id_ws),
            )
            self._conn.commit()

    def marcar_desconectada(self, id_ws: str):
        # Atajo: reutiliza set_estado con el estado DESCONECTADA.
        self.set_estado(id_ws, 'DESCONECTADA')

    def marcar_todas_desconectadas(self):
        """
        Pone todas las estaciones en estado DESCONECTADA y resetea su telemetría.

        ¿Por qué se llama al arrancar WM_Central?
        Al reiniciar CENTRAL (por ejemplo, un redeploy en Railway), no hay forma de
        saber qué Monitores siguen activos y cuáles se cayeron mientras CENTRAL estuvo
        parada. Para evitar mostrar estados obsoletos en el dashboard, se parte de
        cero: todas las estaciones quedan DESCONECTADAS. A medida que cada Monitor
        se vuelva a registrar por sockets, su estado se actualizará correctamente.
        Los datos de identificación (id_ws y ubicacion) se CONSERVAN; solo se
        descarta la información de estado transitoria (estado, caudal, volumen, operario).
        """
        # Arranque de CENTRAL: hasta que una WS no se (re)registre no se conoce su
        # estado real, asi que se BORRA todo lo que se sabia de ella (estado, caudal,
        # volumen y operario) y queda DESCONECTADA. Las estaciones (id y ubicacion)
        # se conservan: lo que se descarta es el estado, no el alta.
        with _lock:
            self._conn.execute(
                """UPDATE estaciones SET estado='DESCONECTADA', caudal=0, volumen=0,
                   operario=NULL, ultima_actualizacion=?""",
                (time.time(),),
            )
            self._conn.commit()

    def get_estacion(self, id_ws: str):
        """
        Devuelve los datos de una estación concreta como diccionario, o None si no existe.

        El resultado es un dict con las claves del esquema (id_ws, ubicacion, estado,
        caudal, volumen, operario, ultima_actualizacion). Gracias a row_factory=sqlite3.Row
        la conversión a dict() es directa.
        """
        # Devuelve UNA estacion como diccionario, o None si no existe.
        with _lock:
            cur = self._conn.execute("SELECT * FROM estaciones WHERE id_ws=?", (id_ws,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_todas_estaciones(self):
        """
        Devuelve la lista completa de estaciones ordenada por id_ws.

        La usa el dashboard Flask (para mostrar la tabla) y hilo_broadcast
        (para publicar el estado global cada 2 segundos a los WM_FO).
        """
        # Devuelve la lista de todas las estaciones (para el dashboard y el broadcast).
        with _lock:
            cur = self._conn.execute("SELECT * FROM estaciones ORDER BY id_ws")
            return [dict(r) for r in cur.fetchall()]

    # ------------------------------------------------------------ eventos
    def log_evento(self, mensaje: str):
        """
        Registra un evento en la tabla 'eventos' y lo imprime en los logs.

        Los eventos son el historial de sucesos relevantes (riegos autorizados,
        fugas, desconexiones, etc.) que se muestran en el panel de eventos del
        dashboard. Se insertan con la marca de tiempo actual y el texto del evento.
        El prefijo [EVENTO] en los logs de Railway facilita filtrar estos mensajes.
        """
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
        """
        Registra un operario en la BD la primera vez que interactúa con el sistema.

        Usa ON CONFLICT DO NOTHING: si el operario ya existe, la instrucción no
        hace nada (no lanza error ni sobreescribe datos). Esto permite llamar a
        esta función en cada solicitud de riego sin tener que comprobar primero
        si el operario ya está dado de alta.
        Si no se proporciona nombre, se usa el propio id_operador como nombre
        (comportamiento por defecto razonable para la práctica).
        """
        # Registra un operario si no existe; si ya existe no hace nada
        # (DO NOTHING). Si no hay nombre, usa el propio id como nombre.
        with _lock:
            self._conn.execute(
                """INSERT INTO operarios (id_operador, nombre) VALUES (?, ?)
                   ON CONFLICT(id_operador) DO NOTHING""",
                (id_operador, nombre or id_operador),
            )
            self._conn.commit()
