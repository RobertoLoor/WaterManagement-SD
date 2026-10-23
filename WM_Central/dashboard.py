# WM_Central/dashboard.py
"""
Panel de monitorización en tiempo real de WM_Central.

¿Qué es Flask?
--------------
Flask es un micro-framework web para Python. Permite definir "rutas" (URLs)
y las funciones Python que las atienden. Aquí se usa para servir el panel
de control del sistema WaterManagement.

¿Qué es el polling y por qué se usa en vez de WebSockets?
----------------------------------------------------------
El dashboard usa POLLING: la página HTML llama a /api/state cada segundo
mediante JavaScript (fetch) y actualiza la tabla con la respuesta JSON.
Se eligió polling frente a WebSockets por simplicidad:
  - No requiere librerías adicionales (Flask-SocketIO, etc.).
  - No hay gestión de conexiones persistentes.
  - 1 petición/segundo es suficiente para el objetivo de la práctica:
    ver el estado de las estaciones con latencia de ~1 s.
  - Flask en modo "threaded=True" puede atender varias peticiones de polling
    concurrentes sin bloqueos.

Rutas expuestas:
  GET  /            → devuelve la página HTML (templates/dashboard.html).
  GET  /api/state   → devuelve JSON con estaciones, eventos y resumen de contadores.
  POST /api/action  → recibe una acción del dashboard (INICIAR, BLOQUEAR, ACTIVAR)
                      y delega en las funciones correspondientes de WM_Central.
"""
from flask import Flask, jsonify, request, render_template

# ---------------------------------------------------------------------------
# RESUMEN PARA ESTUDIAR
# ---------------------------------------------------------------------------
# Flask es un framework web pequeno para Python. Este modulo expone 3 "rutas":
#   GET  /            -> devuelve la pagina HTML (templates/dashboard.html)
#   GET  /api/state   -> devuelve JSON con estaciones, eventos y un resumen
#   POST /api/action  -> recibe una orden (INICIAR / BLOQUEAR / ACTIVAR)
# La pagina llama a /api/state cada segundo y redibuja la tabla ("polling").
# ---------------------------------------------------------------------------


def crear_app(db, fn_iniciar, fn_bloquear, fn_activar):
    """
    Crea y configura la aplicación Flask del dashboard.

    Patrón de inyección de dependencias:
    ------------------------------------
    En vez de importar directamente el módulo WM_Central (lo que crearía una
    dependencia circular: WM_Central importa dashboard, dashboard importaría
    WM_Central), WM_Central le PASA a esta función:
      - db         : instancia de Database (acceso a SQLite).
      - fn_iniciar : función accion_iniciar_riego() de WM_Central.
      - fn_bloquear: función accion_bloquear() de WM_Central.
      - fn_activar : función accion_activar() de WM_Central.

    Así el dashboard no sabe nada de Kafka, sockets ni del resto del sistema:
    solo llama a las funciones que le pasan y devuelve el resultado al navegador.
    Este patrón también facilita las pruebas unitarias (se pueden inyectar mocks).

    :param db:         Instancia de utils.db.Database.
    :param fn_iniciar: Callable(id_ws, duracion_seg) -> (ok: bool, msg: str).
    :param fn_bloquear:Callable(id_ws) -> (ok: bool, msg: str).
    :param fn_activar: Callable(id_ws) -> (ok: bool, msg: str).
    :returns: Aplicación Flask configurada (llamar a app.run() para arrancarla).
    """
    # "Inyeccion de dependencias": en vez de importar WM_Central (lo que crearia una
    # dependencia circular), WM_Central le PASA aqui la base de datos y las tres
    # funciones que ejecutan las acciones. Asi el dashboard no sabe nada de Kafka.
    app = Flask(__name__)

    @app.route("/")
    def index():
        # Flask busca 'dashboard.html' en la carpeta 'templates/' junto a este fichero.
        # La convención de Flask es que las plantillas HTML viven en un subdirectorio
        # llamado "templates/" relativo al módulo donde se crea la app Flask.
        return render_template("dashboard.html")

    @app.route("/api/state")
    def api_state():
        # Lee de SQLite el estado actual de todas las estaciones y los ultimos 30 eventos.
        estaciones = db.get_todas_estaciones()
        eventos = db.get_ultimos_eventos(30)
        # Contador por estado (para las "chips" de la cabecera del panel).
        # Campos del JSON devuelto:
        #   estaciones : lista de dicts con los datos de cada estación (id_ws, ubicacion,
        #                estado, caudal, volumen, operario, ultima_actualizacion).
        #   eventos    : lista de los últimos 30 eventos del historial (timestamp, mensaje).
        #   resumen    : dict con el número de estaciones en cada estado posible.
        #                Lo usan las "chips" de colores en la cabecera del dashboard.
        resumen = {"DISPONIBLE": 0, "REGANDO": 0, "FUGA": 0, "FUERA_DE_SERVICIO": 0, "DESCONECTADA": 0}
        for e in estaciones:
            resumen[e["estado"]] = resumen.get(e["estado"], 0) + 1
        # jsonify convierte el diccionario a una respuesta HTTP en formato JSON.
        return jsonify({"estaciones": estaciones, "eventos": eventos, "resumen": resumen})

    @app.route("/api/action", methods=["POST"])
    def api_action():
        # force=True: interpreta el cuerpo como JSON aunque falte la cabecera
        # Content-Type: application/json. Útil para peticiones desde fetch() en
        # JavaScript que a veces omiten esa cabecera al usar el valor por defecto.
        data = request.get_json(force=True)
        accion = data.get("accion")
        id_ws = data.get("id_ws")

        # Segun la accion pedida por el boton, llamamos a la funcion correspondiente
        # (definidas en WM_Central.py). Cada una devuelve (ok, mensaje).
        # Patrón de delegación: el dashboard no implementa la lógica de negocio,
        # simplemente delega en las funciones fn_* que le inyectaron al crear la app.
        if accion == "INICIAR":
            ok, msg = fn_iniciar(id_ws, int(data.get("duracion_seg", 60)))
        elif accion == "BLOQUEAR":
            ok, msg = fn_bloquear(id_ws)
        elif accion == "ACTIVAR":
            ok, msg = fn_activar(id_ws)
        else:
            # 400 = "Bad Request": la accion no existe.
            return jsonify({"ok": False, "mensaje": "Accion desconocida."}), 400

        return jsonify({"ok": ok, "mensaje": msg})

    return app
