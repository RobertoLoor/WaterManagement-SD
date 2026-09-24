# WM_Central/dashboard.py
"""
Panel de monitorizacion en tiempo real de CENTRAL.
Se implementa como una app Flask muy ligera: la pagina hace polling cada
segundo al endpoint /api/state, suficiente para el objetivo de la practica
sin anadir dependencias de websockets.
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
    # "Inyeccion de dependencias": en vez de importar WM_Central (lo que crearia una
    # dependencia circular), WM_Central le PASA aqui la base de datos y las tres
    # funciones que ejecutan las acciones. Asi el dashboard no sabe nada de Kafka.
    app = Flask(__name__)
 
    @app.route("/")
    def index():
        # Flask busca 'dashboard.html' en la carpeta 'templates/' junto a este fichero.
        return render_template("dashboard.html")
 
    @app.route("/api/state")
    def api_state():
        # Lee de SQLite el estado actual de todas las estaciones y los ultimos 30 eventos.
        estaciones = db.get_todas_estaciones()
        eventos = db.get_ultimos_eventos(30)
        # Contador por estado (para las "chips" de la cabecera del panel).
        resumen = {"DISPONIBLE": 0, "REGANDO": 0, "FUGA": 0, "FUERA_DE_SERVICIO": 0, "DESCONECTADA": 0}
        for e in estaciones:
            resumen[e["estado"]] = resumen.get(e["estado"], 0) + 1
        # jsonify convierte el diccionario a una respuesta HTTP en formato JSON.
        return jsonify({"estaciones": estaciones, "eventos": eventos, "resumen": resumen})
 
    @app.route("/api/action", methods=["POST"])
    def api_action():
        # force=True: interpreta el cuerpo como JSON aunque falte la cabecera Content-Type.
        data = request.get_json(force=True)
        accion = data.get("accion")
        id_ws = data.get("id_ws")
 
        # Segun la accion pedida por el boton, llamamos a la funcion correspondiente
        # (definidas en WM_Central.py). Cada una devuelve (ok, mensaje).
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