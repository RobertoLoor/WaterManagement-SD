# WM_Central/dashboard.py
"""
Panel de monitorizacion en tiempo real de CENTRAL.
Se implementa como una app Flask muy ligera: la pagina hace polling cada
segundo al endpoint /api/state, suficiente para el objetivo de la practica
sin anadir dependencias de websockets.
"""
from flask import Flask, jsonify, request, render_template


def crear_app(db, fn_iniciar, fn_bloquear, fn_activar):
    app = Flask(__name__)

    @app.route("/")
    def index():
        return render_template("dashboard.html")

    @app.route("/api/state")
    def api_state():
        estaciones = db.get_todas_estaciones()
        eventos = db.get_ultimos_eventos(30)
        resumen = {"DISPONIBLE": 0, "REGANDO": 0, "FUGA": 0, "FUERA_DE_SERVICIO": 0, "DESCONECTADA": 0}
        for e in estaciones:
            resumen[e["estado"]] = resumen.get(e["estado"], 0) + 1
        return jsonify({"estaciones": estaciones, "eventos": eventos, "resumen": resumen})

    @app.route("/api/action", methods=["POST"])
    def api_action():
        data = request.get_json(force=True)
        accion = data.get("accion")
        id_ws = data.get("id_ws")

        if accion == "INICIAR":
            ok, msg = fn_iniciar(id_ws, int(data.get("duracion_seg", 60)))
        elif accion == "BLOQUEAR":
            ok, msg = fn_bloquear(id_ws)
        elif accion == "ACTIVAR":
            ok, msg = fn_activar(id_ws)
        else:
            return jsonify({"ok": False, "mensaje": "Accion desconocida."}), 400

        return jsonify({"ok": ok, "mensaje": msg})

    return app
