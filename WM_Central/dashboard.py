# WM_Central/dashboard.py
"""
Servidor web con Flask para la monitorización en tiempo real del sistema.
Provee la interfaz visual en HTML y endpoints HTTP para consultar el estado
de las estaciones (polling) y ejecutar órdenes manuales de riego o bloqueo.
"""
from flask import Flask, jsonify, request, render_template


# Configura la aplicación web Flask inyectando la base de datos y las funciones de control.
def crear_app(db, fn_iniciar, fn_bloquear, fn_activar):
    app = Flask(__name__)

    # Ruta raíz: Flask busca por defecto la plantilla dashboard.html en la carpeta 'templates'.
    @app.route("/")
    def index():
        return render_template("dashboard.html")

    # Endpoint de consulta: el navegador pide esta URL periódicamente (polling) para refrescar datos.
    @app.route("/api/state")
    def api_state():
        estaciones = db.get_todas_estaciones()
        eventos = db.get_ultimos_eventos(30)
        # Contabiliza cuántas estaciones hay en cada estado para los contadores superiores de la web.
        resumen = {"DISPONIBLE": 0, "REGANDO": 0, "FUGA": 0, "FUERA_DE_SERVICIO": 0, "DESCONECTADA": 0}
        for e in estaciones:
            resumen[e["estado"]] = resumen.get(e["estado"], 0) + 1
        # jsonify convierte el diccionario a formato JSON y añade las cabeceras HTTP necesarias.
        return jsonify({"estaciones": estaciones, "eventos": eventos, "resumen": resumen})

    # Endpoint para recibir acciones enviadas desde los botones de la interfaz web mediante peticiones POST.
    @app.route("/api/action", methods=["POST"])
    def api_action():
        # force=True parsea el cuerpo como JSON aunque el navegador no envíe la cabecera Content-Type exacta.
        data = request.get_json(force=True) or {}
        accion = data.get("accion")
        id_ws = data.get("id_ws")

        if accion == "INICIAR":
            try:
                duracion = int(data.get("duracion_seg", 60))
            except (ValueError, TypeError):
                duracion = 60
            ok, msg = fn_iniciar(id_ws, duracion)
        elif accion == "BLOQUEAR":
            ok, msg = fn_bloquear(id_ws)
        elif accion == "ACTIVAR":
            ok, msg = fn_activar(id_ws)
        else:
            return jsonify({"ok": False, "mensaje": "Accion desconocida."}), 400

        return jsonify({"ok": ok, "mensaje": msg})

    return app
