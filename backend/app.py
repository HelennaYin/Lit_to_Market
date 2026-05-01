"""Flask application entrypoint for the LitMarket API."""

from __future__ import annotations

from flask import Flask, jsonify

from backend.api.research import bp as research_bp
from backend.api.sectors import bp as sectors_bp
from backend.api.viral import bp as viral_bp
from backend.database import get_db_path, init_db
from backend.scheduler import start_scheduler


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["JSON_SORT_KEYS"] = False

    init_db()
    app.register_blueprint(research_bp)
    app.register_blueprint(sectors_bp)
    app.register_blueprint(viral_bp)
    start_scheduler()

    @app.get("/api/health")
    def health():
        return jsonify({"ok": True, "database": str(get_db_path())})

    @app.after_request
    def add_cors_headers(response):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return response

    @app.errorhandler(400)
    @app.errorhandler(404)
    def handle_client_error(error):
        return jsonify({"error": error.description}), error.code

    @app.errorhandler(500)
    def handle_server_error(error):
        return jsonify({"error": "Internal server error"}), 500

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
