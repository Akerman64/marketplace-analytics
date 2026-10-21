"""API Marketplace simulée (backoffice Maelys).

Endpoints (auth Bearer sauf /health) :
    GET /health
    GET /orders?date=YYYY-MM-DD
    GET /sellers?limit=N
    GET /products
    GET /customers?limit=N

API_FAILURE_RATE (défaut 0.05) renvoie aléatoirement des 503 pour simuler
une API instable et éprouver la logique de retry du Hook Airflow.
"""

import json
import os
import random
from datetime import date
from functools import lru_cache

from flask import Flask, Response, jsonify, request

import generator

API_VERSION = "1.0.0"
API_TOKEN = os.environ.get("API_TOKEN", "formation-token-2026")
FAILURE_RATE = float(os.environ.get("API_FAILURE_RATE", "0.05"))

app = Flask(__name__)


def _json(payload) -> Response:
    return Response(json.dumps(payload, ensure_ascii=False), mimetype="application/json")


@app.before_request
def _auth_and_chaos():
    if request.path == "/health":
        return None
    if request.headers.get("Authorization") != f"Bearer {API_TOKEN}":
        return jsonify(error="Unauthorized", detail="Bearer token manquant ou invalide"), 401
    if random.random() < FAILURE_RATE:
        return jsonify(error="Service temporarily unavailable"), 503
    return None


@app.get("/health")
def health():
    return jsonify(status="ok", version=API_VERSION)


@app.get("/orders")
def orders():
    raw = request.args.get("date")
    if not raw:
        return jsonify(error="Paramètre 'date' obligatoire (YYYY-MM-DD)"), 400
    try:
        day = date.fromisoformat(raw)
    except ValueError:
        return jsonify(error=f"Date invalide : {raw}"), 400
    data = generator.orders_for(day)
    return _json({"date": day.isoformat(), "count": len(data), "orders": data})


@app.get("/sellers")
def sellers():
    limit = request.args.get("limit", type=int)
    data = [generator.public(s) for s in generator.sellers()]
    if limit:
        data = data[:limit]
    return _json({"count": len(data), "sellers": data})


@lru_cache(maxsize=1)
def _products_payload() -> bytes:
    data = list(generator.products())
    return json.dumps({"count": len(data), "products": data}, ensure_ascii=False).encode("utf-8")


@app.get("/products")
def products():
    return Response(_products_payload(), mimetype="application/json")


@app.get("/customers")
def customers():
    limit = min(request.args.get("limit", default=100, type=int), 10_000)
    data = generator.customers(limit)
    return _json({"count": len(data), "customers": data})


# Pré-calcul du catalogue au démarrage du worker (premier appel rapide)
generator.sellers()
generator.products()
