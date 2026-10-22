"""API Marketplace (backoffice Maelys) : la porte d'accès à la base MySQL du site.

Endpoints (auth Bearer sauf /health) :
    GET /health
    GET /orders?date=YYYY-MM-DD
    GET /sellers?limit=N
    GET /products
    GET /customers?limit=N

L'API lit tout dans la base MySQL du backoffice. Une journée jamais demandée
est d'abord enregistrée dans la base par le simulateur du site (db.record_day).

API_FAILURE_RATE (défaut 0.05) renvoie aléatoirement des 503 pour simuler
une API instable et éprouver la logique de retry du Hook Airflow.
"""

import json
import os
import random
from datetime import date

from flask import Flask, Response, jsonify, request

import db
import generator

API_VERSION = "2.0.0"
API_TOKEN = os.environ.get("API_TOKEN", "formation-token-2026")
FAILURE_RATE = float(os.environ.get("API_FAILURE_RATE", "0.05"))

app = Flask(__name__)


def _json(payload) -> Response:
    return Response(json.dumps(payload, ensure_ascii=False, default=str), mimetype="application/json")


def _query(sql: str, params: tuple = ()) -> list:
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


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
    try:
        _query("SELECT 1 AS ok")
        return jsonify(status="ok", version=API_VERSION, database="ok")
    except Exception as exc:  # base injoignable
        return jsonify(status="degraded", version=API_VERSION, database=str(exc)), 503


@app.get("/orders")
def orders():
    raw = request.args.get("date")
    if not raw:
        return jsonify(error="Paramètre 'date' obligatoire (YYYY-MM-DD)"), 400
    try:
        day = date.fromisoformat(raw)
    except ValueError:
        return jsonify(error=f"Date invalide : {raw}"), 400
    conn = db.connect()
    try:
        db.record_day(conn, day)
        data = db.read_orders(conn, day)
    finally:
        conn.close()
    return _json({"date": day.isoformat(), "count": len(data), "orders": data})


@app.get("/sellers")
def sellers():
    limit = request.args.get("limit", type=int)
    sql = ("SELECT seller_id, name, seller_type, country, city, joined_date, main_category, is_active "
           "FROM sellers ORDER BY seller_id")
    data = _query(sql + " LIMIT %s", (limit,)) if limit else _query(sql)
    for s in data:
        s["joined_date"] = s["joined_date"].isoformat()
        s["is_active"] = bool(s["is_active"])
    return _json({"count": len(data), "sellers": data})


@app.get("/products")
def products():
    data = _query("SELECT product_id, name, category, seller_id, price, created_at FROM products ORDER BY product_id")
    for p in data:
        p["price"] = float(p["price"])
        p["created_at"] = p["created_at"].isoformat()
    return _json({"count": len(data), "products": data})


@app.get("/customers")
def customers():
    limit = min(request.args.get("limit", default=100, type=int), 10_000)
    data = _query("SELECT customer_id, first_name, last_name, email, country, signup_date "
                  "FROM customers ORDER BY customer_id LIMIT %s", (limit,))
    for c in data:
        c["signup_date"] = c["signup_date"].isoformat()
    return _json({"count": len(data), "customers": data})


# Le simulateur a besoin du catalogue en mémoire pour générer une nouvelle journée
generator.sellers()
generator.products()
