from flask import Flask, render_template, request, redirect, url_for
import sqlite3
from pathlib import Path

app = Flask(__name__)

DB_PATH = Path("invoices.db")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT NOT NULL,
            amount REAL NOT NULL
        )
    """)
    conn.commit()
    conn.close()


# ✅ Initialize DB on import (important for Render / Gunicorn)
init_db()


# 🏠 HOME — shows form only
@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


# 👀 PREVIEW — shows invoice preview (no DB write)
@app.route("/preview", methods=["POST"])
def preview():
    client = request.form.get("client")
    amount = request.form.get("amount")

    return render_template(
        "preview.html",
        client=client,
        amount=amount
    )


# 💾 SAVE — writes invoice to DB
@app.route("/save", methods=["POST"])
def save_invoice():
    client = request.form.get("client")
    amount = request.form.get("amount")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO invoices (client, amount) VALUES (?, ?)",
        (client, amount),
    )
    conn.commit()
    conn.close()

    return redirect(url_for("invoices"))


# 📜 STEP 3.1.1 — Invoice History (READ ONLY)
@app.route("/invoices")
def invoices():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, client, amount FROM invoices ORDER BY id DESC")
    invoices = c.fetchall()
    conn.close()

    return render_template("invoices.html", invoices=invoices)


# ❤️ HEALTH CHECK (Render)
@app.route("/health")
def health():
    return "OK", 200