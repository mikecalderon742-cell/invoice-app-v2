from flask import Flask, render_template, request
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


# IMPORTANT: initialize DB on startup (Render + Gunicorn safe)
init_db()


@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        client = request.form.get("client")
        amount = request.form.get("amount")
        return render_template("preview.html", client=client, amount=amount)

    return render_template("index.html")


@app.route("/save", methods=["POST"])
def save_invoice():
    client = request.form.get("client")
    amount = request.form.get("amount")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO invoices (client, amount) VALUES (?, ?)",
        (client, amount)
    )
    conn.commit()
    conn.close()

    return render_template("saved.html", client=client, amount=amount)


@app.route("/invoices")
def invoices():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, client, amount FROM invoices ORDER BY id DESC")
    invoices = c.fetchall()
    conn.close()

    return render_template("invoices.html", invoices=invoices)


@app.route("/health")
def health():
    return "OK", 200