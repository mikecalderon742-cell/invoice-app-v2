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


# ✅ IMPORTANT: initialize DB on import (works with Gunicorn)
init_db()


@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
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

        return f"""
        <h1>Invoice Saved</h1>
        <p><strong>Client:</strong> {client}</p>
        <p><strong>Amount:</strong> ${amount}</p>
        <a href="/">Create another invoice</a>
        """

    return render_template("index.html")


@app.route("/health")
def health():
    return "OK", 200