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


# Initialize DB on import (important for Render)
init_db()


@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


# 🔹 PREVIEW ROUTE (THIS WAS MISSING)
@app.route("/preview", methods=["POST"])
def preview():
    client = request.form.get("client")
    amount = request.form.get("amount")

    return render_template(
        "preview.html",
        client=client,
        amount=amount
    )


# 🔹 SAVE INVOICE
@app.route("/save", methods=["POST"])
def save():
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

    return render_template(
        "saved.html",
        client=client,
        amount=amount
    )

@app.route("/pdf", methods=["POST"])
def pdf():
    client = request.form.get("client")
    amount = request.form.get("amount")

    return f"""
    <h1>PDF Generation Coming Next</h1>
    <p>Client: {client}</p>
    <p>Amount: ${amount}</p>
    <a href="/">Back to Home</a>
    """

# 🔹 INVOICE HISTORY
@app.route("/invoices")
def invoices():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, client, amount FROM invoices ORDER BY id DESC")
    invoices = c.fetchall()
    conn.close()

    return render_template("invoices.html", invoices=invoices)


# 🔹 RENDER HEALTH CHECK
@app.route("/health")
def health():
    return "OK", 200