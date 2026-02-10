from flask import Flask, render_template, request, send_file
import sqlite3
from pathlib import Path
import io

from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

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


# 🔹 HOME
@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


# 🔹 PREVIEW
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


# 🔹 PDF GENERATION (STEP 5.4 — POLISHED LAYOUT)
@app.route("/pdf", methods=["POST"])
def pdf():
    client = request.form.get("client")
    amount = request.form.get("amount")

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=LETTER)

    width, height = LETTER

    # Title
    c.setFont("Helvetica-Bold", 24)
    c.drawString(1 * inch, height - 1.5 * inch, "Invoice")

    # Divider
    c.setLineWidth(2)
    c.line(
        1 * inch,
        height - 1.7 * inch,
        width - 1 * inch,
        height - 1.7 * inch
    )

    # Body
    c.setFont("Helvetica", 14)
    c.drawString(1 * inch, height - 2.6 * inch, f"Client: {client}")
    c.drawString(1 * inch, height - 3.3 * inch, f"Amount Due: ${amount}")

    c.showPage()
    c.save()

    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name="invoice.pdf",
        mimetype="application/pdf"
    )


# 🔹 INVOICE HISTORY
@app.route("/invoices")
def invoices():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, client, amount FROM invoices ORDER BY id DESC")
    invoices = c.fetchall()
    conn.close()

    return render_template("invoices.html", invoices=invoices)


# 🔹 HEALTH CHECK (RENDER)
@app.route("/health")
def health():
    return "OK", 200