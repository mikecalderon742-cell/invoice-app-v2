from flask import Flask, render_template, request, send_file
import sqlite3
from pathlib import Path
import io
from datetime import datetime

from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

app = Flask(__name__)

DB_PATH = Path("invoices.db")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT NOT NULL,
            amount REAL NOT NULL,
            invoice_number TEXT,
            created_at TEXT
        )
    """)

    conn.commit()
    conn.close()


# ✅ Initialize DB on import (Render-safe)
init_db()


@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


@app.route("/preview", methods=["POST"])
def preview():
    client = request.form.get("client")
    amount = request.form.get("amount")

    return render_template(
        "preview.html",
        client=client,
        amount=amount
    )


@app.route("/save", methods=["POST"])
def save():
    client = request.form.get("client")
    amount = request.form.get("amount")

    invoice_number = datetime.now().strftime("%Y%m%d%H%M%S")
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO invoices (client, amount, invoice_number, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (client, amount, invoice_number, created_at),
    )
    conn.commit()
    conn.close()

    return render_template(
        "saved.html",
        client=client,
        amount=amount,
        invoice_number=invoice_number
    )


@app.route("/pdf", methods=["POST"])
def pdf():
    client = request.form.get("client")
    amount = request.form.get("amount")
    invoice_number = request.form.get("invoice_number")

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=LETTER)

    pdf.setFont("Helvetica-Bold", 20)
    pdf.drawString(72, 720, "Invoice")

    pdf.setFont("Helvetica", 12)
    pdf.drawString(72, 690, f"Invoice #: {invoice_number}")
    pdf.drawString(72, 660, f"Client: {client}")
    pdf.drawString(72, 630, f"Amount Due: ${amount}")

    pdf.showPage()
    pdf.save()
    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"invoice_{invoice_number}.pdf",
        mimetype="application/pdf"
    )


@app.route("/invoices")
def invoices():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT invoice_number, client, amount, created_at
        FROM invoices
        ORDER BY id DESC
    """)
    invoices = c.fetchall()
    conn.close()

    return render_template("invoices.html", invoices=invoices)


# ✅ STEP 6.1.3 — EDIT BY INVOICE NUMBER (FIXED)
@app.route("/edit/<invoice_number>")
def edit(invoice_number):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute(
        """
        SELECT invoice_number, client, amount
        FROM invoices
        WHERE invoice_number = ?
        """,
        (invoice_number,)
    )
    invoice = c.fetchone()
    conn.close()

    if invoice is None:
        return "Invoice not found", 404

    return render_template(
        "edit.html",
        invoice_number=invoice[0],
        client=invoice[1],
        amount=invoice[2]
    )


@app.route("/health")
def health():
    return "OK", 200