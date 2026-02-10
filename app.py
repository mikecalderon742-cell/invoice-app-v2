from flask import Flask, render_template, request, send_file
import sqlite3
from pathlib import Path
import io
from datetime import datetime

from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

app = Flask(__name__)

DB_PATH = Path("invoices.db")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Base table
    c.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT NOT NULL,
            amount REAL NOT NULL
        )
    """)

    # Add new columns safely (no crash if they exist)
    try:
        c.execute("ALTER TABLE invoices ADD COLUMN invoice_number TEXT")
    except sqlite3.OperationalError:
        pass

    try:
        c.execute("ALTER TABLE invoices ADD COLUMN created_at TEXT")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()


# ✅ Initialize DB on import
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

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute(
        "SELECT id FROM invoices WHERE client = ? AND amount = ? ORDER BY id DESC LIMIT 1",
        (client, amount),
    )
    invoice = c.fetchone()
    conn.close()

    invoice_number = invoice[0] if invoice else "N/A"

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

@app.route("/edit/<int:invoice_id>", methods=["GET"])
def edit(invoice_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute(
        "SELECT id, client, amount, created_at FROM invoices WHERE id = ?",
        (invoice_id,)
    )
    invoice = c.fetchone()
    conn.close()

    if invoice is None:
        return "Invoice not found", 404

    return render_template("edit.html", invoice=invoice)

@app.route("/health")
def health():
    return "OK", 200