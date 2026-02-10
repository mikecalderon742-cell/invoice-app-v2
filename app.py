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

    invoice_number = datetime.now().strftime("%Y%m%d%H%M%S")
    invoice_date = datetime.now().strftime("%B %d, %Y")

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=LETTER)
    width, height = LETTER

    # Title
    c.setFont("Helvetica-Bold", 24)
    c.drawString(1 * inch, height - 1.5 * inch, "Invoice")

    # Meta
    c.setFont("Helvetica", 11)
    c.drawString(1 * inch, height - 2.1 * inch, f"Invoice #: {invoice_number}")
    c.drawString(1 * inch, height - 2.4 * inch, f"Date: {invoice_date}")

    # Divider
    c.setLineWidth(2)
    c.line(
        1 * inch,
        height - 2.7 * inch,
        width - 1 * inch,
        height - 2.7 * inch
    )

    # Body
    c.setFont("Helvetica", 14)
    c.drawString(1 * inch, height - 3.6 * inch, f"Client: {client}")
    c.drawString(1 * inch, height - 4.3 * inch, f"Amount Due: ${amount}")

    c.showPage()
    c.save()

    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name="invoice.pdf",
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


@app.route("/health")
def health():
    return "OK", 200