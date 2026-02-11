from flask import Flask, render_template, request, redirect, url_for, send_file
import sqlite3
from pathlib import Path
from datetime import datetime
import io

from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

app = Flask(__name__)
DB_PATH = Path("invoices.db")


def get_db():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_db()
    c = conn.cursor()

    # Create base table if it doesn't exist
    c.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT,
            amount REAL
        )
    """)

    # Add new columns safely (won’t crash if they exist)
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


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/preview", methods=["POST"])
def preview():
    client = request.form.get("client")
    amount = request.form.get("amount")

    if not client or not amount:
        return redirect(url_for("home"))

    invoice_number = datetime.now().strftime("%Y%m%d%H%M%S")
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    return render_template(
        "preview.html",
        client=client,
        amount=amount,
        invoice_number=invoice_number,
        created_at=created_at
    )


@app.route("/save", methods=["POST"])
def save():
    client = request.form.get("client")
    amount = request.form.get("amount")
    invoice_number = request.form.get("invoice_number")
    created_at = request.form.get("created_at")

    if not all([client, amount, invoice_number, created_at]):
        return redirect(url_for("home"))

    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT INTO invoices (invoice_number, client, amount, created_at) VALUES (?, ?, ?, ?)",
        (invoice_number, client, amount, created_at)
    )
    conn.commit()
    conn.close()

    return render_template(
        "saved.html",
        client=client,
        amount=amount,
        invoice_number=invoice_number,
        created_at=created_at
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
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "SELECT invoice_number, client, amount, created_at FROM invoices ORDER BY id DESC"
    )
    invoices = c.fetchall()
    conn.close()

    return render_template("invoices.html", invoices=invoices)


@app.route("/health")
def health():
    return "OK", 200