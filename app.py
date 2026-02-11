from flask import Flask, render_template, request, send_file, redirect, url_for
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

    # Base table
    c.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT NOT NULL,
            amount REAL NOT NULL
        )
    """)

    # Safely add new columns if they don't exist
    try:
        c.execute("ALTER TABLE invoices ADD COLUMN invoice_number TEXT")
    except sqlite3.OperationalError:
        pass

    try:
        c.execute("ALTER TABLE invoices ADD COLUMN created_at TEXT")
    except sqlite3.OperationalError:
        pass

    try:
        c.execute("ALTER TABLE invoices ADD COLUMN status TEXT DEFAULT 'Unpaid'")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()


init_db()


@app.route("/")
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

    c.execute("""
        INSERT INTO invoices (client, amount, invoice_number, created_at, status)
        VALUES (?, ?, ?, ?, ?)
    """, (client, amount, invoice_number, created_at, "Unpaid"))

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

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=LETTER)

    pdf.setFont("Helvetica-Bold", 20)
    pdf.drawString(72, 720, "Invoice")

    pdf.setFont("Helvetica", 12)
    pdf.drawString(72, 690, f"Client: {client}")
    pdf.drawString(72, 660, f"Amount Due: ${amount}")

    pdf.showPage()
    pdf.save()

    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name="invoice.pdf",
        mimetype="application/pdf"
    )


@app.route("/invoices")
def invoices():
    search = request.args.get("search", "")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    if search:
        c.execute("""
            SELECT id, invoice_number, client, amount, created_at, status
            FROM invoices
            WHERE client LIKE ?
            ORDER BY id DESC
        """, (f"%{search}%",))
    else:
        c.execute("""
            SELECT id, invoice_number, client, amount, created_at, status
            FROM invoices
            ORDER BY id DESC
        """)

    invoices = c.fetchall()
    conn.close()

    return render_template("invoices.html", invoices=invoices)


@app.route("/health")
def health():
    return "OK", 200


if __name__ == "__main__":
    app.run(debug=True)