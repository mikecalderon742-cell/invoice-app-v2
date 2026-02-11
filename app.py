from flask import Flask, render_template, request, send_file, redirect, url_for
import sqlite3
from pathlib import Path
from datetime import datetime
import io

from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

app = Flask(__name__)
DB_PATH = Path("invoices.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_number TEXT,
            client TEXT,
            amount REAL,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()


init_db()


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/preview", methods=["POST"])
def preview():
    client = request.form["client"]
    amount = request.form["amount"]

    invoice_number = datetime.now().strftime("%Y%m%d%H%M%S")
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return render_template(
        "preview.html",
        client=client,
        amount=amount,
        invoice_number=invoice_number,
        created_at=created_at
    )


@app.route("/save", methods=["POST"])
def save():
    client = request.form["client"]
    amount = request.form["amount"]
    invoice_number = request.form["invoice_number"]
    created_at = request.form["created_at"]

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
    client = request.form["client"]
    amount = request.form["amount"]
    invoice_number = request.form["invoice_number"]

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=LETTER)

    pdf.setFont("Helvetica-Bold", 20)
    pdf.drawString(72, 720, "Invoice")

    pdf.setFont("Helvetica", 12)
    pdf.drawString(72, 680, f"Invoice #: {invoice_number}")
    pdf.drawString(72, 650, f"Client: {client}")
    pdf.drawString(72, 620, f"Amount Due: ${amount}")

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
    invoices = conn.execute(
        "SELECT * FROM invoices ORDER BY id DESC"
    ).fetchall()
    conn.close()

    return render_template("invoices.html", invoices=invoices)


@app.route("/edit/<int:invoice_id>", methods=["GET", "POST"])
def edit(invoice_id):
    conn = get_db()
    c = conn.cursor()

    if request.method == "POST":
        client = request.form["client"]
        amount = request.form["amount"]
        c.execute(
            "UPDATE invoices SET client=?, amount=? WHERE id=?",
            (client, amount, invoice_id)
        )
        conn.commit()
        conn.close()
        return redirect(url_for("invoices"))

    invoice = c.execute(
        "SELECT * FROM invoices WHERE id=?",
        (invoice_id,)
    ).fetchone()
    conn.close()

    if not invoice:
        return "Invoice not found", 404

    return render_template("edit.html", invoice=invoice)


@app.route("/health")
def health():
    return "OK", 200