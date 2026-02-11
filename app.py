from flask import Flask, render_template, request, send_file
import sqlite3
import os
from datetime import datetime
import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import LETTER

app = Flask(__name__)

DB_PATH = "invoices.db"


def get_db():
    return sqlite3.connect(DB_PATH)


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
    created_at = request.form["created_at"]

    buffer = io.BytesIO()
    p = canvas.Canvas(buffer, pagesize=LETTER)

    p.setFont("Helvetica-Bold", 20)
    p.drawString(72, 720, "Invoice")

    p.setFont("Helvetica", 12)
    p.drawString(72, 690, f"Invoice #: {invoice_number}")
    p.drawString(72, 670, f"Date: {created_at}")
    p.drawString(72, 650, f"Client: {client}")
    p.drawString(72, 630, f"Amount Due: ${amount}")

    p.showPage()
    p.save()

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

    c.execute("""
        SELECT invoice_number, client, amount, created_at
        FROM invoices
        ORDER BY id DESC
    """)

    invoices = c.fetchall()
    conn.close()

    return render_template("invoices.html", invoices=invoices)

@app.route("/delete", methods=["POST"])
def delete():
    invoice_number = request.form["invoice_number"]

    conn = get_db()
    c = conn.cursor()

    c.execute("DELETE FROM invoices WHERE invoice_number = ?", (invoice_number,))
    conn.commit()
    conn.close()

    return render_template("deleted.html")

@app.route("/health")
def health():
    return "OK", 200


if __name__ == "__main__":
    app.run(debug=True)