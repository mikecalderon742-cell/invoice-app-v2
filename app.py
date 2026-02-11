from flask import Flask, render_template, request, send_file, redirect, url_for
import sqlite3
from pathlib import Path
import io
from datetime import datetime
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

app = Flask(__name__)

DB_PATH = Path("invoices.db")


# -----------------------------
# DATABASE SETUP
# -----------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_number TEXT,
            client TEXT NOT NULL,
            amount REAL NOT NULL,
            created_at TEXT
        )
    """)

    conn.commit()
    conn.close()


init_db()


# -----------------------------
# HOME
# -----------------------------
@app.route("/")
def home():
    return render_template("index.html")


# -----------------------------
# PREVIEW
# -----------------------------
@app.route("/preview", methods=["POST"])
def preview():
    client = request.form.get("client")
    amount = request.form.get("amount")

    return render_template(
        "preview.html",
        client=client,
        amount=amount
    )


# -----------------------------
# SAVE
# -----------------------------
@app.route("/save", methods=["POST"])
def save():
    client = request.form.get("client")
    amount = request.form.get("amount")

    invoice_number = datetime.now().strftime("%Y%m%d%H%M%S")
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        INSERT INTO invoices (invoice_number, client, amount, created_at)
        VALUES (?, ?, ?, ?)
    """, (invoice_number, client, amount, created_at))

    conn.commit()
    conn.close()

    return render_template(
        "saved.html",
        client=client,
        amount=amount,
        invoice_number=invoice_number,
        created_at=created_at
    )


# -----------------------------
# GENERATE PDF (from history or preview)
# -----------------------------
@app.route("/pdf/<invoice_number>")
def generate_pdf(invoice_number):

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        SELECT client, amount, created_at
        FROM invoices
        WHERE invoice_number = ?
    """, (invoice_number,))

    invoice = c.fetchone()
    conn.close()

    if not invoice:
        return "Invoice not found", 404

    client, amount, created_at = invoice

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=LETTER)

    pdf.setFont("Helvetica-Bold", 20)
    pdf.drawString(72, 720, "Invoice")

    pdf.setFont("Helvetica", 12)
    pdf.drawString(72, 690, f"Invoice #: {invoice_number}")
    pdf.drawString(72, 660, f"Date: {created_at}")
    pdf.drawString(72, 630, f"Client: {client}")
    pdf.drawString(72, 600, f"Amount Due: ${amount}")

    pdf.showPage()
    pdf.save()

    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"invoice_{invoice_number}.pdf",
        mimetype="application/pdf"
    )


# -----------------------------
# DELETE INVOICE
# -----------------------------
@app.route("/delete/<invoice_number>")
def delete(invoice_number):

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("DELETE FROM invoices WHERE invoice_number = ?", (invoice_number,))
    conn.commit()
    conn.close()

    return redirect(url_for("invoices"))


# -----------------------------
# INVOICE HISTORY
# -----------------------------
@app.route("/invoices")
def invoices():

    search = request.args.get("search")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    if search:
        c.execute("""
            SELECT invoice_number, client, amount, created_at
            FROM invoices
            WHERE client LIKE ?
            ORDER BY id DESC
        """, ('%' + search + '%',))
    else:
        c.execute("""
            SELECT invoice_number, client, amount, created_at
            FROM invoices
            ORDER BY id DESC
        """)

    invoices = c.fetchall()
    conn.close()

    return render_template("invoices.html", invoices=invoices)


# -----------------------------
# HEALTH CHECK
# -----------------------------
@app.route("/health")
def health():
    return "OK", 200


if __name__ == "__main__":
    app.run(debug=True)