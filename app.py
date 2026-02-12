from flask import Flask, render_template, request, send_file, redirect
from datetime import datetime
import sqlite3
from pathlib import Path
import io
from datetime import datetime
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

app = Flask(__name__)
@app.context_processor
def inject_now():
    return {'now': datetime.now}

DB_PATH = Path("invoices.db")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Use amount column (this matches your live DB)
    c.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT NOT NULL,
            amount REAL NOT NULL,
            created_at TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS invoice_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER,
            description TEXT,
            amount REAL,
            FOREIGN KEY (invoice_id) REFERENCES invoices(id)
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
    client = request.form.get("client")
    descriptions = request.form.getlist("description")
    amounts = request.form.getlist("amount")

    items = []
    total = 0

    for desc, amt in zip(descriptions, amounts):
        if desc and amt:
            amt = float(amt)
            total += amt
            items.append((desc, amt))

    return render_template(
        "preview.html",
        client=client,
        items=items,
        total=total
    )


@app.route("/save", methods=["POST"])
def save():
    client = request.form.get("client")
    descriptions = request.form.getlist("description")
    amounts = request.form.getlist("amount")
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    total = 0
    cleaned_items = []

    for desc, amt in zip(descriptions, amounts):
        if desc and amt:
            amt = float(amt)
            total += amt
            cleaned_items.append((desc, amt))

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 🔥 INSERT INTO amount (NOT total)
    c.execute(
        "INSERT INTO invoices (client, amount, created_at) VALUES (?, ?, ?)",
        (client, total, created_at),
    )
    invoice_id = c.lastrowid

    for desc, amt in cleaned_items:
        c.execute(
            "INSERT INTO invoice_items (invoice_id, description, amount) VALUES (?, ?, ?)",
            (invoice_id, desc, amt),
        )

    conn.commit()
    conn.close()

    return redirect("/invoices")


@app.route("/history-pdf/<int:invoice_id>")
def history_pdf(invoice_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute(
        "SELECT client, amount FROM invoices WHERE id = ?",
        (invoice_id,)
    )
    invoice = c.fetchone()

    if not invoice:
        conn.close()
        return "Invoice not found", 404

    client, total = invoice

    c.execute(
        "SELECT description, amount FROM invoice_items WHERE invoice_id = ?",
        (invoice_id,)
    )
    items = c.fetchall()
    conn.close()

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=LETTER)

    pdf.setFont("Helvetica-Bold", 20)
    pdf.drawString(72, 720, "Invoice")

    pdf.setFont("Helvetica", 12)
    pdf.drawString(72, 690, f"Invoice ID: {invoice_id}")
    pdf.drawString(72, 660, f"Client: {client}")

    y = 620
    for desc, amt in items:
        pdf.drawString(72, y, f"{desc} - ${amt}")
        y -= 20

    pdf.drawString(72, y - 20, f"Total Due: ${total}")

    pdf.showPage()
    pdf.save()

    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"invoice_{invoice_id}.pdf",
        mimetype="application/pdf"
    )

@app.route("/edit/<int:invoice_id>", methods=["GET"])
def edit(invoice_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Get invoice
    c.execute(
        "SELECT id, client FROM invoices WHERE id = ?",
        (invoice_id,)
    )
    invoice = c.fetchone()

    if not invoice:
        conn.close()
        return "Invoice not found", 404

    # Get items
    c.execute(
        "SELECT description, amount FROM invoice_items WHERE invoice_id = ?",
        (invoice_id,)
    )
    items = c.fetchall()

    conn.close()

    return render_template(
        "edit.html",
        invoice_id=invoice_id,
        client=invoice[1],
        items=items
    )

@app.route("/update/<int:invoice_id>", methods=["POST"])
def update(invoice_id):
    client = request.form.get("client")
    descriptions = request.form.getlist("description")
    amounts = request.form.getlist("amount")

    total = 0
    cleaned_items = []

    for desc, amt in zip(descriptions, amounts):
        if desc and amt:
            amt = float(amt)
            total += amt
            cleaned_items.append((desc, amt))

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Update invoice total + client
    c.execute(
        "UPDATE invoices SET client = ?, amount = ? WHERE id = ?",
        (client, total, invoice_id)
    )

    # Delete old items
    c.execute("DELETE FROM invoice_items WHERE invoice_id = ?", (invoice_id,))

    # Insert updated items
    for desc, amt in cleaned_items:
        c.execute(
            "INSERT INTO invoice_items (invoice_id, description, amount) VALUES (?, ?, ?)",
            (invoice_id, desc, amt)
        )

    conn.commit()
    conn.close()

    return redirect("/invoices")

@app.route("/delete/<int:invoice_id>")
def delete(invoice_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("DELETE FROM invoice_items WHERE invoice_id = ?", (invoice_id,))
    c.execute("DELETE FROM invoices WHERE id = ?", (invoice_id,))

    conn.commit()
    conn.close()

    return redirect("/invoices")


from datetime import datetime

@app.route("/invoices")
def invoices():
    search = request.args.get("search")

    conn = sqlite3.connect("invoices.db")
    cursor = conn.cursor()

    if search:
        cursor.execute("SELECT * FROM invoices WHERE client LIKE ?", ('%' + search + '%',))
    else:
        cursor.execute("SELECT * FROM invoices")

    invoices = cursor.fetchall()
    conn.close()

    # ---- Monthly Calculations (Safe Python Logic) ----
    current_month = datetime.now().strftime("%Y-%m")

    monthly_invoices = [
        invoice for invoice in invoices
        if invoice[3].startswith(current_month)
    ]

    monthly_revenue = sum(invoice[2] for invoice in monthly_invoices)
    monthly_count = len(monthly_invoices)

    return render_template(
        "invoices.html",
        invoices=invoices,
        monthly_revenue=monthly_revenue,
        monthly_count=monthly_count
    )

@app.route("/health")
def health():
    return "OK", 200