from flask import Flask, render_template, request, send_file, redirect
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path
import io
from collections import defaultdict
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

app = Flask(__name__)

DB_PATH = Path("invoices.db")


# -------------------------
# DATABASE INITIALIZATION
# -------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

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


@app.context_processor
def inject_now():
    return {'now': datetime.now}


# -------------------------
# HOME
# -------------------------
@app.route("/")
def home():
    return render_template("index.html")


# -------------------------
# PREVIEW
# -------------------------
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


# -------------------------
# SAVE INVOICE
# -------------------------
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


# -------------------------
# INVOICES DASHBOARD
# -------------------------
@app.route("/invoices")
def invoices_page():
    range_filter = request.args.get("range", "30")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, client, amount, created_at FROM invoices ORDER BY created_at DESC")
    invoices = cursor.fetchall()
    conn.close()

    # Filter by date range
    if range_filter != "all":
        days = int(range_filter)
        cutoff = datetime.now() - timedelta(days=days)

        filtered = []
        for invoice in invoices:
            invoice_date = datetime.strptime(invoice[3], "%Y-%m-%d %H:%M:%S")
            if invoice_date >= cutoff:
                filtered.append(invoice)
        invoices = filtered
    # -------------------------
    # OVERDUE DETECTION (Phase 3A Fix)
    # -------------------------

    from datetime import date

    today = date.today()
    overdue_list = []

    processed_invoices = []

    for invoice in invoices:
        invoice_id, client, amount, created_at = invoice

        # Convert stored string to date object
        invoice_datetime = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
        invoice_date = invoice_datetime.date()

        # Since you don't have a status column yet,
        # we treat all invoices as unpaid for overdue logic
        if invoice_date < today:
            overdue_list.append(invoice_id)

        processed_invoices.append(
            (invoice_id, client, amount, created_at)
        )

    overdue_count = len(overdue_list)

    invoices = processed_invoices

    # -------------------------
    # ANALYTICS (NEW - STEP A)
    # -------------------------

    # 1. Revenue Trend (last 6 months)
    revenue_by_month = defaultdict(float)

    for invoice in invoices:
        invoice_date = datetime.strptime(invoice[3], "%Y-%m-%d %H:%M:%S")
        month_key = invoice_date.strftime("%Y-%m")
        revenue_by_month[month_key] += invoice[2]

    sorted_months = sorted(revenue_by_month.keys())
    revenue_trend = [
        {"month": month, "total": revenue_by_month[month]}
        for month in sorted_months
    ]

    # 2. Status Distribution (placeholder: all Paid)
    status_distribution = {
        "Paid": len(invoices)
    }

    # 3. Top Clients
    client_totals = defaultdict(float)
    for invoice in invoices:
        client_totals[invoice[1]] += invoice[2]

    top_clients = sorted(
        client_totals.items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]

    # 4. Monthly Revenue (current month)
    current_month = datetime.now().month
    monthly_revenue = sum(
        invoice[2] for invoice in invoices
        if datetime.strptime(invoice[3], "%Y-%m-%d %H:%M:%S").month == current_month
    )

    return render_template(
    "invoices.html",
    invoices=invoices,
    monthly_revenue=monthly_revenue,
    active_range=range_filter,
    revenue_trend=revenue_trend,
    status_distribution=status_distribution,
    top_clients=top_clients,
    overdue_list=overdue_list,
    overdue_count=overdue_count
)

# -------------------------
# EDIT
# -------------------------
@app.route("/edit/<int:invoice_id>")
def edit(invoice_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT id, client FROM invoices WHERE id = ?", (invoice_id,))
    invoice = c.fetchone()

    if not invoice:
        conn.close()
        return "Invoice not found", 404

    c.execute("SELECT description, amount FROM invoice_items WHERE invoice_id = ?", (invoice_id,))
    items = c.fetchall()

    conn.close()

    return render_template(
        "edit.html",
        invoice_id=invoice_id,
        client=invoice[1],
        items=items
    )


# -------------------------
# UPDATE
# -------------------------
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

    c.execute(
        "UPDATE invoices SET client = ?, amount = ? WHERE id = ?",
        (client, total, invoice_id)
    )

    c.execute("DELETE FROM invoice_items WHERE invoice_id = ?", (invoice_id,))

    for desc, amt in cleaned_items:
        c.execute(
            "INSERT INTO invoice_items (invoice_id, description, amount) VALUES (?, ?, ?)",
            (invoice_id, desc, amt)
        )

    conn.commit()
    conn.close()

    return redirect("/invoices")


# -------------------------
# DELETE
# -------------------------
@app.route("/delete/<int:invoice_id>")
def delete(invoice_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("DELETE FROM invoice_items WHERE invoice_id = ?", (invoice_id,))
    c.execute("DELETE FROM invoices WHERE id = ?", (invoice_id,))

    conn.commit()
    conn.close()

    return redirect("/invoices")


# -------------------------
# PDF
# -------------------------
@app.route("/history-pdf/<int:invoice_id>")
def history_pdf(invoice_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT client, amount FROM invoices WHERE id = ?", (invoice_id,))
    invoice = c.fetchone()

    if not invoice:
        conn.close()
        return "Invoice not found", 404

    client, total = invoice

    c.execute("SELECT description, amount FROM invoice_items WHERE invoice_id = ?", (invoice_id,))
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


@app.route("/health")
def health():
    return "OK", 200

import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)