from flask import Flask, render_template, request, send_file, redirect
from datetime import datetime, timedelta
import psycopg2
from urllib.parse import urlparse
from pathlib import Path
import os
from collections import defaultdict
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    result = urlparse(DATABASE_URL)
    conn = psycopg2.connect(
        dbname=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port
    )
    return conn

app = Flask(__name__)

DB_PATH = Path("invoices.db")


# -------------------------
# DATABASE INITIALIZATION
# -------------------------
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id SERIAL PRIMARY KEY,
            client TEXT NOT NULL,
            amount NUMERIC NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'Sent'
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS invoice_items (
            id SERIAL PRIMARY KEY,
            invoice_id INTEGER REFERENCES invoices(id) ON DELETE CASCADE,
            description TEXT,
            amount NUMERIC
        );
    """)

    conn.commit()
    cursor.close()
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

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
    "INSERT INTO invoices (client, amount, created_at, status) VALUES (?, ?, ?, ?)",
    (client, total, created_at, "Sent"),
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

    conn = get_connection_db_()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, client, amount, created_at, due_date, status FROM invoices ORDER BY created_at DESC"
    )
    invoices = cursor.fetchall()
    conn.close()

    # -------------------------
    # FILTER BY DATE RANGE
    # -------------------------
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
    # OVERDUE + DUE DATE LOGIC
    # -------------------------
    from datetime import date
    today = date.today()

    processed_invoices = []
    overdue_list = []

    for invoice in invoices:
        invoice_id, client, amount, created_at, due_date, status = invoice

        # --- Safe due_date handling ---
        if due_date:
            due_date_obj = datetime.strptime(due_date, "%Y-%m-%d").date()
        else:
            created_date_obj = datetime.strptime(
                created_at, "%Y-%m-%d %H:%M:%S"
            ).date()
            due_date_obj = created_date_obj + timedelta(days=14)

            # Persist generated due_date
            conn_fix = get_db_connection()
            c_fix = conn_fix.cursor()
            c_fix.execute(
                "UPDATE invoices SET due_date = ? WHERE id = ?",
                (due_date_obj.strftime("%Y-%m-%d"), invoice_id),
            )
            conn_fix.commit()
            conn_fix.close()

        # --- Overdue logic ---
        if status == "Sent" and due_date_obj < today:
            status = "Overdue"

            conn_update = get_db_connection()
            c_update = conn_update.cursor()
            c_update.execute(
                "UPDATE invoices SET status = ? WHERE id = ?",
                ("Overdue", invoice_id),
            )
            conn_update.commit()
            conn_update.close()

            overdue_list.append(invoice_id)

        processed_invoices.append(
            (
                invoice_id,
                client,
                amount,
                created_at,
                due_date_obj.strftime("%Y-%m-%d"),
                status,
            )
        )

    invoices = processed_invoices
    overdue_count = len(overdue_list)

    # -------------------------
    # ANALYTICS
    # -------------------------

    # Revenue Trend
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

    # Status Distribution (FIXED)
    status_distribution = {
        "Paid": sum(1 for inv in invoices if inv[5] == "Paid"),
        "Sent": sum(1 for inv in invoices if inv[5] == "Sent"),
        "Overdue": sum(1 for inv in invoices if inv[5] == "Overdue"),
    }

    # Top Clients
    client_totals = defaultdict(float)
    for invoice in invoices:
        client_totals[invoice[1]] += invoice[2]

    top_clients = sorted(
        client_totals.items(),
        key=lambda x: x[1],
        reverse=True,
    )[:5]

    # Monthly Revenue
    now_dt = datetime.now()
    current_year = now_dt.year
    current_month = now_dt.month

    monthly_revenue = sum(
        invoice[2]
        for invoice in invoices
        if (
            datetime.strptime(invoice[3], "%Y-%m-%d %H:%M:%S").month
            == current_month
            and datetime.strptime(invoice[3], "%Y-%m-%d %H:%M:%S").year
            == current_year
        )
    )

    # Previous Month Revenue
    if current_month == 1:
        prev_month = 12
        prev_year = current_year - 1
    else:
        prev_month = current_month - 1
        prev_year = current_year

    previous_month_revenue = sum(
        invoice[2]
        for invoice in invoices
        if (
            datetime.strptime(invoice[3], "%Y-%m-%d %H:%M:%S").month
            == prev_month
            and datetime.strptime(invoice[3], "%Y-%m-%d %H:%M:%S").year
            == prev_year
        )
    )

    if previous_month_revenue > 0:
        revenue_growth = round(
            ((monthly_revenue - previous_month_revenue)
             / previous_month_revenue) * 100,
            1,
        )
    else:
        revenue_growth = 100 if monthly_revenue > 0 else 0

    return render_template(
        "invoices.html",
        invoices=invoices,
        monthly_revenue=monthly_revenue,
        active_range=range_filter,
        revenue_trend=revenue_trend,
        status_distribution=status_distribution,
        top_clients=top_clients,
        overdue_list=overdue_list,
        overdue_count=overdue_count,
        revenue_growth=revenue_growth,
    )

# -------------------------
# EDIT
# -------------------------
@app.route("/edit/<int:invoice_id>")
def edit(invoice_id):
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("SELECT id, client FROM invoices WHERE id = %s", (invoice_id,))
    invoice = c.fetchone()

    if not invoice:
        conn.close()
        return "Invoice not found", 404

    c.execute("SELECT description, amount FROM invoice_items WHERE invoice_id = %s", (invoice_id,))
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

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        "UPDATE invoices SET client = ?, amount = ? WHERE id = ?",
        (client, total, invoice_id)
    )

    c.execute("DELETE FROM invoice_items WHERE invoice_id = %s", (invoice_id,))

    for desc, amt in cleaned_items:
        c.execute(
            "INSERT INTO invoice_items (invoice_id, description, amount) VALUES (?, ?, ?)",
            (invoice_id, desc, amt)
        )

    conn.commit()
    conn.close()

    return redirect("/invoices")

# -------------------------
# UPDATE STATUS
# -------------------------
@app.route("/update-status/<int:invoice_id>/<string:new_status>")
def update_status(invoice_id, new_status):
    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        "UPDATE invoices SET status = ? WHERE id = ?",
        (new_status, invoice_id)
    )

    conn.commit()
    conn.close()

    return redirect("/invoices")

# -------------------------
# DELETE
# -------------------------
@app.route("/delete/<int:invoice_id>")
def delete(invoice_id):
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("DELETE FROM invoice_items WHERE invoice_id = %s", (invoice_id,))
    c.execute("DELETE FROM invoices WHERE id = %s", (invoice_id,))

    conn.commit()
    conn.close()

    return redirect("/invoices")


# -------------------------
# PDF
# -------------------------
@app.route("/history-pdf/<int:invoice_id>")
def history_pdf(invoice_id):
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("SELECT client, amount FROM invoices WHERE id = %s", (invoice_id,))
    invoice = c.fetchone()

    if not invoice:
        conn.close()
        return "Invoice not found", 404

    client, total = invoice

    c.execute("SELECT description, amount FROM invoice_items WHERE invoice_id = %s", (invoice_id,))
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