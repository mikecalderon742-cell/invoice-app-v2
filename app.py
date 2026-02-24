from flask import Flask, render_template, request, send_file, redirect
from datetime import datetime, timedelta
import psycopg2
from urllib.parse import urlparse
from pathlib import Path
import os
import io
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
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

    # New: clients table (future-proofing)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            company TEXT,
            phone TEXT,
            address TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # New: extra columns on invoices (safe even if they already exist)
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS invoice_number TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS due_date TIMESTAMP;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS notes TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS terms TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS client_id INTEGER REFERENCES clients(id);")

    conn.commit()
    cursor.close()
    conn.close()


def update_overdue_statuses():
    """Mark invoices as Overdue when past due_date and not already Paid/Overdue."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE invoices
        SET status = 'Overdue'
        WHERE status NOT IN ('Paid', 'Overdue')
          AND due_date IS NOT NULL
          AND due_date < NOW();
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

    # Optional new fields (we can wire these into index.html later)
    notes = request.form.get("notes") or ""
    terms = request.form.get("terms") or "Payment due within 30 days."

    created_at = datetime.now()
    status = "Sent"

    # Default due date: 30 days from now
    due_date = created_at + timedelta(days=30)

    total = 0
    cleaned_items = []

    for desc, amt in zip(descriptions, amounts):
        if desc and amt:
            amt = float(amt)
            total += amt
            cleaned_items.append((desc, amt))

    conn = get_db_connection()
    cursor = conn.cursor()

    # Insert invoice and get its ID
    cursor.execute("""
        INSERT INTO invoices (client, amount, created_at, status, due_date, notes, terms)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (client, total, created_at, status, due_date, notes, terms))

    invoice_id = cursor.fetchone()[0]

    # Compute invoice_number based on ID, e.g. INV-00001
    invoice_number = f"INV-{invoice_id:05d}"
    cursor.execute(
        "UPDATE invoices SET invoice_number = %s WHERE id = %s",
        (invoice_number, invoice_id)
    )

    # Insert invoice items
    for desc, amt in cleaned_items:
        cursor.execute(
            "INSERT INTO invoice_items (invoice_id, description, amount) VALUES (%s, %s, %s)",
            (invoice_id, desc, amt),
        )

    conn.commit()
    cursor.close()
    conn.close()

    return redirect("/invoices")


# -------------------------
# INVOICES PAGE
# -------------------------
@app.route("/invoices")
def invoices_page():
    # First, auto-update overdue statuses
    update_overdue_statuses()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, client, amount, created_at, status, invoice_number, due_date
        FROM invoices
        ORDER BY created_at DESC
    """)
    invoices = cursor.fetchall()
    conn.close()

    # ---- STANDARDIZE NUMBERS (Convert Decimal to float) ----
    cleaned_invoices = []
    for invoice in invoices:
        invoice_list = list(invoice)
        invoice_list[2] = float(invoice_list[2])  # amount column
        cleaned_invoices.append(invoice_list)

    invoices = cleaned_invoices

    # ---- KPI CALCULATIONS ----
    total_invoices = len(invoices)
    total_revenue = sum(inv[2] for inv in invoices)

    now = datetime.now()
    current_month = now.month
    current_year = now.year

    monthly_revenue = sum(
        inv[2]
        for inv in invoices
        if inv[3].month == current_month and inv[3].year == current_year
    )

    growth = round((monthly_revenue / total_revenue) * 100, 1) if total_revenue > 0 else 0
    avg_invoice = round(total_revenue / total_invoices, 2) if total_invoices > 0 else 0

    paid_count = sum(1 for inv in invoices if inv[4] == "Paid")
    overdue_count = sum(1 for inv in invoices if inv[4] == "Overdue")

    # ---- STATUS DISTRIBUTION (FOR TEMPLATE) ----
    status_distribution = {
        "Paid": paid_count,
        "Sent": sum(1 for inv in invoices if inv[4] == "Sent"),
        "Overdue": overdue_count,
    }

    return render_template(
        "invoices.html",
        invoices=invoices,
        total_invoices=total_invoices,
        total_revenue=total_revenue,
        monthly_revenue=monthly_revenue,
        growth=growth,
        avg_invoice=avg_invoice,
        paid_count=paid_count,
        overdue_count=overdue_count,
        status_distribution=status_distribution
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

    c.execute(
        "SELECT description, amount FROM invoice_items WHERE invoice_id = %s",
        (invoice_id,),
    )
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
        "UPDATE invoices SET client = %s, amount = %s WHERE id = %s",
        (client, total, invoice_id)
    )

    c.execute(
        "DELETE FROM invoice_items WHERE invoice_id = %s",
        (invoice_id,)
    )

    for desc, amt in cleaned_items:
        c.execute(
            "INSERT INTO invoice_items (invoice_id, description, amount) VALUES (%s, %s, %s)",
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
        "UPDATE invoices SET status = %s WHERE id = %s",
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

    c.execute(
        "SELECT description, amount FROM invoice_items WHERE invoice_id = %s",
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


@app.route("/health")
def health():
    return "OK", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)