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

    # Base invoices + items tables
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

    # New: clients table
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

    # New: extra columns on invoices (safe if already exist)
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS invoice_number TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS due_date TIMESTAMP;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS notes TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS terms TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS client_id INTEGER REFERENCES clients(id);")

    conn.commit()
    cursor.close()
    conn.close()


def update_overdue_statuses():
    """
    Mark invoices as Overdue when past due_date and not already Paid/Overdue.
    Runs each time the invoices page is loaded.
    """
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
# HOME (NEW INVOICE FORM)
# -------------------------
@app.route("/")
def home():
    """
    Show the New Invoice form with a dropdown of existing clients.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, email
        FROM clients
        ORDER BY created_at DESC
    """)
    clients = cursor.fetchall()
    conn.close()

    return render_template("index.html", clients=clients)


# -------------------------
# PREVIEW (currently not used by the form, kept for future)
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
    """
    Save a new invoice:
    - Uses existing client if client_id is provided
    - Or creates a new client if new_client_name is provided
    - Falls back to plain 'client' text if needed
    """
    # Client selection
    selected_client_id = request.form.get("client_id")
    new_client_name = request.form.get("new_client_name")
    new_client_email = request.form.get("new_client_email")
    new_client_company = request.form.get("new_client_company")
    new_client_phone = request.form.get("new_client_phone")
    new_client_address = request.form.get("new_client_address")
    new_client_notes = request.form.get("new_client_notes")

    # Optional invoice meta
    notes = request.form.get("invoice_notes") or ""
    terms = request.form.get("invoice_terms") or "Payment due within 30 days."

    descriptions = request.form.getlist("description")
    amounts = request.form.getlist("amount")

    created_at = datetime.now()
    status = "Sent"
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

    # Figure out which client to use
    client_name_for_invoice = None
    client_id = None

    # 1) If an existing client is selected
    if selected_client_id:
        try:
            cid_int = int(selected_client_id)
            cursor.execute(
                "SELECT id, name FROM clients WHERE id = %s",
                (cid_int,)
            )
            row = cursor.fetchone()
            if row:
                client_id, client_name_for_invoice = row
        except ValueError:
            pass

    # 2) If new client details are provided, create a client
    if not client_name_for_invoice and new_client_name:
        cursor.execute(
            """
            INSERT INTO clients (name, email, company, phone, address, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                new_client_name,
                new_client_email or None,
                new_client_company or None,
                new_client_phone or None,
                new_client_address or None,
                new_client_notes or None
            )
        )
        client_id = cursor.fetchone()[0]
        client_name_for_invoice = new_client_name

    # 3) Fallback: free-text client field (for safety)
    if not client_name_for_invoice:
        client_name_for_invoice = request.form.get("client") or "Unknown client"

    # Insert invoice and get its ID
    cursor.execute("""
        INSERT INTO invoices (client, amount, created_at, status, due_date, notes, terms, client_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (client_name_for_invoice, total, created_at, status, due_date, notes, terms, client_id))

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
# INVOICES PAGE (with search & filters)
# -------------------------
@app.route("/invoices")
def invoices_page():
    # Auto-update overdue statuses
    update_overdue_statuses()

    # Read filters from query string
    q = (request.args.get("q") or "").strip()
    status_filter = (request.args.get("status") or "").strip()
    from_date_str = (request.args.get("from_date") or "").strip()
    to_date_str = (request.args.get("to_date") or "").strip()

    from_dt = None
    to_dt = None

    # Parse date strings (YYYY-MM-DD) if present
    if from_date_str:
        try:
            from_dt = datetime.strptime(from_date_str, "%Y-%m-%d")
        except ValueError:
            from_dt = None

    if to_date_str:
        try:
            # We include the whole day by setting time to end of day
            to_dt = datetime.strptime(to_date_str, "%Y-%m-%d") + timedelta(days=1)
        except ValueError:
            to_dt = None

    conn = get_db_connection()
    cursor = conn.cursor()

    # 1) Fetch ALL invoices for KPIs
    cursor.execute("""
        SELECT id, client, amount, created_at, status, invoice_number, due_date
        FROM invoices
        ORDER BY created_at DESC
    """)
    all_rows = cursor.fetchall()

    all_invoices = []
    for row in all_rows:
        row_list = list(row)
        row_list[2] = float(row_list[2])  # amount
        all_invoices.append(row_list)

    total_invoices = len(all_invoices)
    total_revenue = sum(inv[2] for inv in all_invoices)

    now = datetime.now()
    current_month = now.month
    current_year = now.year

    monthly_revenue = sum(
        inv[2]
        for inv in all_invoices
        if inv[3].month == current_month and inv[3].year == current_year
    )

    growth = round((monthly_revenue / total_revenue) * 100, 1) if total_revenue > 0 else 0
    avg_invoice = round(total_revenue / total_invoices, 2) if total_invoices > 0 else 0

    paid_count = sum(1 for inv in all_invoices if inv[4] == "Paid")
    overdue_count = sum(1 for inv in all_invoices if inv[4] == "Overdue")

    status_distribution = {
        "Paid": paid_count,
        "Sent": sum(1 for inv in all_invoices if inv[4] == "Sent"),
        "Overdue": overdue_count,
    }

    # 2) Build filtered query for the table
    base_sql = """
        SELECT id, client, amount, created_at, status, invoice_number, due_date
        FROM invoices
    """
    conditions = []
    params = []

    # Text search on client or invoice_number
    if q:
        like = f"%{q.lower()}%"
        conditions.append("(LOWER(client) LIKE %s OR LOWER(COALESCE(invoice_number, '')) LIKE %s)")
        params.extend([like, like])

    # Status filter
    allowed_statuses = {"Paid", "Sent", "Overdue"}
    if status_filter in allowed_statuses:
        conditions.append("status = %s")
        params.append(status_filter)

    # Date range filters
    if from_dt:
        conditions.append("created_at >= %s")
        params.append(from_dt)
    if to_dt:
        conditions.append("created_at < %s")
        params.append(to_dt)

    filtered_sql = base_sql
    if conditions:
        filtered_sql += " WHERE " + " AND ".join(conditions)
    filtered_sql += " ORDER BY created_at DESC"

    cursor.execute(filtered_sql, tuple(params))
    filtered_rows = cursor.fetchall()

    conn.close()

    invoices = []
    for row in filtered_rows:
        row_list = list(row)
        row_list[2] = float(row_list[2])  # amount
        invoices.append(row_list)

    filtered_count = len(invoices)

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
        status_distribution=status_distribution,
        # Filters for the template
        q=q,
        status_filter=status_filter,
        from_date_str=from_date_str,
        to_date_str=to_date_str,
        filtered_count=filtered_count
    )


# -------------------------
# CLIENTS PAGE
# -------------------------
@app.route("/clients")
def clients_page():
    """
    Simple clients listing + quick add form.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, email, company, created_at
        FROM clients
        ORDER BY created_at DESC
    """)
    clients = cursor.fetchall()
    conn.close()

    return render_template("clients.html", clients=clients)


@app.route("/clients/add", methods=["POST"])
def add_client():
    """
    Add a new client from the Clients page.
    """
    name = request.form.get("name")
    email = request.form.get("email")
    company = request.form.get("company")
    phone = request.form.get("phone")
    address = request.form.get("address")
    notes = request.form.get("notes")

    if not name:
        return redirect("/clients")

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO clients (name, email, company, phone, address, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (name, email, company, phone, address, notes))
    conn.commit()
    cursor.close()
    conn.close()

    return redirect("/clients")


@app.route("/clients/delete/<int:client_id>")
def delete_client(client_id):
    """
    Delete a client (only if no invoices reference them ideally, but
    for now we won't enforce that).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM clients WHERE id = %s", (client_id,))
    conn.commit()
    cursor.close()
    conn.close()

    return redirect("/clients")


# -------------------------
# EDIT INVOICE
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
# UPDATE INVOICE
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
# DELETE INVOICE
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