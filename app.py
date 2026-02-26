from flask import Flask, render_template, request, send_file, redirect
from datetime import datetime, timedelta
import psycopg2
from urllib.parse import urlparse
from pathlib import Path
import os
import io
import smtplib
from email.message import EmailMessage
import base64
import requests
import secrets
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
        port=result.port,
    )
    return conn


app = Flask(__name__)

# Simple plan metadata (we’ll expand and enforce later)
PLAN_DEFINITIONS = {
    "free": {
        "name": "Starter",
        "price_label": "$0 / month",
        "tagline": "For freelancers just getting started.",
        "features": [
            "Up to 10 invoices / month",
            "Single invoice template",
            "Basic dashboard",
        ],
    },
    "pro": {
        "name": "Pro",
        "price_label": "$29 / month",
        "tagline": "For growing businesses who invoice regularly.",
        "features": [
            "Unlimited invoices",
            "Multiple invoice templates",
            "Email delivery + PDFs",
            "Public invoice links & Pay Now",
            "Recurring invoices",
        ],
        "recommended": True,
    },
    "enterprise": {
        "name": "Studio",
        "price_label": "Contact us",
        "tagline": "For agencies and teams that need more.",
        "features": [
            "All Pro features",
            "Custom branding & domains",
            "Priority support",
            "Team access (coming soon)",
        ],
    },
}

DB_PATH = Path("invoices.db")


# -------------------------
# DATABASE INITIALIZATION
# -------------------------
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    # ==========================
    # USERS (multi-tenant base)
    # ==========================
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT,
            plan TEXT DEFAULT 'free',
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # ==========================
    # CORE TABLES
    # ==========================
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS invoices (
            id SERIAL PRIMARY KEY,
            client TEXT NOT NULL,
            amount NUMERIC NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'Sent'
        );
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_items (
            id SERIAL PRIMARY KEY,
            invoice_id INTEGER REFERENCES invoices(id) ON DELETE CASCADE,
            description TEXT,
            amount NUMERIC
        );
    """
    )

    cursor.execute(
        """
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
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            invoice_id INTEGER REFERENCES invoices(id) ON DELETE CASCADE,
            amount NUMERIC NOT NULL,
            method TEXT,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS recurring_invoices (
            id SERIAL PRIMARY KEY,
            invoice_id INTEGER REFERENCES invoices(id) ON DELETE CASCADE,
            frequency TEXT NOT NULL,        -- 'weekly', 'biweekly', 'monthly', 'custom'
            interval_days INTEGER NOT NULL, -- how many days between invoices
            next_run_date DATE NOT NULL,    -- when to generate the next invoice
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
    )

    # ==========================
    # BUSINESS PROFILE
    # ==========================
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS business_profile (
            id SERIAL PRIMARY KEY,
            business_name TEXT,
            email TEXT,
            phone TEXT,
            website TEXT,
            address TEXT,
            logo_url TEXT,
            brand_color TEXT,
            accent_color TEXT,
            default_terms TEXT,
            default_notes TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
    )

    # ==========================
    # SCHEMA EVOLUTIONS
    # ==========================

    # Extra columns on invoices
    cursor.execute(
        "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS invoice_number TEXT;"
    )
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS due_date TIMESTAMP;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS notes TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS terms TEXT;")
    cursor.execute(
        "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS client_id INTEGER REFERENCES clients(id);"
    )
    cursor.execute(
        "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS last_emailed_at TIMESTAMP;"
    )
    cursor.execute(
        "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS last_emailed_to TEXT;"
    )
    cursor.execute(
        "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS public_token TEXT UNIQUE;"
    )

    # Multi-tenant ownership columns
    cursor.execute(
        "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);"
    )
    cursor.execute(
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);"
    )
    cursor.execute(
        "ALTER TABLE business_profile ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);"
    )

    conn.commit()

    # ==========================
    # ENSURE A DEFAULT USER
    # ==========================
    cursor.execute("SELECT id FROM users ORDER BY id ASC LIMIT 1;")
    row = cursor.fetchone()
    if not row:
        # Simple default owner; we’ll replace with real auth later
        cursor.execute(
            """
            INSERT INTO users (email, password_hash, plan, is_active)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """,
            ("owner@example.com", None, "pro", True),
        )
        default_user_id = cursor.fetchone()[0]

        # Optionally attach existing invoices/clients/business_profile rows to this user
        cursor.execute(
            "UPDATE invoices SET user_id = %s WHERE user_id IS NULL;",
            (default_user_id,),
        )
        cursor.execute(
            "UPDATE clients SET user_id = %s WHERE user_id IS NULL;",
            (default_user_id,),
        )
        cursor.execute(
            "UPDATE business_profile SET user_id = %s WHERE user_id IS NULL;",
            (default_user_id,),
        )

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
    cursor.execute(
        """
        UPDATE invoices
        SET status = 'Overdue'
        WHERE status NOT IN ('Paid', 'Overdue')
          AND due_date IS NOT NULL
          AND due_date < NOW();
    """
    )
    conn.commit()
    cursor.close()
    conn.close()


def get_or_create_public_token(invoice_id: int) -> str:
    """
    Ensure an invoice has a unique public_token used for the /public/<token> link.
    Returns the token string.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if it already has a token
    cursor.execute(
        "SELECT public_token FROM invoices WHERE id = %s",
        (invoice_id,),
    )
    row = cursor.fetchone()
    if not row:
        cursor.close()
        conn.close()
        raise ValueError(f"Invoice {invoice_id} not found")

    existing_token = row[0]
    if existing_token:
        cursor.close()
        conn.close()
        return existing_token

    # Generate a new unique token
    token = None
    while True:
        candidate = secrets.token_urlsafe(16)
        cursor.execute(
            "SELECT id FROM invoices WHERE public_token = %s",
            (candidate,),
        )
        clash = cursor.fetchone()
        if not clash:
            token = candidate
            break

    cursor.execute(
        "UPDATE invoices SET public_token = %s WHERE id = %s",
        (token, invoice_id),
    )
    conn.commit()
    cursor.close()
    conn.close()
    return token


init_db()


@app.context_processor
def inject_now():
    return {"now": datetime.now}

@app.context_processor
def inject_business_profile():
    """
    Makes `business_profile` available in all templates.
    """
    return {"business_profile": get_business_profile()}

@app.context_processor
def inject_current_user():
    """
    Makes `current_user` and `user_plan` available in all templates.
    """
    user = get_current_user()
    plan = user.get("plan") or "free"
    return {
        "current_user": user,
        "user_plan": plan,
        "plan_definitions": PLAN_DEFINITIONS,
    }


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
    cursor.execute(
        """
        SELECT id, name, email
        FROM clients
        ORDER BY created_at DESC
    """
    )
    clients = cursor.fetchall()
    conn.close()

    return render_template("index.html", clients=clients)


@app.route("/pricing")
def pricing():
    user = get_current_user()
    return render_template(
        "pricing.html",
        current_user=user,
        user_plan=user.get("plan") or "free",
        plans=PLAN_DEFINITIONS,
    )


# -------------------------
# PREVIEW (kept for future)
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
        "preview.html", client=client, items=items, total=total
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
                (cid_int,),
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
                new_client_notes or None,
            ),
        )
        client_id = cursor.fetchone()[0]
        client_name_for_invoice = new_client_name

    # 3) Fallback: free-text client field (for safety)
    if not client_name_for_invoice:
        client_name_for_invoice = request.form.get("client") or "Unknown client"

    # Insert invoice and get its ID
    cursor.execute(
        """
        INSERT INTO invoices (client, amount, created_at, status, due_date, notes, terms, client_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """,
        (
            client_name_for_invoice,
            total,
            created_at,
            status,
            due_date,
            notes,
            terms,
            client_id,
        ),
    )

    invoice_id = cursor.fetchone()[0]

    # Compute invoice_number based on ID, e.g. INV-00001
    invoice_number = f"INV-{invoice_id:05d}"
    cursor.execute(
        "UPDATE invoices SET invoice_number = %s WHERE id = %s",
        (invoice_number, invoice_id),
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
# INVOICES PAGE (with search, filters, payments, charts)
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
    cursor.execute(
        """
        SELECT id, client, amount, created_at, status, invoice_number, due_date
        FROM invoices
        ORDER BY created_at DESC
    """
    )
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

    growth = (
        round((monthly_revenue / total_revenue) * 100, 1) if total_revenue > 0 else 0
    )
    avg_invoice = (
        round(total_revenue / total_invoices, 2) if total_invoices > 0 else 0
    )

    paid_count = sum(1 for inv in all_invoices if inv[4] == "Paid")
    overdue_count = sum(1 for inv in all_invoices if inv[4] == "Overdue")

    status_distribution = {
        "Paid": paid_count,
        "Sent": sum(1 for inv in all_invoices if inv[4] == "Sent"),
        "Overdue": overdue_count,
    }

    # 1b) Monthly revenue data for chart (last 6 months)
    cursor.execute(
        """
        SELECT date_trunc('month', created_at) AS month, SUM(amount) AS total
        FROM invoices
        GROUP BY month
        ORDER BY month DESC
        LIMIT 6
    """
    )
    monthly_rows = cursor.fetchall()

    monthly_chart_labels = []
    monthly_chart_totals = []
    for month_dt, total_amt in reversed(monthly_rows):
        monthly_chart_labels.append(month_dt.strftime("%b %Y"))
        monthly_chart_totals.append(float(total_amt))

    status_chart_labels = ["Paid", "Sent", "Overdue"]
    status_chart_values = [
        paid_count,
        status_distribution.get("Sent", 0),
        overdue_count,
    ]

    # 2) Build filtered query for the table, with payments aggregated
    base_sql = """
        SELECT
            i.id,
            i.client,
            i.amount,
            i.created_at,
            i.status,
            i.invoice_number,
            i.due_date,
            COALESCE(SUM(p.amount), 0) AS total_paid
        FROM invoices i
        LEFT JOIN payments p ON p.invoice_id = i.id
    """
    conditions = []
    params = []

    # Text search on client or invoice_number
    if q:
        like = f"%{q.lower()}%"
        conditions.append(
            "(LOWER(i.client) LIKE %s OR LOWER(COALESCE(i.invoice_number, '')) LIKE %s)"
        )
        params.extend([like, like])

    # Status filter
    allowed_statuses = {"Paid", "Sent", "Overdue"}
    if status_filter in allowed_statuses:
        conditions.append("i.status = %s")
        params.append(status_filter)

    # Date range filters
    if from_dt:
        conditions.append("i.created_at >= %s")
        params.append(from_dt)
    if to_dt:
        conditions.append("i.created_at < %s")
        params.append(to_dt)

    filtered_sql = base_sql
    if conditions:
        filtered_sql += " WHERE " + " AND ".join(conditions)
    filtered_sql += """
        GROUP BY
            i.id,
            i.client,
            i.amount,
            i.created_at,
            i.status,
            i.invoice_number,
            i.due_date
        ORDER BY i.created_at DESC
    """

    cursor.execute(filtered_sql, tuple(params))
    filtered_rows = cursor.fetchall()

    conn.close()

    invoices = []
    for row in filtered_rows:
        row_list = list(row)
        row_list[2] = float(row_list[2])  # amount
        row_list[7] = float(row_list[7])  # total_paid
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
        filtered_count=filtered_count,
        # Chart data
        monthly_chart_labels=monthly_chart_labels,
        monthly_chart_totals=monthly_chart_totals,
        status_chart_labels=status_chart_labels,
        status_chart_values=status_chart_values,
    )


@app.route("/settings", methods=["GET", "POST"])
def settings():
    """
    Business Profile & Branding settings page.
    """
    profile = get_business_profile()
    feedback_message = None
    feedback_type = None

    if request.method == "POST":
        business_name = (request.form.get("business_name") or "").strip()
        email = (request.form.get("email") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        website = (request.form.get("website") or "").strip()
        address = (request.form.get("address") or "").strip()
        logo_url = (request.form.get("logo_url") or "").strip()
        brand_color = (request.form.get("brand_color") or "").strip() or "#151B54"
        accent_color = (request.form.get("accent_color") or "").strip() or "#1d4ed8"
        default_terms = (request.form.get("default_terms") or "").strip()
        default_notes = (request.form.get("default_notes") or "").strip()

        data = {
            "business_name": business_name or "InvoicePro",
            "email": email,
            "phone": phone,
            "website": website,
            "address": address,
            "logo_url": logo_url,
            "brand_color": brand_color,
            "accent_color": accent_color,
            "default_terms": default_terms,
            "default_notes": default_notes,
        }

        upsert_business_profile(data)
        profile = get_business_profile()

        feedback_message = "Business profile updated successfully."
        feedback_type = "success"

    return render_template(
        "settings.html",
        profile=profile,
        feedback_message=feedback_message,
        feedback_type=feedback_type,
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
    cursor.execute(
        """
        SELECT id, name, email, company, created_at
        FROM clients
        ORDER BY created_at DESC
    """
    )
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
    cursor.execute(
        """
        INSERT INTO clients (name, email, company, phone, address, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
    """,
        (name, email, company, phone, address, notes),
    )
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
# ADD PAYMENT
# -------------------------
@app.route("/add-payment/<int:invoice_id>", methods=["GET", "POST"])
def add_payment(invoice_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, client, amount, status, invoice_number
        FROM invoices
        WHERE id = %s
    """,
        (invoice_id,),
    )
    invoice = cursor.fetchone()

    if not invoice:
        conn.close()
        return "Invoice not found", 404

    invoice_id_db, client_name, amount, status, invoice_number = invoice
    amount_float = float(amount)
    inv_label = invoice_number or f"#{invoice_id_db}"

    feedback_message = None
    feedback_type = None

    if request.method == "POST":
        try:
            amt_str = (request.form.get("amount") or "").strip()
            pay_amount = float(amt_str)
        except ValueError:
            pay_amount = 0.0

        method = (request.form.get("method") or "").strip()
        note = (request.form.get("note") or "").strip()

        if pay_amount <= 0:
            feedback_message = "Payment amount must be greater than zero."
            feedback_type = "error"
        else:
            cursor.execute(
                """
                INSERT INTO payments (invoice_id, amount, method, note)
                VALUES (%s, %s, %s, %s)
            """,
                (invoice_id_db, pay_amount, method or None, note or None),
            )

            # Recompute total paid
            cursor.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE invoice_id = %s",
                (invoice_id_db,),
            )
            total_paid = float(cursor.fetchone()[0])

            # Auto-mark as Paid if fully covered
            if total_paid >= amount_float:
                cursor.execute(
                    "UPDATE invoices SET status = 'Paid' WHERE id = %s",
                    (invoice_id_db,),
                )

            conn.commit()
            feedback_message = (
                f"Recorded payment of ${pay_amount:,.2f} on invoice {inv_label}."
            )
            feedback_type = "success"

    # Fetch payments list for display
    cursor.execute(
        """
        SELECT amount, method, note, created_at
        FROM payments
        WHERE invoice_id = %s
        ORDER BY created_at DESC
    """,
        (invoice_id_db,),
    )
    payments = cursor.fetchall()

    # Compute total paid & balance
    total_paid = sum(float(p[0]) for p in payments)
    balance = amount_float - total_paid

    conn.close()

    return render_template(
        "add_payment.html",
        invoice_id=invoice_id_db,
        client_name=client_name,
        amount=amount_float,
        status=status,
        invoice_number=invoice_number,
        inv_label=inv_label,
        payments=payments,
        total_paid=total_paid,
        balance=balance,
        feedback_message=feedback_message,
        feedback_type=feedback_type,
    )


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
        "edit.html", invoice_id=invoice_id, client=invoice[1], items=items
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
        (client, total, invoice_id),
    )

    c.execute("DELETE FROM invoice_items WHERE invoice_id = %s", (invoice_id,))

    for desc, amt in cleaned_items:
        c.execute(
            "INSERT INTO invoice_items (invoice_id, description, amount) VALUES (%s, %s, %s)",
            (invoice_id, desc, amt),
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
        "UPDATE invoices SET status = %s WHERE id = %s", (new_status, invoice_id)
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
# PDF GENERATION HELPER
# -------------------------
def generate_invoice_pdf_bytes(invoice_id: int):
    """
    Generate a PDF for the given invoice_id and return raw bytes.
    """
    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT client, amount, created_at, due_date, invoice_number
        FROM invoices
        WHERE id = %s
    """,
        (invoice_id,),
    )
    row = c.fetchone()

    if not row:
        conn.close()
        return None, "Invoice not found"

    client, amount, created_at, due_date, invoice_number = row

    c.execute(
        "SELECT description, amount FROM invoice_items WHERE invoice_id = %s",
        (invoice_id,),
    )
    items = c.fetchall()
    conn.close()

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=LETTER)

    pdf.setFont("Helvetica-Bold", 20)
    pdf.drawString(72, 750, "Invoice")

    pdf.setFont("Helvetica", 12)
    inv_label = invoice_number or f"#{invoice_id}"
    pdf.drawString(72, 720, f"Invoice: {inv_label}")
    pdf.drawString(72, 700, f"Client: {client}")
    if created_at:
        pdf.drawString(72, 680, f"Created: {created_at.strftime('%Y-%m-%d')}")
    if due_date:
        pdf.drawString(72, 660, f"Due: {due_date.strftime('%Y-%m-%d')}")

    y = 630
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(72, y, "Line Items")
    y -= 20
    pdf.setFont("Helvetica", 11)

    for desc, amt in items:
        pdf.drawString(72, y, f"{desc}")
        pdf.drawRightString(540, y, f"${amt}")
        y -= 18
        if y < 72:
            pdf.showPage()
            y = 750

    y -= 10
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(72, y, f"Total Due: ${amount}")

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    return buffer.getvalue(), None


# -------------------------
# PDF DOWNLOAD ROUTE
# -------------------------
@app.route("/history-pdf/<int:invoice_id>")
def history_pdf(invoice_id):
    pdf_bytes, err = generate_invoice_pdf_bytes(invoice_id)
    if err:
        return err, 404

    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"invoice_{invoice_id}.pdf",
        mimetype="application/pdf",
    )


@app.route("/public/<string:token>")
def public_invoice(token):
    """
    Public, read-only view for a single invoice by its public_token.
    No auth, so the token should be unguessable.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Look up invoice by token
    cursor.execute(
        """
        SELECT
            i.id,
            i.client,
            i.amount,
            i.created_at,
            i.status,
            i.invoice_number,
            i.due_date,
            i.notes,
            i.terms,
            c.name,
            c.email,
            c.company
        FROM invoices i
        LEFT JOIN clients c ON i.client_id = c.id
        WHERE i.public_token = %s
        """,
        (token,),
    )
    inv_row = cursor.fetchone()

    if not inv_row:
        cursor.close()
        conn.close()
        return "Invoice not found or link invalid.", 404

    (
        invoice_id,
        client_name,
        amount,
        created_at,
        status,
        invoice_number,
        due_date,
        notes,
        terms,
        client_name_from_client,
        client_email,
        client_company,
    ) = inv_row

    # Prefer the dedicated client record name if present
    if client_name_from_client:
        client_name = client_name_from_client

    amount_float = float(amount)
    inv_label = invoice_number or f"#{invoice_id}"

    # Fetch items
    cursor.execute(
        """
        SELECT description, amount
        FROM invoice_items
        WHERE invoice_id = %s
        ORDER BY id ASC
        """,
        (invoice_id,),
    )
    items = cursor.fetchall()

    # Fetch payments
    cursor.execute(
        """
        SELECT amount, method, note, created_at
        FROM payments
        WHERE invoice_id = %s
        ORDER BY created_at DESC
        """,
        (invoice_id,),
    )
    payments = cursor.fetchall()

    total_paid = sum(float(p[0]) for p in payments)
    balance = amount_float - total_paid

    cursor.close()
    conn.close()

    # Reuse your existing PDF route
    pdf_url = f"/history-pdf/{invoice_id}"

    return render_template(
        "public_invoice.html",
        invoice_id=invoice_id,
        inv_label=inv_label,
        client_name=client_name,
        client_email=client_email,
        client_company=client_company,
        amount=amount_float,
        status=status,
        created_at=created_at,
        due_date=due_date,
        notes=notes,
        terms=terms,
        items=items,
        payments=payments,
        total_paid=total_paid,
        balance=balance,
        pdf_url=pdf_url,
        # flags for template
        is_public_view=True,
        public_token=token,
    )


# -------------------------
# EMAIL SENDING HELPER
# -------------------------
def send_email_via_resend(to_email: str, subject: str, body_text: str, pdf_bytes: bytes, filename: str):
    """
    Send email using the Resend HTTP API.
    Returns (success: bool, error_message: str or None)
    """
    api_key = os.environ.get("RESEND_API_KEY")
    resend_from = os.environ.get("RESEND_FROM")

    if not api_key:
        return False, "Resend configuration missing: RESEND_API_KEY is not set."

    if not resend_from:
        return False, (
            "Resend configuration missing: RESEND_FROM is not set. "
            "Set RESEND_FROM to something like 'InvoicePro <billing@mikeinvoices.com>'."
        )

    # Extra guard: don't let gmail.com slip through by mistake
    if "gmail.com" in resend_from.lower():
        return False, (
            "Resend cannot send from a gmail.com address. "
            f"Current RESEND_FROM value is: '{resend_from}'. "
            "Please change RESEND_FROM to use your verified domain, e.g. "
            "'InvoicePro <billing@mikeinvoices.com>'."
        )

    # Resend expects attachments in base64
    encoded_pdf = base64.b64encode(pdf_bytes).decode("utf-8")

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": resend_from,
                "to": [to_email],
                "subject": subject,
                "text": body_text,
                "attachments": [
                    {
                        "filename": filename,
                        "content": encoded_pdf,
                        "contentType": "application/pdf",
                    }
                ],
            },
            timeout=10,
        )

        if resp.status_code >= 400:
            # Try to give a clearer explanation for common cases
            try:
                data = resp.json()
                msg = data.get("message", "")
            except Exception:
                msg = resp.text

            return False, f"Resend API error {resp.status_code}: {msg or resp.text}"

        return True, None

    except Exception as e:
        return False, f"Error sending via Resend: {e}"
def send_invoice_email(invoice_id: int, to_email: str, subject: str, body_text: str):
    """
    Generate the invoice PDF and send it via either:
    - Resend (HTTP API) if RESEND_API_KEY is set (preferred, works on platforms blocking SMTP)
    - SMTP if SMTP_HOST/SMTP_FROM are configured and Resend is not available

    Returns (success: bool, error_message: str or None)
    """
    pdf_bytes, err = generate_invoice_pdf_bytes(invoice_id)
    if err:
        return False, err

    filename = f"invoice_{invoice_id}.pdf"

    # 1) Prefer Resend if configured (works over HTTPS, avoids SMTP port blocking)
    if os.environ.get("RESEND_API_KEY"):
        success, api_err = send_email_via_resend(
            to_email=to_email,
            subject=subject,
            body_text=body_text,
            pdf_bytes=pdf_bytes,
            filename=filename,
        )
        if success:
            # Update invoice with last emailed info
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE invoices SET last_emailed_at = %s, last_emailed_to = %s WHERE id = %s",
                (datetime.now(), to_email, invoice_id),
            )
            conn.commit()
            cur.close()
            conn.close()
            return True, None
        else:
            # If Resend fails, return that error (don't silently fall back)
            return False, api_err

    # 2) Fallback to SMTP if Resend is not configured
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    smtp_from = os.environ.get("SMTP_FROM") or smtp_user

    if not smtp_host or not smtp_from:
        return (
            False,
            "No email provider available. Either configure Resend (RESEND_API_KEY & RESEND_FROM) "
            "or SMTP (SMTP_HOST & SMTP_FROM).",
        )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = to_email
    msg.set_content(body_text)

    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename=filename,
    )

    try:
        # Short timeout so we fail quickly instead of hanging until the worker dies
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
    except Exception as e:
        return False, f"Error sending email (connection or SMTP error): {e}"

    # Update invoice with last emailed info
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE invoices SET last_emailed_at = %s, last_emailed_to = %s WHERE id = %s",
        (datetime.now(), to_email, invoice_id),
    )
    conn.commit()
    cur.close()
    conn.close()

    return True, None


def get_default_user():
    """
    For now, returns the first user in the users table.
    Later, this will be replaced with real auth (session-based).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, email, plan, is_active, created_at FROM users ORDER BY id ASC LIMIT 1;"
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        # Should not happen because init_db ensures a default, but be defensive
        return {
            "id": None,
            "email": "unknown@example.com",
            "plan": "free",
            "is_active": True,
            "created_at": None,
        }

    user_id, email, plan, is_active, created_at = row
    return {
        "id": user_id,
        "email": email,
        "plan": plan or "free",
        "is_active": is_active,
        "created_at": created_at,
    }


def get_current_user():
    """
    Stub for now: always return the default user.
    Later, this will look at session['user_id'].
    """
    return get_default_user()


def get_plan_for_current_user():
    user = get_current_user()
    return user.get("plan") or "free"


def get_business_profile():
    """
    Return a dict of business profile settings for the current user.
    If no row exists, return sensible defaults (does NOT write to DB).
    """
    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, business_name, email, phone, website, address,
               logo_url, brand_color, accent_color, default_terms, default_notes
        FROM business_profile
        WHERE user_id = %s
        ORDER BY id ASC
        LIMIT 1
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    # Defaults if nothing in DB yet
    if not row:
        return {
            "id": None,
            "business_name": "InvoicePro",
            "email": "",
            "phone": "",
            "website": "",
            "address": "",
            "logo_url": "",
            "brand_color": "#151B54",   # your current main accent
            "accent_color": "#1d4ed8",  # secondary accent
            "default_terms": "",
            "default_notes": "",
            "user_id": user_id,
        }

    (
        bp_id,
        business_name,
        email,
        phone,
        website,
        address,
        logo_url,
        brand_color,
        accent_color,
        default_terms,
        default_notes,
    ) = row

    return {
        "id": bp_id,
        "business_name": business_name or "InvoicePro",
        "email": email or "",
        "phone": phone or "",
        "website": website or "",
        "address": address or "",
        "logo_url": logo_url or "",
        "brand_color": brand_color or "#151B54",
        "accent_color": accent_color or "#1d4ed8",
        "default_terms": default_terms or "",
        "default_notes": default_notes or "",
        "user_id": user_id,
    }


def upsert_business_profile(data: dict):
    """
    Insert or update the business_profile row for the current user.
    Expects keys: business_name, email, phone, website, address,
                  logo_url, brand_color, accent_color, default_terms, default_notes.
    """
    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cursor = conn.cursor()

    # See if a row already exists for this user
    cursor.execute(
        "SELECT id FROM business_profile WHERE user_id = %s ORDER BY id ASC LIMIT 1",
        (user_id,),
    )
    row = cursor.fetchone()

    now = datetime.now()

    if row:
        bp_id = row[0]
        cursor.execute(
            """
            UPDATE business_profile
            SET business_name = %s,
                email = %s,
                phone = %s,
                website = %s,
                address = %s,
                logo_url = %s,
                brand_color = %s,
                accent_color = %s,
                default_terms = %s,
                default_notes = %s,
                updated_at = %s
            WHERE id = %s AND user_id = %s
            """,
            (
                data.get("business_name"),
                data.get("email"),
                data.get("phone"),
                data.get("website"),
                data.get("address"),
                data.get("logo_url"),
                data.get("brand_color"),
                data.get("accent_color"),
                data.get("default_terms"),
                data.get("default_notes"),
                now,
                bp_id,
                user_id,
            ),
        )
    else:
        cursor.execute(
            """
            INSERT INTO business_profile
                (business_name, email, phone, website, address,
                 logo_url, brand_color, accent_color, default_terms, default_notes,
                 updated_at, user_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                data.get("business_name"),
                data.get("email"),
                data.get("phone"),
                data.get("website"),
                data.get("address"),
                data.get("logo_url"),
                data.get("brand_color"),
                data.get("accent_color"),
                data.get("default_terms"),
                data.get("default_notes"),
                now,
                user_id,
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


# -------------------------
# SEND EMAIL ROUTE
# -------------------------
@app.route("/send-email/<int:invoice_id>", methods=["GET", "POST"])
def send_email_view(invoice_id):
    """
    Show a form to send the invoice via email, and handle sending.
    Also ensures a public invoice link is available and includes it in the default message.
    """
    profile = get_business_profile()
    business_name = profile["business_name"] or "InvoicePro"

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            invoices.id,
            invoices.client,
            invoices.amount,
            invoices.created_at,
            invoices.status,
            invoices.invoice_number,
            invoices.due_date,
            invoices.last_emailed_at,
            invoices.last_emailed_to,
            clients.email,
            invoices.public_token
        FROM invoices
        LEFT JOIN clients ON invoices.client_id = clients.id
        WHERE invoices.id = %s
    """,
        (invoice_id,),
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        return "Invoice not found", 404

    (
        invoice_id_db,
        client_name,
        amount,
        created_at,
        status,
        invoice_number,
        due_date,
        last_emailed_at,
        last_emailed_to,
        client_email,
        public_token_db,
    ) = row

    amount_float = float(amount)
    inv_label = invoice_number or f"#{invoice_id_db}"

    # Ensure we have a public token for this invoice
    token = public_token_db
    if not token:
        token = get_or_create_public_token(invoice_id_db)

    # Build the full public URL (e.g. https://app.onrender.com/public/...)
    base_url = request.url_root.rstrip("/")  # includes scheme + host + /
    public_url = f"{base_url}/public/{token}"

    # Defaults for the form
    default_to_email = last_emailed_to or client_email or ""
    default_subject = f"Invoice {inv_label} from {business_name}"
    default_message = (
        f"Hi {client_name},\n\n"
        f"Please find attached your invoice {inv_label} for ${amount_float:,.2f} from {business_name}.\n"
        + (
            f"Due date: {due_date.strftime('%Y-%m-%d')}\n"
            if due_date
            else ""
        )
        + (
            f"\nYou can also view this invoice online here:\n{public_url}\n\n"
        )
        + "Thank you for your business!\n\n"
        + f"— {business_name}"
    )

    feedback_message = None
    feedback_type = None

    if request.method == "POST":
        to_email = (request.form.get("to_email") or "").strip()
        subject = request.form.get("subject") or default_subject
        message_body = request.form.get("message") or default_message

        if not to_email:
            feedback_message = "Recipient email is required."
            feedback_type = "error"
        else:
            success, err = send_invoice_email(
                invoice_id_db, to_email, subject, message_body
            )
            if success:
                feedback_message = f"Invoice {inv_label} was emailed to {to_email}."
                feedback_type = "success"
                default_to_email = to_email
            else:
                feedback_message = err or "Failed to send email."
                feedback_type = "error"

    return render_template(
        "send_email.html",
        invoice_id=invoice_id_db,
        client_name=client_name,
        amount=amount_float,
        created_at=created_at,
        status=status,
        invoice_number=invoice_number,
        inv_label=inv_label,
        due_date=due_date,
        last_emailed_at=last_emailed_at,
        last_emailed_to=last_emailed_to,
        default_to_email=default_to_email,
        default_subject=default_subject,
        default_message=default_message,
        feedback_message=feedback_message,
        feedback_type=feedback_type,
        public_url=public_url,
    )


@app.route("/health")
def health():
    return "OK", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)