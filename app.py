from flask import Flask, render_template, request, send_file, redirect, session, url_for
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
from werkzeug.security import generate_password_hash, check_password_hash
import stripe  # installed and ready

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

app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

# -------------------------
# STRIPE CONFIG
# -------------------------
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")  # Stripe secret key
STRIPE_PRICE_PRO = os.environ.get("STRIPE_PRICE_PRO")  # Pro subscription price ID
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY")

# -------------------------
# PLAN DEFINITIONS
# -------------------------
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

# Simple numeric levels for gating
PLAN_LEVELS = {"free": 1, "pro": 2, "enterprise": 3}

DB_PATH = Path("invoices.db")


# -------------------------
# DATABASE INITIALIZATION
# -------------------------
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    # USERS
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

    # CORE TABLES
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
            frequency TEXT NOT NULL,
            interval_days INTEGER NOT NULL,
            next_run_date DATE NOT NULL,
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # BUSINESS PROFILE
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

    # SCHEMA EVOLUTIONS
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

    cursor.execute(
        "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);"
    )
    cursor.execute(
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);"
    )
    cursor.execute(
        "ALTER TABLE business_profile ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);"
    )

    # 🔹 NEW: store which visual template this invoice uses
    cursor.execute(
        "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS template_style TEXT;"
    )

    # Stripe linkage for subscriptions
    cursor.execute(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT;"
    )
    cursor.execute(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT;"
    )

    conn.commit()

    # DEFAULT OWNER USER
    cursor.execute("SELECT id FROM users ORDER BY id ASC LIMIT 1;")
    row = cursor.fetchone()
    if not row:
        cursor.execute(
            """
            INSERT INTO users (email, password_hash, plan, is_active)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """,
            ("owner@example.com", None, "pro", True),
        )
        default_user_id = cursor.fetchone()[0]

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


# -------------------------
# USER + PLAN HELPERS
# -------------------------
def get_default_user():
    """
    Fallback user if no session user is set.
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
    If session['user_id'] is set, return that user.
    Otherwise, return the default user.
    """
    user_id = session.get("user_id")
    if user_id:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, email, plan, is_active, created_at FROM users WHERE id = %s",
            (user_id,),
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            uid, email, plan, is_active, created_at = row
            if is_active:
                return {
                    "id": uid,
                    "email": email,
                    "plan": plan or "free",
                    "is_active": is_active,
                    "created_at": created_at,
                }

    return get_default_user()


def get_plan_for_current_user():
    user = get_current_user()
    return user.get("plan") or "free"


@app.route("/debug-plan")
def debug_plan():
    """
    Quick JSON view of the current user + plan, for debugging.
    """
    user = get_current_user()
    return {
        "id": user.get("id"),
        "email": user.get("email"),
        "plan": user.get("plan"),
    }


@app.route("/dev/force-pro")
def dev_force_pro():
    """
    DEV ONLY: Force the current user to Pro in the database.
    This bypasses Stripe entirely so we can move forward.
    """
    user = get_current_user()
    user_id = user.get("id")

    if not user_id:
        return "No current user found.", 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET plan = %s WHERE id = %s",
            ("pro", user_id),
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        return f"Error updating user plan: {e}", 500

    return f"User {user_id} is now Pro."


def plan_allows(required_plan: str) -> bool:
    """
    Return True if the current user's plan is >= required_plan.
    """
    user_plan = get_plan_for_current_user()
    return PLAN_LEVELS.get(user_plan, 0) >= PLAN_LEVELS.get(required_plan, 0)


def check_invoice_quota_or_reason():
    """
    Enforce simple invoice quotas by plan.

    free -> max 10 invoices / calendar month
    pro / enterprise -> unlimited
    """
    user = get_current_user()
    user_id = user["id"]
    plan = user.get("plan") or "free"

    if plan != "free":
        return True, None

    now = datetime.now()
    month_start = datetime(now.year, now.month, 1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM invoices
        WHERE user_id = %s
          AND created_at >= %s
          AND created_at < %s
        """,
        (user_id, month_start, next_month),
    )
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if count >= 10:
        return (
            False,
            "You've reached the 10 invoices / month limit on the Starter plan.",
        )

    return True, None


def get_business_profile():
    """
    Return a dict of business profile settings for the current user.
    If no row exists, return sensible defaults.
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

    if not row:
        return {
            "id": None,
            "business_name": "InvoicePro",
            "email": "",
            "phone": "",
            "website": "",
            "address": "",
            "logo_url": "",
            "brand_color": "#151B54",
            "accent_color": "#1d4ed8",
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
    """
    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cursor = conn.cursor()

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
# INITIALIZE DB ON STARTUP
# -------------------------
init_db()


# -------------------------
# CONTEXT PROCESSORS
# -------------------------
@app.context_processor
def inject_now():
    return {"now": datetime.now}


@app.context_processor
def inject_business_profile_ctx():
    return {"business_profile": get_business_profile()}


@app.context_processor
def inject_current_user_ctx():
    user = get_current_user()
    plan = user.get("plan") or "free"
    is_authenticated = "user_id" in session and user.get("id") is not None
    return {
        "current_user": user,
        "user_plan": plan,
        "plan_definitions": PLAN_DEFINITIONS,
        "is_authenticated": is_authenticated,
    }


# -------------------------
# HOME (NEW INVOICE FORM)
# -------------------------
@app.route("/")
def home():
    conn = get_db_connection()
    cursor = conn.cursor()
    user = get_current_user()
    user_id = user["id"]
    cursor.execute(
        """
        SELECT id, name, email
        FROM clients
        WHERE user_id = %s
        ORDER BY created_at DESC
        """,
        (user_id,),
    )
    clients = cursor.fetchall()
    conn.close()

    return render_template("index.html", clients=clients)


# -------------------------
# AUTH
# -------------------------
@app.route("/register", methods=["GET", "POST"])
def register():
    error = None

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""

        if not email or not password:
            error = "Email and password are required."
        elif password != confirm:
            error = "Passwords do not match."
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
            existing = cursor.fetchone()
            if existing:
                error = "An account with that email already exists."
                cursor.close()
                conn.close()
            else:
                password_hash = generate_password_hash(password)
                cursor.execute(
                    """
                    INSERT INTO users (email, password_hash, plan, is_active)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, plan;
                    """,
                    (email, password_hash, "free", True),
                )
                row = cursor.fetchone()
                conn.commit()
                cursor.close()
                conn.close()

                if row:
                    user_id, plan = row
                    session["user_id"] = user_id
                    return redirect(url_for("invoices_page"))

    return render_template("register.html", error=error)


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        if not email or not password:
            error = "Email and password are required."
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, email, password_hash, plan, is_active FROM users WHERE email = %s",
                (email,),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()

            if not row:
                error = "Invalid email or password."
            else:
                user_id, user_email, password_hash, plan, is_active = row
                if not is_active:
                    error = "This account is inactive."
                elif not password_hash:
                    error = "This account cannot be logged into yet."
                elif not check_password_hash(password_hash, password):
                    error = "Invalid email or password."
                else:
                    session["user_id"] = user_id
                    return redirect(url_for("invoices_page"))

    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/pricing")
def pricing():
    user = get_current_user()
    user_plan = user.get("plan") or "free"
    is_pro = PLAN_LEVELS.get(user_plan, 0) >= PLAN_LEVELS.get("pro", 0)

    return render_template(
        "pricing.html",
        current_user=user,
        user_plan=user_plan,
        plans=PLAN_DEFINITIONS,
        stripe_publishable_key=STRIPE_PUBLISHABLE_KEY,
        is_pro=is_pro,
    )


# Stub route so url_for('create_checkout_session') works.
# The button can show "Coming soon" and just bounce back to pricing for now.
@app.route("/create-checkout-session", methods=["POST"])
def create_checkout_session_route():
    return redirect(url_for("pricing", upgraded="stub"))


# -------------------------
# PREVIEW (optional)
# -------------------------
@app.route("/preview", methods=["POST"])
def preview():
    """
    Preview an invoice BEFORE saving.
    Uses the same form fields as /save but does NOT write to the database.
    """
    user = get_current_user()
    user_id = user["id"]

    # Determine client display
    client_id = request.form.get("client_id")
    new_client_name = (request.form.get("new_client_name") or "").strip()
    new_client_email = (request.form.get("new_client_email") or "").strip()

    client_name = ""
    client_email = ""

    # If they picked an existing client, fetch their name/email
    if client_id:
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "SELECT name, email FROM clients WHERE id = %s AND user_id = %s",
                (int(client_id), user_id),
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                client_name = row[0] or ""
                client_email = row[1] or ""
        except Exception:
            # soft-fail, we'll fall back to new client fields
            pass

    if not client_name:
        client_name = new_client_name or "Unspecified client"
    if not client_email:
        client_email = new_client_email

    invoice_notes = request.form.get("invoice_notes") or ""
    invoice_terms = request.form.get("invoice_terms") or "Payment due within 30 days."
    template_style = request.form.get("template_style") or "modern"

    descriptions = request.form.getlist("description")
    amounts = request.form.getlist("amount")

    items = []
    total = 0.0

    for desc, amt in zip(descriptions, amounts):
        if desc and amt:
            try:
                value = float(amt)
            except ValueError:
                continue
            total += value
            items.append((desc, value))

    preview_created_at = datetime.now()

    return render_template(
        "preview.html",
        client_name=client_name,
        client_email=client_email,
        invoice_notes=invoice_notes,
        invoice_terms=invoice_terms,
        items=items,
        total=total,
        template_style=template_style,
        preview_created_at=preview_created_at,
    )


# -------------------------
# SAVE INVOICE (with free-plan quota)
# -------------------------
@app.route("/save", methods=["POST"])
def save():
    """
    Save a new invoice:
    - Enforces free plan monthly quota
    - Uses existing client if client_id is provided
    - Or creates a new client if new_client_name is provided
    - Falls back to plain 'client' text if needed
    - Stores selected template_style for later PDF/web rendering
    """
    allowed, reason = check_invoice_quota_or_reason()
    if not allowed:
        return render_template(
            "upgrade_gate.html",
            title="Invoice limit reached",
            reason=reason,
            required_plan="pro",
            plans=PLAN_DEFINITIONS,
        )

    selected_client_id = request.form.get("client_id")
    new_client_name = request.form.get("new_client_name")
    new_client_email = request.form.get("new_client_email")
    new_client_company = request.form.get("new_client_company")
    new_client_phone = request.form.get("new_client_phone")
    new_client_address = request.form.get("new_client_address")
    new_client_notes = request.form.get("new_client_notes")

    notes = request.form.get("invoice_notes") or ""
    terms = request.form.get("invoice_terms") or "Payment due within 30 days."

    # 🔹 NEW: template style selection from the form
    template_style = request.form.get("template_style") or "modern"

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

    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cursor = conn.cursor()

    client_name_for_invoice = None
    client_id = None

    if selected_client_id:
        try:
            cid_int = int(selected_client_id)
            cursor.execute(
                "SELECT id, name FROM clients WHERE id = %s AND user_id = %s",
                (cid_int, user_id),
            )
            row = cursor.fetchone()
            if row:
                client_id, client_name_for_invoice = row
        except ValueError:
            pass

    if not client_name_for_invoice and new_client_name:
        cursor.execute(
            """
            INSERT INTO clients (name, email, company, phone, address, notes, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                new_client_name,
                new_client_email or None,
                new_client_company or None,
                new_client_phone or None,
                new_client_address or None,
                new_client_notes or None,
                user_id,
            ),
        )
        client_id = cursor.fetchone()[0]
        client_name_for_invoice = new_client_name

    if not client_name_for_invoice:
        client_name_for_invoice = request.form.get("client") or "Unknown client"

    # 🔹 NEW: store template_style column
    cursor.execute(
        """
        INSERT INTO invoices (
            client,
            amount,
            created_at,
            status,
            due_date,
            notes,
            terms,
            template_style,
            client_id,
            user_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            template_style,
            client_id,
            user_id,
        ),
    )

    invoice_id = cursor.fetchone()[0]

    invoice_number = f"INV-{invoice_id:05d}"
    cursor.execute(
        "UPDATE invoices SET invoice_number = %s WHERE id = %s",
        (invoice_number, invoice_id),
    )

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
# INVOICES DASHBOARD
# -------------------------
@app.route("/invoices")
def invoices_page():
    update_overdue_statuses()

    q = (request.args.get("q") or "").strip()
    status_filter = (request.args.get("status") or "").strip()
    from_date_str = (request.args.get("from_date") or "").strip()
    to_date_str = (request.args.get("to_date") or "").strip()

    from_dt = None
    to_dt = None

    if from_date_str:
        try:
            from_dt = datetime.strptime(from_date_str, "%Y-%m-%d")
        except ValueError:
            from_dt = None

    if to_date_str:
        try:
            to_dt = datetime.strptime(to_date_str, "%Y-%m-%d") + timedelta(days=1)
        except ValueError:
            to_dt = None

    conn = get_db_connection()
    cursor = conn.cursor()

    current_user = get_current_user()
    user_id = current_user["id"]

    # KPIs
    cursor.execute(
        """
        SELECT id, client, amount, created_at, status
        FROM invoices
        WHERE user_id = %s
        ORDER BY created_at DESC
        """,
        (user_id,),
    )
    all_rows = cursor.fetchall()

    all_invoices = []
    for row in all_rows:
        row_list = list(row)
        row_list[2] = float(row_list[2])
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

    # Monthly revenue data for chart (last 6 months)
    cursor.execute(
        """
        SELECT date_trunc('month', created_at) AS month, SUM(amount) AS total
        FROM invoices
        WHERE user_id = %s
        GROUP BY month
        ORDER BY month DESC
        LIMIT 6
        """,
        (user_id,),
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

    # Top clients (for leaderboard)
    cursor.execute(
        """
        SELECT
            client,
            SUM(amount) AS total_billed,
            COUNT(*) AS invoice_count
        FROM invoices
        WHERE user_id = %s
        GROUP BY client
        HAVING SUM(amount) > 0
        ORDER BY total_billed DESC
        LIMIT 5
        """,
        (user_id,),
    )
    tc_rows = cursor.fetchall()
    top_clients = []
    if tc_rows:
        top_total = float(tc_rows[0][1]) if tc_rows[0][1] is not None else 0.0
        for name, total_amt, inv_count in tc_rows:
            total_float = float(total_amt or 0)
            pct = 0.0
            if top_total > 0:
                pct = round((total_float / top_total) * 100, 1)
            top_clients.append([name, total_float, inv_count, pct])

    # Filtered table query
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
        WHERE i.user_id = %s
    """
    conditions = []
    params = [user_id]

    if q:
    like = f"%{q.lower()}%"
    conditions.append(
        """
        (
            LOWER(i.client) LIKE %s
            OR LOWER(COALESCE(i.invoice_number, '')) LIKE %s
            OR LOWER(COALESCE(i.notes, '')) LIKE %s
            OR LOWER(COALESCE(i.terms, '')) LIKE %s
        )
        """
    )
    params.extend([like, like, like, like])

    allowed_statuses = {"Paid", "Sent", "Overdue"}
    if status_filter in allowed_statuses:
        conditions.append("i.status = %s")
        params.append(status_filter)

    if from_dt:
        conditions.append("i.created_at >= %s")
        params.append(from_dt)
    if to_dt:
        conditions.append("i.created_at < %s")
        params.append(to_dt)

    filtered_sql = base_sql
    if conditions:
        filtered_sql += " AND " + " AND ".join(conditions)
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
        row_list[2] = float(row_list[2])
        row_list[7] = float(row_list[7])
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
        q=q,
        status_filter=status_filter,
        from_date_str=from_date_str,
        to_date_str=to_date_str,
        filtered_count=filtered_count,
        monthly_chart_labels=monthly_chart_labels,
        monthly_chart_totals=monthly_chart_totals,
        status_chart_labels=status_chart_labels,
        status_chart_values=status_chart_values,
        top_clients=top_clients,
    )


# -------------------------
# SETTINGS
# -------------------------
@app.route("/settings", methods=["GET", "POST"])
def settings():
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
# CLIENTS
# -------------------------
@app.route("/clients")
def clients_page():
    conn = get_db_connection()
    cursor = conn.cursor()
    user = get_current_user()
    user_id = user["id"]
    cursor.execute(
        """
        SELECT id, name, email, company, created_at
        FROM clients
        WHERE user_id = %s
        ORDER BY created_at DESC
        """,
        (user_id,),
    )
    clients = cursor.fetchall()
    conn.close()

    return render_template("clients.html", clients=clients)


@app.route("/clients/add", methods=["POST"])
def add_client():
    name = request.form.get("name")
    email = request.form.get("email")
    company = request.form.get("company")
    phone = request.form.get("phone")
    address = request.form.get("address")
    notes = request.form.get("notes")

    if not name:
        return redirect("/clients")

    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO clients (name, email, company, phone, address, notes, user_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (name, email, company, phone, address, notes, user_id),
    )
    conn.commit()
    cursor.close()
    conn.close()

    return redirect("/clients")


@app.route("/clients/delete/<int:client_id>")
def delete_client(client_id):
    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM clients WHERE id = %s AND user_id = %s",
        (client_id, user_id),
    )
    conn.commit()
    cursor.close()
    conn.close()

    return redirect("/clients")


# -------------------------
# PAYMENTS
# -------------------------
@app.route("/add-payment/<int:invoice_id>", methods=["GET", "POST"])
def add_payment(invoice_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    user = get_current_user()
    user_id = user["id"]

    cursor.execute(
        """
        SELECT id, client, amount, status, invoice_number
        FROM invoices
        WHERE id = %s AND user_id = %s
        """,
        (invoice_id, user_id),
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

            cursor.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE invoice_id = %s",
                (invoice_id_db,),
            )
            total_paid = float(cursor.fetchone()[0])

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
# EDIT / UPDATE / DELETE INVOICE
# -------------------------
@app.route("/edit/<int:invoice_id>")
def edit(invoice_id):
    conn = get_db_connection()
    c = conn.cursor()

    current_user = get_current_user()
    user_id = current_user["id"]

    c.execute(
        "SELECT id, client FROM invoices WHERE id = %s AND user_id = %s",
        (invoice_id, user_id),
    )
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

    current_user = get_current_user()
    user_id = current_user["id"]

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        "UPDATE invoices SET client = %s, amount = %s WHERE id = %s AND user_id = %s",
        (client, total, invoice_id, user_id),
    )

    c.execute(
        "DELETE FROM invoice_items WHERE invoice_id = %s",
        (invoice_id,),
    )

    for desc, amt in cleaned_items:
        c.execute(
            "INSERT INTO invoice_items (invoice_id, description, amount) VALUES (%s, %s, %s)",
            (invoice_id, desc, amt),
        )

    conn.commit()
    conn.close()

    return redirect("/invoices")


@app.route("/update-status/<int:invoice_id>/<string:new_status>")
def update_status(invoice_id, new_status):
    conn = get_db_connection()
    c = conn.cursor()

    current_user = get_current_user()
    user_id = current_user["id"]

    c.execute(
        "UPDATE invoices SET status = %s WHERE id = %s AND user_id = %s",
        (new_status, invoice_id, user_id),
    )

    conn.commit()
    conn.close()

    return redirect("/invoices")


@app.route("/delete/<int:invoice_id>")
def delete(invoice_id):
    conn = get_db_connection()
    c = conn.cursor()

    current_user = get_current_user()
    user_id = current_user["id"]

    c.execute(
        "SELECT id FROM invoices WHERE id = %s AND user_id = %s",
        (invoice_id, user_id),
    )
    row = c.fetchone()
    if not row:
        conn.close()
        return "Invoice not found", 404

    c.execute("DELETE FROM invoice_items WHERE invoice_id = %s", (invoice_id,))
    c.execute("DELETE FROM invoices WHERE id = %s", (invoice_id,))

    conn.commit()
    conn.close()

    return redirect("/invoices")


# -------------------------
# PDF GENERATION
# -------------------------
def generate_invoice_pdf_bytes(invoice_id: int):
    conn = get_db_connection()
    c = conn.cursor()

    # 🔹 Grab template_style along with core fields
    c.execute(
        """
        SELECT client, amount, created_at, due_date, invoice_number, template_style
        FROM invoices
        WHERE id = %s
        """,
        (invoice_id,),
    )
    row = c.fetchone()

    if not row:
        conn.close()
        return None, "Invoice not found"

    client, amount, created_at, due_date, invoice_number, template_style = row
    template_style = (template_style or "modern").lower()

    c.execute(
        "SELECT description, amount FROM invoice_items WHERE invoice_id = %s",
        (invoice_id,),
    )
    items = c.fetchall()
    conn.close()

    amount_float = float(amount)
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=LETTER)

    page_width, page_height = LETTER

    # ---------- TEMPLATE STYLES ----------
    # Basic defaults
    header_bar_color = (21 / 255, 27 / 255, 84 / 255)  # deep blue
    title_text = "Invoice"
    accent_color = header_bar_color

    if template_style == "minimal":
        header_bar_color = (0.18, 0.20, 0.24)  # dark slate
        title_text = "Invoice"
        accent_color = (0.6, 0.6, 0.65)
    elif template_style == "bold":
        header_bar_color = (0.97, 0.45, 0.09)  # orange
        title_text = "Invoice Statement"
        accent_color = (0.97, 0.45, 0.09)
    elif template_style == "doodle":
        header_bar_color = (0.33, 0.27, 0.96)  # purple/blue
        title_text = "Invoice"
        accent_color = (0.33, 0.27, 0.96)

        # Simple doodle-like shapes in background
        pdf.setFillColorRGB(0.93, 0.95, 1.0)
        pdf.circle(60, page_height - 120, 26, fill=1, stroke=0)
        pdf.setFillColorRGB(0.96, 0.92, 1.0)
        pdf.circle(page_width - 80, page_height - 200, 30, fill=1, stroke=0)
        pdf.setFillColorRGB(0.90, 0.96, 0.98)
        pdf.rect(page_width - 150, 40, 120, 60, fill=1, stroke=0)

    # ---------- HEADER BAR ----------
    pdf.setFillColorRGB(*header_bar_color)
    pdf.rect(0, page_height - 60, page_width, 60, fill=1, stroke=0)

    pdf.setFillColorRGB(1, 1, 1)
    pdf.setFont("Helvetica-Bold", 22)
    pdf.drawString(72, page_height - 40, title_text)

    # Reset to dark text for body
    pdf.setFillColorRGB(0.1, 0.1, 0.15)
    pdf.setFont("Helvetica", 11)

    inv_label = invoice_number or f"#{invoice_id}"
    y = page_height - 90

    pdf.drawString(72, y, f"Invoice: {inv_label}")
    y -= 16
    pdf.drawString(72, y, f"Client: {client}")

    if created_at:
        y -= 16
        pdf.drawString(72, y, f"Created: {created_at.strftime('%Y-%m-%d')}")
    if due_date:
        y -= 16
        pdf.drawString(72, y, f"Due: {due_date.strftime('%Y-%m-%d')}")

    # Right side summary box
    right_box_top = page_height - 90
    right_box_left = page_width - 220
    pdf.setFillColorRGB(1, 1, 1)
    pdf.setStrokeColorRGB(*accent_color)
    pdf.rect(right_box_left, right_box_top - 50, 180, 50, fill=1, stroke=1)

    pdf.setFillColorRGB(0.1, 0.1, 0.15)
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(right_box_left + 10, right_box_top - 20, "Total")
    pdf.setFont("Helvetica-Bold", 14)
    pdf.setFillColorRGB(*accent_color)
    pdf.drawRightString(
        right_box_left + 170,
        right_box_top - 30,
        f"${amount_float:,.2f}",
    )

    # ---------- LINE ITEMS ----------
    y = right_box_top - 80
    pdf.setFillColorRGB(0.1, 0.1, 0.15)
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(72, y, "Line Items")
    y -= 20

    pdf.setFont("Helvetica", 11)
    pdf.setFillColorRGB(0.3, 0.3, 0.35)
    pdf.drawString(72, y, "Description")
    pdf.drawRightString(page_width - 72, y, "Amount")
    y -= 12

    pdf.setStrokeColorRGB(0.85, 0.87, 0.9)
    pdf.line(72, y, page_width - 72, y)
    y -= 18

    pdf.setFont("Helvetica", 10)
    pdf.setFillColorRGB(0.1, 0.1, 0.15)

    for desc, amt in items:
        amt_float = float(amt)
        pdf.drawString(72, y, f"{desc}")
        pdf.drawRightString(
            page_width - 72,
            y,
            f"${amt_float:,.2f}",
        )
        y -= 16
        if y < 72:
            pdf.showPage()
            y = page_height - 72
            pdf.setFont("Helvetica", 10)
            pdf.setFillColorRGB(0.1, 0.1, 0.15)

    # ---------- TOTAL FOOTER ----------
    if y < 110:
        pdf.showPage()
        y = page_height - 100

    y -= 10
    pdf.setStrokeColorRGB(0.85, 0.87, 0.9)
    pdf.line(72, y, page_width - 72, y)
    y -= 24

    pdf.setFont("Helvetica-Bold", 12)
    pdf.setFillColorRGB(*accent_color)
    pdf.drawRightString(
        page_width - 72,
        y,
        f"Total Due: ${amount_float:,.2f}",
    )

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    return buffer.getvalue(), None

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


# -------------------------
# PUBLIC INVOICE PORTAL
# -------------------------
@app.route("/public/<string:token>")
def public_invoice(token):
    conn = get_db_connection()
    cursor = conn.cursor()

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

    if client_name_from_client:
        client_name = client_name_from_client

    amount_float = float(amount)
    inv_label = invoice_number or f"#{invoice_id}"

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
        is_public_view=True,
        public_token=token,
    )



@app.route("/invoice/<int:invoice_id>")
def invoice_detail(invoice_id):
    """
    Internal preview of an invoice (owner view).
    Uses the same layout as the client portal, but keeps full app chrome.
    """
    current_user = get_current_user()
    user_id = current_user["id"]

    conn = get_db_connection()
    cursor = conn.cursor()
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
        WHERE i.id = %s AND i.user_id = %s
        """,
        (invoice_id, user_id),
    )
    inv_row = cursor.fetchone()

    if not inv_row:
        cursor.close()
        conn.close()
        return "Invoice not found", 404

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

    if client_name_from_client:
        client_name = client_name_from_client

    amount_float = float(amount)
    inv_label = invoice_number or f"#{invoice_id}"

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
        is_public_view=False,  # owner view, full app nav
        public_token=None,
    )


# -------------------------
# EMAIL (Resend / SMTP)
# -------------------------
def send_email_via_resend(
    to_email: str, subject: str, body_text: str, pdf_bytes: bytes, filename: str
):
    api_key = os.environ.get("RESEND_API_KEY")
    resend_from = os.environ.get("RESEND_FROM")

    if not api_key:
        return False, "Resend configuration missing: RESEND_API_KEY is not set."

    if not resend_from:
        return (
            False,
            "Resend configuration missing: RESEND_FROM is not set. "
            "Set RESEND_FROM to something like 'InvoicePro <billing@mikeinvoices.com>'.",
        )

    if "gmail.com" in resend_from.lower():
        return (
            False,
            "Resend cannot send from a gmail.com address. "
            f"Current RESEND_FROM value is: '{resend_from}'. "
            "Use your verified domain, e.g. 'InvoicePro <billing@mikeinvoices.com>'.",
        )

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
    pdf_bytes, err = generate_invoice_pdf_bytes(invoice_id)
    if err:
        return False, err

    filename = f"invoice_{invoice_id}.pdf"

    if os.environ.get("RESEND_API_KEY"):
        success, api_err = send_email_via_resend(
            to_email=to_email,
            subject=subject,
            body_text=body_text,
            pdf_bytes=pdf_bytes,
            filename=filename,
        )
        if success:
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
            return False, api_err

    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    smtp_from = os.environ.get("SMTP_FROM") or smtp_user

    if not smtp_host or not smtp_from:
        return (
            False,
            "No email provider available. Configure Resend (RESEND_API_KEY & RESEND_FROM) "
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
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
    except Exception as e:
        return False, f"Error sending email (connection or SMTP error): {e}"

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


@app.route("/send-email/<int:invoice_id>", methods=["GET", "POST"])
def send_email_view(invoice_id):
    """
    Email invoice with attached PDF + public link.
    GATED: requires Pro plan or above.
    """
    if not plan_allows("pro"):
        return render_template(
            "upgrade_gate.html",
            title="Upgrade to email invoices",
            reason="Email delivery with PDF attachments is available on the Pro plan and above.",
            required_plan="pro",
            plans=PLAN_DEFINITIONS,
        )

    profile = get_business_profile()
    business_name = profile["business_name"] or "InvoicePro"

    conn = get_db_connection()
    cursor = conn.cursor()
    current_user = get_current_user()
    user_id = current_user["id"]

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
        WHERE invoices.id = %s AND invoices.user_id = %s
        """,
        (invoice_id, user_id),
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

    token = public_token_db
    if not token:
        token = get_or_create_public_token(invoice_id_db)

    base_url = request.url_root.rstrip("/")
    public_url = f"{base_url}/public/{token}"

    default_to_email = last_emailed_to or client_email or ""
    default_subject = f"Invoice {inv_label} from {business_name}"
    default_message = (
        f"Hi {client_name},\n\n"
        f"Please find attached your invoice {inv_label} for ${amount_float:,.2f} from {business_name}.\n"
        + (f"Due date: {due_date.strftime('%Y-%m-%d')}\n" if due_date else "")
        + f"\nYou can also view this invoice online here:\n{public_url}\n\n"
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


@app.route("/stripe/webhook", methods=["POST"])
def stripe_webhook():
    """
    Handle Stripe webhooks to mark users as Pro when checkout completes.
    This is a simplified version that ONLY updates the user's plan.
    """
    payload = request.data
    sig_header = request.headers.get("Stripe-Signature")

    if not STRIPE_WEBHOOK_SECRET:
        print("[Stripe] Webhook called but STRIPE_WEBHOOK_SECRET is not set", flush=True)
        return "Webhook secret not configured", 500

    try:
        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=STRIPE_WEBHOOK_SECRET,
        )
    except ValueError:
        print("[Stripe] Invalid payload", flush=True)
        return "Invalid payload", 400
    except stripe.error.SignatureVerificationError:
        print("[Stripe] Invalid signature", flush=True)
        return "Invalid signature", 400

    print(f"[Stripe] Received event: {event['type']}", flush=True)

    if event["type"] == "checkout.session.completed":
        session_obj = event["data"]["object"]
        metadata = session_obj.get("metadata") or {}
        user_id_str = metadata.get("user_id")

        print(f"[Stripe] checkout.session.completed metadata={metadata}", flush=True)

        if user_id_str:
            try:
                user_id = int(user_id_str)
            except ValueError:
                print(f"[Stripe] Invalid user_id in metadata: {user_id_str}", flush=True)
                return "Bad metadata", 200

            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE users SET plan = %s WHERE id = %s",
                    ("pro", user_id),
                )
                conn.commit()
                cursor.close()
                conn.close()
                print(f"[Stripe] Upgraded user {user_id} to Pro", flush=True)
            except Exception as e:
                print(f"[Stripe] DB error while upgrading user {user_id}: {e}", flush=True)
        else:
            print("[Stripe] No user_id in metadata; cannot upgrade", flush=True)

    return "OK", 200


# -------------------------
# HEALTHCHECK
# -------------------------
@app.route("/health")
def health():
    return "OK", 200


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)