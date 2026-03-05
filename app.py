from flask import Flask, render_template, request, send_file, redirect, session, url_for, send_from_directory
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
from reportlab.lib.utils import ImageReader  # 🔹 for drawing signature image
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import stripe  # installed and ready
from openai import OpenAI  # ✅ AI helper
from flask import request, jsonify

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

client = OpenAI(api_key=os.getenv("OPENAI_AI_KEY"))

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

# File upload config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}

app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

# -------------------------
# STRIPE CONFIG
# -------------------------
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")  # Stripe secret key
STRIPE_PRICE_PRO = os.environ.get("STRIPE_PRICE_PRO")  # Pro subscription price ID
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY")

# -------------------------
# AI HELPER CONFIG
# -------------------------
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
ai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
AI_MODEL_FREE = "gpt-4o-mini"   # lightweight helper for free users
AI_MODEL_PRO = "gpt-4.1-mini"   # more capable helper for Pro

# -------------------------
# PLAN DEFINITIONS (with EN/ES variants)
# -------------------------
PLAN_DEFINITIONS = {
    "free": {
        "name_en": "Starter",
        "name_es": "Inicio",
        "price_label": "$0 / month",
        "tagline_en": "For freelancers just getting started.",
        "tagline_es": "Para freelancers que están empezando.",
        "features_en": [
            "Up to 10 invoices / month",
            "Single invoice template",
            "Basic dashboard",
        ],
        "features_es": [
            "Hasta 10 facturas al mes",
            "Una sola plantilla de factura",
            "Panel básico",
        ],
    },
    "pro": {
        "name_en": "Pro",
        "name_es": "Pro",
        "price_label": "$29 / month",
        "tagline_en": "For growing businesses who invoice regularly.",
        "tagline_es": "Para negocios en crecimiento que facturan con frecuencia.",
        "features_en": [
            "Unlimited invoices",
            "Multiple invoice templates",
            "Email delivery + PDFs",
            "Public invoice links & Pay Now",
            "Recurring invoices",
        ],
        "features_es": [
            "Facturas ilimitadas",
            "Múltiples plantillas de factura",
            "Envío por email + PDFs",
            "Enlaces públicos de factura y botón Pagar ahora",
            "Facturas recurrentes",
        ],
        "recommended": True,
    },
    "enterprise": {
        "name_en": "Studio",
        "name_es": "Studio",
        "price_label": "Contact us",
        "tagline_en": "For agencies and teams that need more.",
        "tagline_es": "Para agencias y equipos que necesitan más.",
        "features_en": [
            "All Pro features",
            "Custom branding & domains",
            "Priority support",
            "Team access (coming soon)",
        ],
        "features_es": [
            "Todas las funciones Pro",
            "Branding y dominios personalizados",
            "Soporte prioritario",
            "Acceso para equipos (próximamente)",
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

    # 🔹 NEW: store client signature as base64 PNG data URL (optional)
    cursor.execute(
        "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS signature_data TEXT;"
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

    # -------------------------
    # Stripe linkage for invoice payments (Pay Now)
    # -------------------------
    cursor.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS stripe_payment_intent_id TEXT;")
    cursor.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS stripe_checkout_session_id TEXT;")

    # Optional: track last Stripe payment intent on invoice (handy for debugging)
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS stripe_last_payment_intent_id TEXT;")

    # Create an idempotency guarantee: the same Stripe payment intent cannot be inserted twice
    # (Postgres supports IF NOT EXISTS for indexes)
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS payments_stripe_pi_unique ON payments(stripe_payment_intent_id) "
        "WHERE stripe_payment_intent_id IS NOT NULL;"
    )

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

def get_user_plan_by_user_id(user_id: int) -> str:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT plan FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return (row[0] if row and row[0] else "free")


def get_invoice_by_public_token(token: str):
    """
    Returns invoice + computed totals by public token:
      invoice_id, user_id, status, amount_total, total_paid, balance, invoice_number
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            i.id,
            i.user_id,
            i.status,
            i.amount,
            i.invoice_number,
            COALESCE(SUM(p.amount), 0) AS total_paid
        FROM invoices i
        LEFT JOIN payments p ON p.invoice_id = i.id
        WHERE i.public_token = %s
        GROUP BY i.id, i.user_id, i.status, i.amount, i.invoice_number
        """,
        (token,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    invoice_id, user_id, status, amount_total, invoice_number, total_paid = row
    amount_total = float(amount_total or 0)
    total_paid = float(total_paid or 0)
    balance = max(amount_total - total_paid, 0.0)

    return {
        "invoice_id": invoice_id,
        "user_id": user_id,
        "status": status or "Sent",
        "amount_total": amount_total,
        "total_paid": total_paid,
        "balance": balance,
        "invoice_number": invoice_number,
    }


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
            "business_name": "BillBeam",
            "email": "",
            "phone": "",
            "website": "",
            "address": "",
            "logo_url": "",
            # Dark-mode first brand
            "brand_color": "#020617",   # deep midnight
            "accent_color": "#3A8BFF",  # beam blue
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
        "business_name": business_name or "BillBeam",
        "email": email or "",
        "phone": phone or "",
        "website": website or "",
        "address": address or "",
        "logo_url": logo_url or "",
        "brand_color": brand_color or "#020617",
        "accent_color": accent_color or "#3A8BFF",
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
    lang = request.args.get("lang", "en")

    # Determine current user plan
    user = get_current_user()
    user_plan = user.get("plan", "free") if user else "free"

    # Build translated plans for template
    plans = {}

    for key, p in PLAN_DEFINITIONS.items():
        plans[key] = {
            "name": p.get(f"name_{lang}", p.get("name_en")),
            "price_label": p["price_label"],
            "tagline": p.get(f"tagline_{lang}", p.get("tagline_en")),
            "features": p.get(f"features_{lang}", p.get("features_en")),
            "recommended": p.get("recommended", False),
        }

    return render_template(
        "pricing.html",
        plans=plans,
        user_plan=user_plan,
        is_pro=(user_plan == "pro"),
        stripe_publishable_key=os.getenv("STRIPE_PUBLISHABLE_KEY"),
    )


@app.route("/landing")
def landing_page():
    """
    Public marketing landing page.
    Does not require login.
    """
    # lang is read in base.html via request.args, but we include it here
    lang = (request.args.get("lang") or "en").lower()
    if lang not in ("en", "es"):
        lang = "en"
    return render_template("landing.html", lang=lang)


@app.route("/launch-checklist")
def launch_checklist_page():
    """
    Public launch checklist page (for you, or to share with friends/advisors).
    """
    lang = (request.args.get("lang") or "en").lower()
    if lang not in ("en", "es"):
        lang = "en"
    return render_template("launch_checklist.html", lang=lang)


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/help")
def help_page():
    return render_template("help.html")


@app.route("/faq")
def faq_page():
    return render_template("faq.html")


@app.route("/changelog")
def changelog_page():
    return render_template("changelog.html")


# Stub route so url_for('create_checkout_session') works.
# The button can show "Coming soon" and just bounce back to pricing for now.


@app.route("/create-checkout-session", methods=["POST"])
def create_checkout_session():
    user = get_current_user()
    user_id = user.get("id")
    user_email = user.get("email")

    if not user_id:
        return jsonify({"error": "No logged-in user found."}), 401

    price_id = os.getenv("STRIPE_PRICE_PRO_MONTHLY") or os.getenv("STRIPE_PRICE_PRO")
    if not price_id:
        return jsonify({"error": "Missing STRIPE_PRICE_PRO_MONTHLY (or STRIPE_PRICE_PRO)"}), 500

    base_url = os.getenv("APP_BASE_URL", "").rstrip("/")
    if not base_url:
        base_url = request.host_url.rstrip("/")

    lang = request.args.get("lang", "en")

    try:
        checkout_session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": price_id, "quantity": 1}],
            allow_promotion_codes=True,

            # ✅ after payment we will verify session_id server-side and flip plan in DB
            success_url=f"{base_url}/billing/success?session_id={{CHECKOUT_SESSION_ID}}&lang={lang}",
            cancel_url=f"{base_url}/billing/cancel?lang={lang}",

            # ✅ stored on Checkout Session (used by billing_success + checkout.session.completed)
            metadata={
                "user_id": str(user_id),
                "user_email": user_email or "",
            },

            # ✅ stored on Subscription too (useful for subscription.updated events later)
            subscription_data={
                "metadata": {
                    "user_id": str(user_id),
                    "user_email": user_email or "",
                }
            },

            client_reference_id=str(user_id),
            customer_email=user_email if user_email else None,
        )

        return jsonify({"url": checkout_session.url})

    except Exception as e:
        print(f"[Stripe] create_checkout_session error: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


@app.route("/public/<string:token>/create-pay-session", methods=["POST"])
def create_public_invoice_pay_session(token):
    """
    Creates a Stripe Checkout Session (mode=payment) for the invoice BALANCE due.
    Called from the public invoice page "Pay Now" button.
    """
    base_url = os.getenv("APP_BASE_URL", "").rstrip("/") or request.host_url.rstrip("/")
    currency = (os.getenv("STRIPE_CURRENCY") or "usd").lower()

    inv = get_invoice_by_public_token(token)
    if not inv:
        return jsonify({"error": "Invoice not found."}), 404

    # Only allow Pay Now if the invoice owner is Pro+
    owner_plan = get_user_plan_by_user_id(inv["user_id"])
    if PLAN_LEVELS.get(owner_plan, 0) < PLAN_LEVELS.get("pro", 0):
        return jsonify({"error": "Pay Now is not enabled for this invoice."}), 403

    if inv["status"] == "Paid" or inv["balance"] <= 0:
        return jsonify({"error": "This invoice is already paid."}), 400

    # Stripe expects cents (integer)
    amount_cents = int(round(inv["balance"] * 100))
    if amount_cents < 50:  # Stripe min charge is typically $0.50 in many configs
        return jsonify({"error": "Balance too small to charge via Stripe."}), 400

    invoice_label = inv["invoice_number"] or f"#{inv['invoice_id']}"
    line_desc = f"Invoice {invoice_label} — Balance Due"

    try:
        checkout_session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            line_items=[
                {
                    "price_data": {
                        "currency": currency,
                        "unit_amount": amount_cents,
                        "product_data": {
                            "name": line_desc,
                        },
                    },
                    "quantity": 1,
                }
            ],
            # Redirect back to the public invoice page
            success_url=f"{base_url}/public/{token}?paid=1&session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{base_url}/public/{token}?canceled=1",

            # Critical metadata for webhook → DB update
            metadata={
                "kind": "invoice_payment",
                "invoice_id": str(inv["invoice_id"]),
                "public_token": token,
                "invoice_user_id": str(inv["user_id"]),
            },
        )

        return jsonify({"url": checkout_session.url})

    except Exception as e:
        print(f"[Stripe] create_public_invoice_pay_session error: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


@app.route("/billing/success")
def billing_success():
    session_id = request.args.get("session_id")
    lang = request.args.get("lang", "en")

    if not session_id:
        return "Missing session_id", 400

    try:
        cs = stripe.checkout.Session.retrieve(session_id)

        metadata = cs.get("metadata") or {}
        user_id_str = metadata.get("user_id") or cs.get("client_reference_id")

        customer_id = cs.get("customer")
        subscription_id = cs.get("subscription")

        print(
            f"[BillingSuccess] session_id={session_id} user_id_str={user_id_str} "
            f"customer_id={customer_id} subscription_id={subscription_id} metadata={metadata}",
            flush=True,
        )

        if not user_id_str:
            print("[BillingSuccess] No user_id on session; cannot upgrade", flush=True)
            return redirect(url_for("pricing", lang=lang, canceled=1))

        user_id = int(user_id_str)

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE users
            SET plan = %s,
                stripe_customer_id = COALESCE(%s, stripe_customer_id),
                stripe_subscription_id = COALESCE(%s, stripe_subscription_id)
            WHERE id = %s
            """,
            ("pro", customer_id, subscription_id, user_id),
        )
        conn.commit()
        updated = cursor.rowcount
        cursor.close()
        conn.close()

        print(f"[BillingSuccess] DB updated rows={updated} for user_id={user_id}", flush=True)

        # If rowcount is 0, you're upgrading a user_id that doesn't exist in DB
        if updated == 0:
            print("[BillingSuccess] WARNING: No user row matched. Check user_id.", flush=True)
            return redirect(url_for("pricing", lang=lang, canceled=1))

    except Exception as e:
        print(f"[BillingSuccess] error: {e}", flush=True)
        return redirect(url_for("pricing", lang=lang, canceled=1))

    return redirect(url_for("pricing", lang=lang, upgraded=1))


@app.route("/billing/cancel")
def billing_cancel():
    lang = request.args.get("lang", "en")
    return redirect(url_for("pricing", canceled=1, lang=lang))


# -------------------------
# PREVIEW (optional)
# -------------------------
@app.route("/preview", methods=["POST"])
def preview_invoice():
    """
    Live preview of an invoice BEFORE saving.
    Mirrors most of the /save logic but does NOT write to the database.
    """
    # ---- CLIENT RESOLUTION (same idea as /save) ----
    selected_client_id = request.form.get("client_id")
    new_client_name = request.form.get("new_client_name")
    new_client_email = request.form.get("new_client_email")
    new_client_company = request.form.get("new_client_company")
    new_client_phone = request.form.get("new_client_phone")
    new_client_address = request.form.get("new_client_address")
    new_client_notes = request.form.get("new_client_notes")

    notes = request.form.get("invoice_notes") or ""
    terms = request.form.get("invoice_terms") or "Payment due within 30 days."
    template_style = request.form.get("template_style") or "modern"

    # 🔹 NEW: signature preview
    signature_data = request.form.get("signature_data") or ""

    descriptions = request.form.getlist("description")
    amounts = request.form.getlist("amount")

    created_at = datetime.now()
    due_date = created_at + timedelta(days=30)

    items = []
    total = 0.0

    for desc, amt in zip(descriptions, amounts):
        if desc and amt:
            try:
                amt_val = float(amt)
            except ValueError:
                continue
            total += amt_val
            items.append((desc, amt_val))

    user = get_current_user()
    user_id = user["id"]

    # Figure out what name/email would be used for this invoice
    display_client_name = None
    display_client_email = new_client_email or ""
    display_client_company = new_client_company or ""
    display_client_phone = new_client_phone or ""
    display_client_address = new_client_address or ""
    display_client_notes = new_client_notes or ""

    conn = get_db_connection()
    cursor = conn.cursor()

    if selected_client_id:
        try:
            cid_int = int(selected_client_id)
            cursor.execute(
                """
                SELECT name, email, company, phone, address, notes
                FROM clients
                WHERE id = %s AND user_id = %s
                """,
                (cid_int, user_id),
            )
            row = cursor.fetchone()
            if row:
                (
                    db_name,
                    db_email,
                    db_company,
                    db_phone,
                    db_address,
                    db_notes,
                ) = row
                display_client_name = db_name
                display_client_email = display_client_email or (db_email or "")
                display_client_company = display_client_company or (db_company or "")
                display_client_phone = display_client_phone or (db_phone or "")
                display_client_address = display_client_address or (db_address or "")
                display_client_notes = display_client_notes or (db_notes or "")
        except ValueError:
            pass

    if not display_client_name and new_client_name:
        display_client_name = new_client_name

    if not display_client_name:
        display_client_name = "Unknown client"

    cursor.close()
    conn.close()

    # Business profile for branding
    profile = get_business_profile()
    business_name = profile.get("business_name") or "BillBeam"

    # "Virtual" invoice label (not saved yet)
    invoice_label = "PREVIEW — Not saved"

    return render_template(
        "preview.html",
        business_profile=profile,
        business_name=business_name,
        client_name=display_client_name,
        client_email=display_client_email,
        client_company=display_client_company,
        client_phone=display_client_phone,
        client_address=display_client_address,
        client_notes=display_client_notes,
        created_at=created_at,
        due_date=due_date,
        items=items,
        total=total,
        notes=notes,
        terms=terms,
        template_style=template_style,
        invoice_label=invoice_label,
        signature_data=signature_data,  # 👈 show signature on preview
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
    - Stores optional client signature_data
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

    # 🔹 NEW: signature data (data URL from hidden field)
    signature_data = request.form.get("signature_data") or None

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

    # 🔹 NEW: store template_style & signature_data column
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
            user_id,
            signature_data
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            signature_data,
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

    # ---------------- KPIs (same idea as before) ----------------
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

    # ---------------- Monthly revenue chart (last 6 months) ----------------
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

    # ---------------- Daily revenue chart (last 30 days) ----------------
    cursor.execute(
        """
        SELECT DATE(created_at) AS day, SUM(amount) AS total
        FROM invoices
        WHERE user_id = %s
        GROUP BY day
        ORDER BY day DESC
        LIMIT 30
        """,
        (user_id,),
    )
    daily_rows = cursor.fetchall()

    daily_chart_labels = []
    daily_chart_totals = []
    for day_dt, total_amt in reversed(daily_rows):
        daily_chart_labels.append(day_dt.strftime("%b %d"))
        daily_chart_totals.append(float(total_amt))

    # ---------------- Status chart data ----------------
    status_chart_labels = ["Paid", "Sent", "Overdue"]
    status_chart_values = [
        paid_count,
        status_distribution.get("Sent", 0),
        overdue_count,
    ]

    # ---------------- Top clients (leaderboard) ----------------
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

    # ---------------- NEW: Top items / services (for Top Items chart) ----------------
    cursor.execute(
        """
        SELECT
            ii.description,
            SUM(ii.amount) AS total_billed,
            COUNT(*) AS line_count
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.id
        WHERE i.user_id = %s
        GROUP BY ii.description
        HAVING SUM(ii.amount) > 0
        ORDER BY total_billed DESC
        LIMIT 7
        """,
        (user_id,),
    )
    item_rows = cursor.fetchall()

    item_labels = []
    item_totals = []
    item_counts = []
    for desc, total_amt, line_count in item_rows:
        item_labels.append(desc or "Untitled item")
        item_totals.append(float(total_amt or 0))
        item_counts.append(line_count or 0)

    # ---------------- Filtered table query ----------------
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
        daily_chart_labels=daily_chart_labels,
        daily_chart_totals=daily_chart_totals,
        status_chart_labels=status_chart_labels,
        status_chart_values=status_chart_values,
        top_clients=top_clients,
        item_labels=item_labels,
        item_totals=item_totals,
        item_counts=item_counts,
    )


# -------------------------
# GLOBAL SEARCH
# -------------------------
@app.route("/search")
def global_search():
    q = (request.args.get("q") or "").strip()
    user = get_current_user()
    user_id = user["id"]

    client_results = []
    invoice_results = []

    if q:
        like = f"%{q.lower()}%"
        conn = get_db_connection()
        cursor = conn.cursor()

        # Clients search
        cursor.execute(
            """
            SELECT id, name, email, company, created_at
            FROM clients
            WHERE user_id = %s
              AND (
                    LOWER(name) LIKE %s
                 OR LOWER(COALESCE(email, '')) LIKE %s
                 OR LOWER(COALESCE(company, '')) LIKE %s
              )
            ORDER BY created_at DESC
            LIMIT 50
            """,
            (user_id, like, like, like),
        )
        client_results = cursor.fetchall()

        # Invoices search
        cursor.execute(
            """
            SELECT id, client, amount, created_at, status, invoice_number
            FROM invoices
            WHERE user_id = %s
              AND (
                    LOWER(client) LIKE %s
                 OR LOWER(COALESCE(invoice_number, '')) LIKE %s
                 OR LOWER(COALESCE(notes, '')) LIKE %s
                 OR LOWER(COALESCE(terms, '')) LIKE %s
              )
            ORDER BY created_at DESC
            LIMIT 50
            """,
            (user_id, like, like, like, like),
        )
        invoice_results = cursor.fetchall()

        conn.close()

    return render_template(
        "search.html",
        q=q,
        client_results=client_results,
        invoice_results=invoice_results,
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
# PDF GENERATION (template-aware, with signature)
# -------------------------
def generate_invoice_pdf_bytes(invoice_id: int):
    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT
            client,
            amount,
            created_at,
            due_date,
            invoice_number,
            template_style,
            notes,
            terms,
            signature_data
        FROM invoices
        WHERE id = %s
        """,
        (invoice_id,),
    )
    row = c.fetchone()

    if not row:
        conn.close()
        return None, "Invoice not found"

    (
        client,
        amount,
        created_at,
        due_date,
        invoice_number,
        template_style,
        notes,
        terms,
        signature_data,
    ) = row

    amount_float = float(amount)
    template_style = (template_style or "modern").lower()

    # Business name for header (fallback to BillBeam)
    profile = get_business_profile()
    business_name = profile.get("business_name") or "BillBeam"

    c.execute(
        "SELECT description, amount FROM invoice_items WHERE invoice_id = %s",
        (invoice_id,),
    )
    items = c.fetchall()
    conn.close()

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesizes=LETTER)
    page_width, page_height = LETTER

    # ---------- TEMPLATE STYLES ----------
    # Use brand/business name as the header title
    header_bar_color = (21 / 255, 27 / 255, 84 / 255)  # deep blue
    accent_color = header_bar_color
    title_text = business_name  # 👈 main title is your business name

    if template_style == "minimal":
        header_bar_color = (0.18, 0.20, 0.24)  # dark slate
        accent_color = (0.6, 0.6, 0.65)
    elif template_style == "bold":
        header_bar_color = (0.97, 0.45, 0.09)  # orange
        accent_color = (0.97, 0.45, 0.09)
    elif template_style == "doodle":
        header_bar_color = (0.33, 0.27, 0.96)  # purple/blue
        accent_color = (0.33, 0.27, 0.96)

        # Soft doodle-like shapes in background
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

    # ---------- INVOICE META ----------
    pdf.setFillColorRGB(0.1, 0.1, 0.15)
    pdf.setFont("Helvetica", 11)

    inv_label = invoice_number or f"#{invoice_id}"
    y = page_height - 90

    pdf.drawString(72, y, f"Invoice: {inv_label}")
    y -= 16
    pdf.drawString(72, y, f"Client: {client}")
    y -= 16

    if created_at:
        pdf.drawString(
            72,
            y,
            f"Created: {created_at.strftime('%Y-%m-%d %I:%M %p')}",
        )
        y -= 16
    if due_date:
        pdf.drawString(
            72,
            y,
            f"Due: {due_date.strftime('%Y-%m-%d')}",
        )
        y -= 16

    # Right side total box
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
        if y < 120:  # keep space for footer / notes
            pdf.showPage()
            page_width, page_height = LETTER
            y = page_height - 100
            pdf.setFont("Helvetica", 10)
            pdf.setFillColorRGB(0.1, 0.1, 0.15)

    # ---------- NOTES & TERMS ----------
    if y < 120:
        pdf.showPage()
        page_width, page_height = LETTER
        y = page_height - 100

    if notes:
        pdf.setFont("Helvetica-Bold", 11)
        pdf.setFillColorRGB(0.1, 0.1, 0.15)
        pdf.drawString(72, y, "Notes")
        y -= 16
        pdf.setFont("Helvetica", 10)
        pdf.drawString(72, y, notes[:120])
        y -= 20

    if terms:
        if y < 80:
            pdf.showPage()
            page_width, page_height = LETTER
            y = page_height - 100
        pdf.setFont("Helvetica-Bold", 11)
        pdf.setFillColorRGB(0.1, 0.1, 0.15)
        pdf.drawString(72, y, "Payment Terms")
        y -= 16
        pdf.setFont("Helvetica", 10)
        pdf.drawString(72, y, terms[:160])
        y -= 20

    # ---------- CLIENT SIGNATURE (if captured) ----------
    if signature_data:
        try:
            # signature_data is typically "data:image/png;base64,AAAA..."
            if signature_data.startswith("data:image"):
                _, b64_data = signature_data.split(",", 1)
            else:
                b64_data = signature_data

            sig_bytes = base64.b64decode(b64_data)
            sig_buf = io.BytesIO(sig_bytes)
            sig_img = ImageReader(sig_buf)

            # Ensure we have enough space; otherwise new page
            if y < 140:
                pdf.showPage()
                page_width, page_height = LETTER
                y = page_height - 160

            pdf.setFont("Helvetica-Bold", 11)
            pdf.setFillColorRGB(0.1, 0.1, 0.15)
            pdf.drawString(72, y, "Client Signature")
            y -= 10

            sig_box_height = 70
            sig_box_width = 200

            pdf.setStrokeColorRGB(0.8, 0.82, 0.86)
            pdf.rect(72, y - sig_box_height, sig_box_width, sig_box_height, fill=0, stroke=1)

            pdf.drawImage(
                sig_img,
                72 + 6,
                y - sig_box_height + 6,
                width=sig_box_width - 12,
                height=sig_box_height - 12,
                mask='auto'
            )

            y -= sig_box_height + 16
        except Exception:
            # If anything goes wrong with the signature, just skip it
            pass

    # ---------- TOTAL FOOTER ----------
    if y < 80:
        pdf.showPage()
        page_width, page_height = LETTER
        y = page_height - 120

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
            c.company,
            i.signature_data
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
        signature_data,
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
        signature_data=signature_data,  # 👈 make available to template
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
            c.company,
            i.signature_data
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
        signature_data,
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
        signature_data=signature_data,  # 👈 show signature on owner view
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
            "Set RESEND_FROM to something like 'BillBeam <billing@billbeam.com>'.",
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
    business_name = profile["business_name"] or "BillBeam"

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


# -------------------------
# AI HELPER SUPPORTING KPI SNAPSHOT
# -------------------------
def get_ai_kpi_summary_for_user(user_id: int) -> str:
    """
    Lightweight invoice KPIs for the AI helper so it can reason
    about your business without pulling full raw tables.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT amount, created_at, status
        FROM invoices
        WHERE user_id = %s
        ORDER BY created_at DESC
        """,
        (user_id,),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    total_invoices = len(rows)
    total_revenue = 0.0
    monthly_revenue = 0.0
    paid_count = 0
    overdue_count = 0
    sent_count = 0

    now = datetime.now()
    this_month = now.month
    this_year = now.year

    for amount, created_at, status in rows:
        amt = float(amount)
        total_revenue += amt

        if created_at and created_at.month == this_month and created_at.year == this_year:
            monthly_revenue += amt

        status = (status or "").strip()
        if status == "Paid":
            paid_count += 1
        elif status == "Overdue":
            overdue_count += 1
        elif status == "Sent":
            sent_count += 1

    if total_revenue <= 0:
        growth_pct = 0.0
    else:
        growth_pct = round((monthly_revenue / total_revenue) * 100, 1)

    return (
        f"Total invoices: {total_invoices}, "
        f"total revenue: ${total_revenue:,.2f}, "
        f"this month revenue: ${monthly_revenue:,.2f} "
        f"({growth_pct}% of all-time), "
        f"status counts → Paid: {paid_count}, Sent: {sent_count}, Overdue: {overdue_count}."
    )


# -------------------------
# AI HELPER ENDPOINT
# -------------------------
@app.route("/ai-helper", methods=["POST"])
def ai_helper():
    """
    JSON API that powers the BillBeam assistant.

    Request body: { "question": "...", "page": "/invoices" }
    Response: { "answer": "..." } or { "error": "..." }

    Behavior is plan-aware:
      - free: shorter, simpler responses + gentle upgrade nudges
      - pro / enterprise: deeper, more strategic guidance

    The helper also gets a small KPI snapshot of the current user's invoices
    so it can reason about revenue, paid vs overdue, etc.
    """
    if not ai_client or not OPENAI_API_KEY:
        return {"error": "AI helper is not configured on the server."}, 500

    data = request.get_json() or {}
    question = (data.get("question") or "").strip()
    page = (data.get("page") or "").strip()

    # Figure out the language for this AI request
    # Priority: JSON "lang" -> query param "lang" -> default "en"
    user_lang = (data.get("lang") or request.args.get("lang") or "en").lower()
    if user_lang not in ("en", "es"):
        user_lang = "en"

    if not question:
        return {"error": "Missing question."}, 400

    # Current user + plan
    user = get_current_user()
    user_plan = user.get("plan") or "free"
    user_id = user.get("id")
    is_pro = PLAN_LEVELS.get(user_plan, 0) >= PLAN_LEVELS.get("pro", 0)

    # KPI snapshot for this user (if we have a concrete user_id)
    kpi_summary = ""
    if user_id:
        try:
            kpi_summary = get_ai_kpi_summary_for_user(user_id)
        except Exception:
            # Don't break AI helper if KPI query fails
            kpi_summary = ""

    # Very small context string so the model knows the app shape
    app_context = f"""
You are the in-app assistant for an invoicing web app called BillBeam.

User language code for this request: '{user_lang}'.
- If it is 'es', you MUST respond in Spanish, with natural, clear business Spanish.
- If it is 'en' or anything else, respond in English.
- Do not mix both languages in the same answer unless explicitly asked.

Key features and routes:
- Create invoice at "/" (new invoice form with client + line items).
- Invoice history dashboard at "/invoices" with KPIs, charts, and status filters.
- Clients listing at "/clients".
- Settings / business profile at "/settings".
- Email an invoice with PDF at "/send-email/<id>" (Pro feature).
- Invoice statuses: "Sent", "Paid", "Overdue".
- There is a public invoice view for clients at "/public/<token>" with a "Download PDF" button.

User plan: {user_plan}.
Current page path: {page}.

High-level metrics for this user (if available):
{kpi_summary}

Guidance rules:
- If the plan is free, keep answers concise and practical. When the user asks about deep automation,
  recurring billing, or heavy AI workflows, gently suggest upgrading to Pro without being pushy.
- If the plan is Pro or Enterprise, provide more detailed, strategic help. Offer step-by-step workflows,
  follow-up suggestions, and concrete ideas for how to use existing features together.

You can:
- Explain how to use any of the routes/features described above.
- Suggest best practices for invoicing, following up on overdue invoices, or pricing services.
- Help the user interpret their KPIs and charts using the high-level metrics (but do NOT invent exact
  client names or invoice IDs that you were not given).
- Suggest better email wording for sending invoices or following up on unpaid ones.

Never:
- Invent new backend routes or database tables that are not mentioned here.
- Claim you can change subscription plans or charge cards directly.
"""

    # Choose model + length by plan
    model_name = AI_MODEL_PRO if is_pro else AI_MODEL_FREE
    max_output_tokens = 600 if is_pro else 220

    try:
        resp = ai_client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": app_context},
                {
                    "role": "user",
                    "content": question,
                },
            ],
            temperature=0.4,
            max_tokens=max_output_tokens,
        )
        answer = resp.choices[0].message.content.strip()
        return {"answer": answer}
    except Exception as e:
        # Don't blow up the app on AI issues
        return {"error": f"AI error: {e}"}, 500


@app.route("/api/ai-assistant", methods=["POST"])
def api_ai_assistant():
    data = request.get_json() or {}
    user_message = (data.get("message") or "").strip()
    lang = data.get("lang") or request.args.get("lang", "en")
    page = data.get("page") or ""
    extra = data.get("extra_context") or {}

    if not user_message:
        return jsonify({"error": "No message provided."}), 400

    # You can enrich this with actual data later (totals, top clients, etc.)
    # For now we give the model context about what BillBeam is.
    if lang == "es":
        system_prompt = (
            "Eres InvoicePro Assistant, un asistente amable y claro dentro de BillBeam, "
            "una app de facturación para freelancers y pequeños negocios. "
            "Tu trabajo es ayudar a los usuarios a:\n"
            "- Entender su panel de facturas y métricas (ingresos, estados, clientes principales).\n"
            "- Redactar notas de facturas, recordatorios de pago y correos de seguimiento.\n"
            "- Decidir cómo organizar conceptos (servicios, horas, productos) y términos de pago.\n"
            "- Dar sugerencias de buenas prácticas, siempre con un tono breve, calmado y práctico.\n"
            "Evita respuestas muy largas. No inventes datos numéricos específicos del usuario; "
            "si necesitas un número exacto, di que no puedes verlo directamente y sugiere dónde buscarlo en BillBeam. "
            "Nunca inventes información sobre pagos reales o clientes específicos."
        )
    else:
        system_prompt = (
            "You are InvoicePro Assistant, a warm, practical AI living inside BillBeam, "
            "an invoicing app for freelancers and small businesses. "
            "Your job is to help users:\n"
            "- Understand their invoice dashboard and metrics (revenue, statuses, top clients).\n"
            "- Draft invoice notes, payment reminders, and follow-up emails.\n"
            "- Decide how to structure line items (services, hours, products) and payment terms.\n"
            "- Suggest best practices in a short, calm, encouraging tone.\n"
            "Keep answers concise (a few short paragraphs max). "
            "Do not fabricate specific user numbers; if you’d need live data, say you can’t see it directly "
            "and point them to where in BillBeam they can check. "
            "Never make up details about specific clients or actual payments."
        )

    # Optional: include some context hints
    context_hint_parts = []
    if page:
        context_hint_parts.append(f"User is currently on the page: {page}.")
    if extra:
        context_hint_parts.append(f"Extra context: {extra}")

    context_hint = "\n".join(context_hint_parts)

    user_content = user_message
    if context_hint:
        user_content = context_hint + "\n\nUser question:\n" + user_message

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.4,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
        )
        reply = completion.choices[0].message.content
        return jsonify({"reply": reply})
    except Exception as e:
        # You can log this properly if you like
        print("AI error:", e)
        if lang == "es":
            msg = "Lo siento, el asistente tuvo un problema. Intenta de nuevo en un momento."
        else:
            msg = "Sorry, the assistant ran into a problem. Try again in a moment."
        return jsonify({"error": msg}), 500


@app.route("/stripe/webhook", methods=["POST"])
def stripe_webhook():
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

    event_type = event.get("type")
    print(f"[Stripe] Received event: {event_type}", flush=True)

    # -------------------------------------------------------
    # 1) Checkout completed => subscription OR invoice payment
    # -------------------------------------------------------
    if event_type == "checkout.session.completed":
        session_obj = event["data"]["object"]
        mode = (session_obj.get("mode") or "").lower()
        metadata = session_obj.get("metadata") or {}

        print(
            f"[Stripe] checkout.session.completed mode={mode} session_id={session_obj.get('id')} metadata={metadata}",
            flush=True
        )

        # -----------------------------
        # A) Subscription checkout -> upgrade user
        # -----------------------------
        if mode == "subscription":
            user_id_str = metadata.get("user_id") or session_obj.get("client_reference_id")
            stripe_customer_id = session_obj.get("customer")

            if not user_id_str:
                print("[Stripe] Subscription checkout missing user_id; cannot upgrade", flush=True)
                return "OK", 200

            try:
                user_id = int(user_id_str)
            except ValueError:
                print(f"[Stripe] Invalid user_id value: {user_id_str}", flush=True)
                return "OK", 200

            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                if stripe_customer_id:
                    cursor.execute(
                        "UPDATE users SET plan = %s, stripe_customer_id = %s WHERE id = %s",
                        ("pro", stripe_customer_id, user_id),
                    )
                else:
                    cursor.execute(
                        "UPDATE users SET plan = %s WHERE id = %s",
                        ("pro", user_id),
                    )
                conn.commit()
                updated = cursor.rowcount
                cursor.close()
                conn.close()
                print(f"[Stripe] Upgraded user {user_id} to Pro (rows_updated={updated})", flush=True)
            except Exception as e:
                print(f"[Stripe] DB error upgrading user {user_id}: {e}", flush=True)

            return "OK", 200

        # -----------------------------
        # B) One-time invoice payment -> record payment + mark paid
        # -----------------------------
        if mode == "payment" and metadata.get("kind") == "invoice_payment":
            invoice_id_str = metadata.get("invoice_id")
            token = metadata.get("public_token")
            payment_intent_id = session_obj.get("payment_intent")
            checkout_session_id = session_obj.get("id")

            # Amount is stored in cents on the session for payment mode
            amount_total_cents = session_obj.get("amount_total") or 0
            amount_paid = float(amount_total_cents) / 100.0

            payment_status = (session_obj.get("payment_status") or "").lower()
            if payment_status != "paid":
                print(f"[Stripe] Invoice payment not paid yet (status={payment_status})", flush=True)
                return "OK", 200

            if not invoice_id_str or not payment_intent_id:
                print("[Stripe] Missing invoice_id or payment_intent on invoice payment session", flush=True)
                return "OK", 200

            try:
                invoice_id = int(invoice_id_str)
            except ValueError:
                print(f"[Stripe] Invalid invoice_id: {invoice_id_str}", flush=True)
                return "OK", 200

            try:
                conn = get_db_connection()
                cursor = conn.cursor()

                # Idempotency: skip if we already recorded this payment intent
                cursor.execute(
                    "SELECT id FROM payments WHERE stripe_payment_intent_id = %s LIMIT 1",
                    (payment_intent_id,),
                )
                existing = cursor.fetchone()
                if existing:
                    print(f"[Stripe] Payment intent already recorded: {payment_intent_id}", flush=True)
                    cursor.close()
                    conn.close()
                    return "OK", 200

                # Insert payment record
                cursor.execute(
                    """
                    INSERT INTO payments (invoice_id, amount, method, note, stripe_payment_intent_id, stripe_checkout_session_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        invoice_id,
                        amount_paid,
                        "Stripe",
                        f"Stripe Checkout (session {checkout_session_id})",
                        payment_intent_id,
                        checkout_session_id,
                    ),
                )

                # Store last PI on invoice (optional, for debugging)
                cursor.execute(
                    "UPDATE invoices SET stripe_last_payment_intent_id = %s WHERE id = %s",
                    (payment_intent_id, invoice_id),
                )

                # Recompute totals and mark Paid if fully covered
                cursor.execute("SELECT amount FROM invoices WHERE id = %s", (invoice_id,))
                inv_row = cursor.fetchone()
                if not inv_row:
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    print(f"[Stripe] Invoice id not found in DB: {invoice_id}", flush=True)
                    return "OK", 200

                invoice_total = float(inv_row[0] or 0)

                cursor.execute(
                    "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE invoice_id = %s",
                    (invoice_id,),
                )
                total_paid = float(cursor.fetchone()[0] or 0)

                if total_paid >= invoice_total and invoice_total > 0:
                    cursor.execute(
                        "UPDATE invoices SET status = 'Paid' WHERE id = %s",
                        (invoice_id,),
                    )
                    print(
                        f"[Stripe] Marked invoice {invoice_id} as Paid. total_paid={total_paid} total={invoice_total}",
                        flush=True
                    )
                else:
                    print(
                        f"[Stripe] Payment recorded but invoice not fully paid. invoice={invoice_id} total_paid={total_paid} total={invoice_total}",
                        flush=True
                    )

                conn.commit()
                cursor.close()
                conn.close()

            except Exception as e:
                print(f"[Stripe] Error recording invoice payment: {e}", flush=True)

            return "OK", 200

        # Unknown checkout session mode -> ignore safely
        print(f"[Stripe] checkout.session.completed ignored mode={mode}", flush=True)
        return "OK", 200

    # -------------------------------------------------------
    # 2) Subscription changes => sync plan by stripe_customer_id
    # -------------------------------------------------------
    elif event_type in ("customer.subscription.updated", "customer.subscription.deleted"):
        sub = event["data"]["object"]
        customer_id = sub.get("customer")
        status = (sub.get("status") or "").lower()

        new_plan = "pro" if status in ("active", "trialing") else "free"

        print(
            f"[Stripe] Subscription sync customer={customer_id} sub={sub.get('id')} status={status} => plan={new_plan}",
            flush=True,
        )

        if not customer_id:
            print("[Stripe] Missing customer_id on subscription event", flush=True)
            return "OK", 200

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE users
                SET plan = %s,
                    stripe_subscription_id = %s
                WHERE stripe_customer_id = %s
                """,
                (new_plan, sub.get("id"), customer_id),
            )
            conn.commit()
            updated = cursor.rowcount
            cursor.close()
            conn.close()

            print(f"[Stripe] Subscription sync rows_updated={updated} for customer_id={customer_id}", flush=True)
        except Exception as e:
            print(f"[Stripe] subscription sync error: {e}", flush=True)

    return "OK", 200


# -------------------------
# HEALTHCHECK
# -------------------------
@app.route("/health")
def health():
    return "OK", 200


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, 'static'),
        'favicon.ico',
        mimetype='image/vnd.microsoft.icon'
    )


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)