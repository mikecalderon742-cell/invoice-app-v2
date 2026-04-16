from flask import (
    Flask,
    render_template,
    request,
    send_file,
    redirect,
    session,
    url_for,
    send_from_directory,
    jsonify,
    g,
)
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse
from email.message import EmailMessage
from functools import wraps

import base64
import hashlib
import io
import logging
import os
import requests
import secrets
import smtplib

import psycopg2
import stripe
from openai import OpenAI
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from werkzeug.security import generate_password_hash, check_password_hash

# -------------------------
# APP / BRAND
# -------------------------
APP_NAME = "BillBeam"
DEFAULT_BUSINESS_NAME = "BillBeam"
DEFAULT_BRAND_COLOR = "#020617"
DEFAULT_ACCENT_COLOR = "#3A8BFF"
ALLOWED_STATUSES = {"Sent", "Paid", "Overdue"}

PUBLIC_VIEW_DEDUPE_MINUTES = int(os.environ.get("PUBLIC_VIEW_DEDUPE_MINUTES", "30"))
DEFAULT_TAX_RESERVE_PERCENT = 25.0

PAYMENT_METHOD_LABELS = {
    "stripe": "Stripe",
    "card": "Card",
    "cash": "Cash",
    "check": "Check",
    "ach": "ACH",
    "bank transfer": "Bank transfer",
    "manual": "Manual entry",
    "terminal": "Terminal",
    "tap_to_pay": "Tap to Pay",
}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("billbeam")

# -------------------------
# ENV / CONFIG
# -------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-me")

c = os.environ.get("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY")

APPLE_IAP_SIMPLE_PRODUCT_ID = os.environ.get("APPLE_IAP_SIMPLE_PRODUCT_ID", "app.billbeam.simple.monthly")
APPLE_IAP_PRO_PRODUCT_ID = os.environ.get("APPLE_IAP_PRO_PRODUCT_ID", "app.billbeam.pro.monthly")
APPLE_IAP_ENTERPRISE_PRODUCT_ID = os.environ.get("APPLE_IAP_ENTERPRISE_PRODUCT_ID", "app.billbeam.business.monthly")

STRIPE_PRICE_SIMPLE = (
    os.environ.get("STRIPE_PRICE_SIMPLE")
    or os.environ.get("STRIPE_PRICE_SIMPLE_MONTHLY")
)

STRIPE_PRICE_PRO = os.environ.get("STRIPE_PRICE_PRO") or os.environ.get("STRIPE_PRICE_PRO_MONTHLY")

STRIPE_PRICE_ENTERPRISE = (
    os.environ.get("STRIPE_PRICE_ENTERPRISE")
    or os.environ.get("STRIPE_PRICE_BUSINESS")
    or os.environ.get("STRIPE_PRICE_ENTERPRISE_MONTHLY")
    or os.environ.get("STRIPE_PRICE_BUSINESS_MONTHLY")
)

STRIPE_CURRENCY = (os.environ.get("STRIPE_CURRENCY") or "usd").lower()

APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "").rstrip("/")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENAI_AI_KEY")
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
ai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

AI_MODEL_FREE = os.environ.get("AI_MODEL_FREE", "gpt-4o-mini")
AI_MODEL_PRO = os.environ.get("AI_MODEL_PRO", "gpt-4.1-mini")
AI_NOTICE_ENABLED = os.environ.get("AI_NOTICE_ENABLED", "true").lower() in ("1", "true", "yes", "on")

APP_TIMEZONE = ZoneInfo(os.environ.get("APP_TIMEZONE", "America/Los_Angeles"))
IS_PRODUCTION = os.environ.get("FLASK_ENV", "").lower() == "production" or os.environ.get("APP_ENV", "").lower() == "production"
IS_DEBUG_MODE = os.environ.get("FLASK_DEBUG", "").lower() in ("1", "true", "yes", "on") or not IS_PRODUCTION

# -------------------------
# APP SECURITY / SESSION
# -------------------------
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = IS_PRODUCTION
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=14)

if SECRET_KEY == "dev-secret-change-me":
    logger.warning("SECRET_KEY is using the development fallback. Set a strong SECRET_KEY before launch.")

STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY
else:
    logger.warning("STRIPE_SECRET_KEY is not set")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}


# -------------------------
# TIME / PARSING HELPERS
# -------------------------
def now_local():
    """
    Returns the app's local current time as a naive datetime so it plays nicely
    with existing PostgreSQL TIMESTAMP columns.
    """
    return datetime.now(APP_TIMEZONE).replace(tzinfo=None)


def normalize_lang(value: str) -> str:
    value = (value or "en").strip().lower()
    return value if value in ("en", "es", "zh") else "en"


def get_request_lang(default: str = "en") -> str:
    """
    Resolve the current request language in one safe place.

    Priority:
    1) explicit ?lang=
    2) POSTed hidden/input lang
    3) session-stored lang
    4) logged-in user's saved language
    5) default fallback
    """
    query_lang = request.args.get("lang")
    form_lang = request.form.get("lang") if request.method == "POST" else None
    session_lang = session.get("lang")

    user_lang = None
    try:
        user_lang = (get_current_user() or {}).get("language")
    except Exception:
        user_lang = None

    return normalize_lang(query_lang or form_lang or session_lang or user_lang or default)


def lang_url_for(endpoint: str, **values) -> str:
    """
    Build a url_for() that always carries the active language unless
    the caller explicitly passed lang already.
    """
    if "lang" not in values or not values.get("lang"):
        values["lang"] = get_request_lang()
    else:
        values["lang"] = normalize_lang(values["lang"])
    return url_for(endpoint, **values)


def lang_redirect(endpoint: str, **values):
    """
    Safe redirect helper that preserves the active language.
    """
    return redirect(lang_url_for(endpoint, **values))


def t(key: str, lang: str = "en") -> str:
    lang = normalize_lang(lang)

    translations = {
        "create_invoice": {
            "en": "Create Invoice",
            "es": "Crear factura",
            "zh": "创建发票",
        },
        "save_invoice": {
            "en": "Save Invoice",
            "es": "Guardar factura",
            "zh": "保存发票",
        },
        "mark_paid": {
            "en": "Mark as Paid",
            "es": "Marcar como pagada",
            "zh": "标记为已支付",
        },
        "business_profile_branding": {
            "en": "Business Profile & Branding",
            "es": "Perfil de negocio y marca",
            "zh": "企业资料与品牌",
        },
        "language_preferences": {
            "en": "Language Preferences",
            "es": "Preferencias de idioma",
            "zh": "语言偏好",
        },
        "preferred_language": {
            "en": "Preferred Language",
            "es": "Idioma preferido",
            "zh": "首选语言",
        },
        "manage_services": {
            "en": "Manage Services",
            "es": "Administrar servicios",
            "zh": "管理服务",
        },
        "service_name": {
            "en": "Service Name",
            "es": "Nombre del servicio",
            "zh": "服务名称",
        },
        "service_description": {
            "en": "Description",
            "es": "Descripción",
            "zh": "描述",
        },
        "service_price": {
            "en": "Price",
            "es": "Precio",
            "zh": "价格",
        },
        "add_service": {
            "en": "Add Service",
            "es": "Agregar servicio",
            "zh": "添加服务",
        },
        "save_settings": {
            "en": "Save Settings",
            "es": "Guardar configuración",
            "zh": "保存设置",
        },
        "services": {
            "en": "Services",
            "es": "Servicios",
            "zh": "服务",
        },
        "active": {
            "en": "Active",
            "es": "Activo",
            "zh": "启用",
        },
        "inactive": {
            "en": "Inactive",
            "es": "Inactivo",
            "zh": "停用",
        },
        "edit": {
            "en": "Edit",
            "es": "Editar",
            "zh": "编辑",
        },
        "update": {
            "en": "Update",
            "es": "Actualizar",
            "zh": "更新",
        },
        "cancel": {
            "en": "Cancel",
            "es": "Cancelar",
            "zh": "取消",
        },
    }

    return translations.get(key, {}).get(lang, translations.get(key, {}).get("en", key))


def parse_float(value, default=0.0):
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def clean_percent(value, default=25.0):
    pct = parse_float(value, default)
    if pct < 0:
        return 0.0
    if pct > 100:
        return 100.0
    return pct


def format_currency(amount):
    try:
        return f"${float(amount or 0):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def normalize_method_label(method: str) -> str:
    raw = (method or "").strip()
    if not raw:
        return "Manual entry"
    return PAYMENT_METHOD_LABELS.get(raw.lower(), raw)


def normalize_public_client_key(request_obj, token: str) -> str:
    ip = (request_obj.headers.get("X-Forwarded-For") or request_obj.remote_addr or "").split(",")[0].strip()
    ua = request_obj.headers.get("User-Agent", "") or ""
    basis = f"{token}|{ip}|{ua[:160]}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def short_datetime(dt):
    if not dt:
        return ""
    try:
        return dt.strftime("%Y-%m-%d %I:%M %p")
    except Exception:
        return ""


def money_to_cents(amount: float) -> int:
    return int(round(float(amount or 0) * 100))


def normalize_plan_key(plan_value: str) -> str:
    plan_value = (plan_value or "free").strip().lower()
    aliases = {
        "starter": "free",
        "free": "free",
        "simple": "simple",
        "receipt": "simple",
        "receipts": "simple",
        "pro": "pro",
        "business": "enterprise",
        "studio": "enterprise",
        "enterprise": "enterprise",
    }
    return aliases.get(plan_value, "free")


def get_price_id_for_plan(plan_key: str):
    plan_key = normalize_plan_key(plan_key)
    if plan_key == "simple":
        return STRIPE_PRICE_SIMPLE
    if plan_key == "pro":
        return STRIPE_PRICE_PRO
    if plan_key == "enterprise":
        return STRIPE_PRICE_ENTERPRISE
    return None


def get_plan_for_apple_product_id(product_id: str):
    product_id = (product_id or "").strip()

    if not product_id:
        return None

    if product_id == APPLE_IAP_SIMPLE_PRODUCT_ID:
        return "simple"

    if product_id == APPLE_IAP_PRO_PRODUCT_ID:
        return "pro"

    if product_id == APPLE_IAP_ENTERPRISE_PRODUCT_ID:
        return "enterprise"

    return None


def resolve_plan_key(user_or_plan=None) -> str:
    if isinstance(user_or_plan, dict):
        return normalize_plan_key(user_or_plan.get("plan") or "free")
    if isinstance(user_or_plan, str):
        return normalize_plan_key(user_or_plan)
    return normalize_plan_key(get_plan_for_current_user())


def is_simple(user_or_plan=None) -> bool:
    return resolve_plan_key(user_or_plan) == "simple"


def can_email_invoices(user_or_plan=None) -> bool:
    return resolve_plan_key(user_or_plan) in ("simple", "pro", "enterprise")


def can_collect_payments(user_or_plan=None) -> bool:
    return resolve_plan_key(user_or_plan) in ("pro", "enterprise")


def can_use_ai(user_or_plan=None) -> bool:
    return resolve_plan_key(user_or_plan) in ("pro", "enterprise")


def can_use_advanced_dashboard(user_or_plan=None) -> bool:
    return resolve_plan_key(user_or_plan) in ("pro", "enterprise")


def can_use_collections(user_or_plan=None) -> bool:
    return resolve_plan_key(user_or_plan) in ("pro", "enterprise")


def can_use_branding(user_or_plan=None) -> bool:
    return resolve_plan_key(user_or_plan) in ("pro", "enterprise")


# -------------------------
# DB CONNECTION
# -------------------------
def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set.")

    result = urlparse(DATABASE_URL)
    return psycopg2.connect(
        dbname=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port,
    )


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
            "Up to 3 invoices / month",
            "Single invoice template",
            "Basic dashboard",
        ],
        "features_es": [
            "Hasta 3 facturas al mes",
            "Una sola plantilla de factura",
            "Panel básico",
        ],
    },
    "simple": {
        "name_en": "Simple",
        "name_es": "Simple",
        "price_label": "$9.99 / month",
        "tagline_en": "For users who already got paid and just need invoices and receipts.",
        "tagline_es": "Para usuarios que ya cobraron y solo necesitan facturas y recibos.",
        "features_en": [
            "Unlimited invoices",
            "Email delivery + PDFs",
            "Public invoice links (view only)",
            "Mark invoices as paid manually",
            "No Stripe payment setup required",
        ],
        "features_es": [
            "Facturas ilimitadas",
            "Envío por email + PDFs",
            "Enlaces públicos de factura (solo vista)",
            "Marcar facturas como pagadas manualmente",
            "No requiere configuración de pagos con Stripe",
        ],
        "recommended": True,
    },
    "pro": {
        "name_en": "Pro",
        "name_es": "Pro",
        "price_label": "$19.99 / month",
        "tagline_en": "For freelancers and small businesses who invoice regularly.",
        "tagline_es": "Para freelancers y pequeños negocios que facturan con frecuencia.",
        "features_en": [
            "Everything in Simple",
            "Stripe payments",
            "Public invoice links & Pay Now",
            "Advanced dashboard insights",
            "BillBeam Assistant",
            "Reminder workflows",
        ],
        "features_es": [
            "Todo lo incluido en Simple",
            "Pagos con Stripe",
            "Enlaces públicos de factura y botón Pagar ahora",
            "Panel avanzado con métricas",
            "BillBeam Assistant",
            "Flujos de recordatorio",
        ],
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

PLAN_LEVELS = {
    "free": 1,
    "starter": 1,
    "simple": 2,
    "pro": 3,
    "business": 4,
    "studio": 4,
    "enterprise": 4,
}


# -------------------------
# DATABASE INITIALIZATION
# -------------------------
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT,
            plan TEXT DEFAULT 'free',
            is_active BOOLEAN DEFAULT TRUE,
            stripe_customer_id TEXT,
            stripe_subscription_id TEXT,
            language TEXT DEFAULT 'en',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS invoices (
            id SERIAL PRIMARY KEY,
            client TEXT NOT NULL,
            amount NUMERIC NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'Sent',
            invoice_number TEXT,
            due_date TIMESTAMP,
            notes TEXT,
            terms TEXT,
            last_emailed_at TIMESTAMP,
            last_emailed_to TEXT,
            public_token TEXT UNIQUE,
            signature_data TEXT,
            template_style TEXT,
            client_id INTEGER,
            user_id INTEGER,
            stripe_last_payment_intent_id TEXT
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_id INTEGER
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
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            invoice_id INTEGER REFERENCES invoices(id) ON DELETE CASCADE,
            amount NUMERIC NOT NULL,
            method TEXT,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            stripe_payment_intent_id TEXT,
            stripe_checkout_session_id TEXT
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
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_id INTEGER
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS services (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            price NUMERIC(10,2),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_events (
            id SERIAL PRIMARY KEY,
            invoice_id INTEGER REFERENCES invoices(id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            title TEXT NOT NULL,
            details TEXT,
            visibility TEXT DEFAULT 'private',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS service_requests (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            client_id INTEGER,
            service_id INTEGER,
            invoice_id INTEGER,

            status TEXT NOT NULL DEFAULT 'requested',
            request_type TEXT DEFAULT 'request',
            source TEXT DEFAULT 'public',

            service_title_snapshot TEXT,
            service_description_snapshot TEXT,
            service_price_snapshot NUMERIC(10,2),

            client_name TEXT NOT NULL,
            client_email TEXT NOT NULL,
            client_phone TEXT,

            request_details TEXT,
            preferred_date_text TEXT,
            preferred_time_text TEXT,
            quantity INTEGER DEFAULT 1,

            intake_answers_json TEXT,

            owner_notes TEXT,
            client_notes TEXT,

            cancel_requested_by_client BOOLEAN DEFAULT FALSE,
            cancel_reason TEXT,

            approved_at TIMESTAMP,
            in_progress_at TIMESTAMP,
            completed_at TIMESTAMP,
            cancelled_at TIMESTAMP,
            converted_to_invoice_at TIMESTAMP,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS service_request_events (
            id SERIAL PRIMARY KEY,
            service_request_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS service_requests_user_idx
        ON service_requests(user_id);
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS service_requests_status_idx
        ON service_requests(status);
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS service_requests_service_idx
        ON service_requests(service_id);
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS service_requests_client_email_idx
        ON service_requests(client_email);
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS service_request_events_request_idx
        ON service_request_events(service_request_id, created_at DESC);
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            notification_type TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT,
            link_url TEXT,
            is_read BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS notifications_user_read_idx
        ON notifications(user_id, is_read, created_at DESC);
        """
    )

    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_connect_account_id TEXT;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_connect_charges_enabled BOOLEAN DEFAULT FALSE;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_connect_payouts_enabled BOOLEAN DEFAULT FALSE;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_connect_details_submitted BOOLEAN DEFAULT FALSE;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_connect_onboarded_at TIMESTAMP;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_connect_last_status_sync TIMESTAMP;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS apple_product_id TEXT;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS apple_original_transaction_id TEXT;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS apple_transaction_id TEXT;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS apple_last_purchase_at TIMESTAMP;")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS language TEXT DEFAULT 'en';")

    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS invoice_number TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS due_date TIMESTAMP;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS notes TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS terms TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS last_emailed_at TIMESTAMP;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS last_emailed_to TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS public_token TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS signature_data TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS template_style TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS client_id INTEGER;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS user_id INTEGER;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS stripe_last_payment_intent_id TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS first_viewed_at TIMESTAMP;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS last_viewed_at TIMESTAMP;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS view_count INTEGER DEFAULT 0;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS last_reminder_sent_at TIMESTAMP;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS last_collection_action_at TIMESTAMP;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS last_payment_recorded_at TIMESTAMP;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS tax_reserve_percent NUMERIC;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payment_terms_label TEXT;")
    cursor.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS collect_in_person_enabled BOOLEAN DEFAULT FALSE;")

    cursor.execute("ALTER TABLE clients ADD COLUMN IF NOT EXISTS user_id INTEGER;")
    cursor.execute("ALTER TABLE business_profile ADD COLUMN IF NOT EXISTS user_id INTEGER;")

    cursor.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS stripe_payment_intent_id TEXT;")
    cursor.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS stripe_checkout_session_id TEXT;")
    cursor.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS payment_source TEXT;")
    cursor.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS payment_status TEXT;")
    cursor.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS occurred_at TIMESTAMP;")
    cursor.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS recorded_by_user_id INTEGER;")
    cursor.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS is_deposit BOOLEAN DEFAULT FALSE;")
    cursor.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS is_final_payment BOOLEAN DEFAULT FALSE;")

    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS payments_stripe_pi_unique
        ON payments(stripe_payment_intent_id)
        WHERE stripe_payment_intent_id IS NOT NULL;
        """
    )
    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS payments_stripe_cs_unique
        ON payments(stripe_checkout_session_id)
        WHERE stripe_checkout_session_id IS NOT NULL;
        """
    )
    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS invoices_public_token_unique
        ON invoices(public_token)
        WHERE public_token IS NOT NULL;
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS invoice_events_invoice_created_idx
        ON invoice_events(invoice_id, created_at DESC);
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS invoices_last_viewed_idx
        ON invoices(last_viewed_at DESC);
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS invoices_last_reminder_idx
        ON invoices(last_reminder_sent_at DESC);
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS services_user_created_idx
        ON services(user_id, created_at DESC);
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS services_user_active_idx
        ON services(user_id, is_active);
        """
    )

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

        cursor.execute("UPDATE invoices SET user_id = %s WHERE user_id IS NULL;", (default_user_id,))
        cursor.execute("UPDATE clients SET user_id = %s WHERE user_id IS NULL;", (default_user_id,))
        cursor.execute("UPDATE business_profile SET user_id = %s WHERE user_id IS NULL;", (default_user_id,))

    conn.commit()
    cursor.close()
    conn.close()


def update_overdue_statuses():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            i.id,
            i.amount,
            i.status,
            i.due_date,
            COALESCE(SUM(CASE WHEN COALESCE(p.payment_status, 'succeeded') != 'failed' THEN p.amount ELSE 0 END), 0) AS total_paid
        FROM invoices i
        LEFT JOIN payments p ON p.invoice_id = i.id
        GROUP BY i.id, i.amount, i.status, i.due_date
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    for invoice_id, amount, status, due_date, total_paid in rows:
        amount = float(amount or 0)
        total_paid = float(total_paid or 0)
        balance = max(amount - total_paid, 0.0)

        if balance <= 0.0001:
            new_status = "Paid"
        elif due_date and due_date < now_local():
            new_status = "Overdue"
        else:
            new_status = "Sent"

        if status != new_status:
            conn2 = get_db_connection()
            cur2 = conn2.cursor()
            cur2.execute("UPDATE invoices SET status = %s WHERE id = %s", (new_status, invoice_id))
            conn2.commit()
            cur2.close()
            conn2.close()


def log_invoice_event(invoice_id: int, event_type: str, title: str, details: str = "", visibility: str = "private"):
    if visibility not in ("private", "public", "both"):
        visibility = "private"

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO invoice_events (invoice_id, event_type, title, details, visibility, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (invoice_id, event_type, title, details or "", visibility, now_local()),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning("Failed to log invoice event for invoice_id=%s: %s", invoice_id, e)
    finally:
        cur.close()
        conn.close()


def get_invoice_events(invoice_id: int, public_only: bool = False):
    conn = get_db_connection()
    cur = conn.cursor()

    if public_only:
        cur.execute(
            """
            SELECT event_type, title, details, created_at, visibility
            FROM invoice_events
            WHERE invoice_id = %s
              AND visibility IN ('public', 'both')
            ORDER BY created_at DESC, id DESC
            """,
            (invoice_id,),
        )
    else:
        cur.execute(
            """
            SELECT event_type, title, details, created_at, visibility
            FROM invoice_events
            WHERE invoice_id = %s
            ORDER BY created_at DESC, id DESC
            """,
            (invoice_id,),
        )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    events = []
    for event_type, title, details, created_at, visibility in rows:
        events.append(
            {
                "event_type": event_type,
                "title": title,
                "details": details or "",
                "created_at": created_at,
                "visibility": visibility,
            }
        )
    return events


def get_or_create_public_token(invoice_id: int) -> str:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT public_token FROM invoices WHERE id = %s", (invoice_id,))
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
        cursor.execute("SELECT id FROM invoices WHERE public_token = %s", (candidate,))
        clash = cursor.fetchone()
        if not clash:
            token = candidate
            break

    cursor.execute("UPDATE invoices SET public_token = %s WHERE id = %s", (token, invoice_id))
    conn.commit()
    cursor.close()
    conn.close()
    return token


# -------------------------
# INVOICE PAYMENT / STATUS HELPERS
# -------------------------
def mark_invoice_paid(invoice_id: int, user_id: int, note: str = "Marked as paid manually."):
    payment_summary = get_invoice_payment_summary(invoice_id)
    if not payment_summary:
        return False, "Invoice not found."

    balance = float(payment_summary.get("balance") or 0)

    if balance <= 0.0001:
        sync_invoice_status(invoice_id)
        return True, None

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        occurred_at = now_local()

        cur.execute(
            """
            INSERT INTO payments (
                invoice_id,
                amount,
                method,
                note,
                payment_source,
                payment_status,
                occurred_at,
                recorded_by_user_id,
                is_deposit,
                is_final_payment
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                invoice_id,
                balance,
                "manual",
                note,
                "manual",
                "succeeded",
                occurred_at,
                user_id,
                False,
                True,
            ),
        )

        cur.execute(
            """
            UPDATE invoices
            SET last_payment_recorded_at = %s,
                last_collection_action_at = %s
            WHERE id = %s
            """,
            (occurred_at, occurred_at, invoice_id),
        )

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.exception("Failed to mark invoice %s as paid manually: %s", invoice_id, e)
        return False, "Failed to mark invoice as paid."
    finally:
        cur.close()
        conn.close()

    summary = sync_invoice_status(invoice_id) or get_invoice_payment_summary(invoice_id) or {}
    total_paid_now = float(summary.get("total_paid") or 0)

    log_invoice_event(
        invoice_id=invoice_id,
        event_type="manual_payment_added",
        title="Payment recorded",
        details=f"Invoice was marked as paid manually. Total paid is now {format_currency(total_paid_now)}.",
        visibility="both",
    )

    log_invoice_event(
        invoice_id=invoice_id,
        event_type="final_payment_received",
        title="Final payment received",
        details="Invoice is now paid in full.",
        visibility="both",
    )

    return True, None


# -------------------------
# USER + PLAN HELPERS
# -------------------------
def get_default_user():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, email, plan, is_active, language, created_at FROM users ORDER BY id ASC LIMIT 1;"
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

    user_id, email, plan, is_active, language, created_at = row
    return {
        "id": user_id,
        "email": email,
        "plan": normalize_plan_key(plan or "free"),
        "is_active": is_active,
        "language": normalize_lang(language or "en"),
        "created_at": created_at,
    }


def get_current_user():
    user_id = session.get("user_id")
    if user_id:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, email, plan, is_active, language, created_at FROM users WHERE id = %s",
            (user_id,),
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            uid, email, plan, is_active, language, created_at = row
            if is_active:
                return {
                    "id": uid,
                    "email": email,
                    "plan": normalize_plan_key(plan or "free"),
                    "is_active": is_active,
                    "language": normalize_lang(language or "en"),
                    "created_at": created_at,
                }

    return {
        "id": None,
        "email": "",
        "plan": "free",
        "is_active": False,
        "language": "en",
        "created_at": None,
    }


def login_required(view_func):
    @wraps(view_func)
    def wrapped_view(*args, **kwargs):
        user = get_current_user()
        if not user.get("id"):
            return lang_redirect("login")
        return view_func(*args, **kwargs)
    return wrapped_view


def get_plan_for_current_user():
    user = get_current_user()
    return normalize_plan_key(user.get("plan") or "free")


def get_user_plan_by_user_id(user_id: int) -> str:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT plan FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return normalize_plan_key(row[0] if row and row[0] else "free")


def get_invoice_by_public_token(token: str):
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


def get_invoice_payment_summary(invoice_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            i.amount,
            i.status,
            i.due_date,
            i.invoice_number,
            COALESCE(SUM(CASE WHEN COALESCE(p.payment_status, 'succeeded') != 'failed' THEN p.amount ELSE 0 END), 0) AS total_paid,
            COUNT(p.id) FILTER (WHERE COALESCE(p.payment_status, 'succeeded') != 'failed') AS payment_count,
            MAX(COALESCE(p.occurred_at, p.created_at)) FILTER (WHERE COALESCE(p.payment_status, 'succeeded') != 'failed') AS last_payment_at
        FROM invoices i
        LEFT JOIN payments p ON p.invoice_id = i.id
        WHERE i.id = %s
        GROUP BY i.id, i.amount, i.status, i.due_date, i.invoice_number
        """,
        (invoice_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    amount_total, status, due_date, invoice_number, total_paid, payment_count, last_payment_at = row
    amount_total = float(amount_total or 0)
    total_paid = float(total_paid or 0)
    balance = max(amount_total - total_paid, 0.0)
    percent_paid = round((total_paid / amount_total) * 100, 1) if amount_total > 0 else 0.0

    return {
        "amount_total": amount_total,
        "status": status or "Sent",
        "due_date": due_date,
        "invoice_number": invoice_number,
        "total_paid": total_paid,
        "balance": balance,
        "payment_count": int(payment_count or 0),
        "last_payment_at": last_payment_at,
        "percent_paid": percent_paid,
        "is_paid_in_full": amount_total > 0 and balance <= 0.0001,
        "is_partially_paid": total_paid > 0 and balance > 0.0001,
        "is_unpaid": total_paid <= 0.0001,
        "is_overdue": bool(due_date and due_date < now_local() and balance > 0.0001),
    }


def derive_invoice_display_status(invoice_row_or_summary):
    if not invoice_row_or_summary:
        return "Sent"

    balance = float(invoice_row_or_summary.get("balance") or 0)
    total_paid = float(invoice_row_or_summary.get("total_paid") or 0)
    due_date = invoice_row_or_summary.get("due_date")

    if balance <= 0.0001:
        return "Paid"
    if due_date and due_date < now_local():
        return "Overdue"
    if total_paid > 0:
        return "Sent"
    return "Sent"


def sync_invoice_status(invoice_id: int):
    summary = get_invoice_payment_summary(invoice_id)
    if not summary:
        return None

    new_status = derive_invoice_display_status(summary)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT status, invoice_number FROM invoices WHERE id = %s", (invoice_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return summary

    old_status, invoice_number = row
    invoice_number = invoice_number or f"#{invoice_id}"

    if old_status != new_status:
        cur.execute("UPDATE invoices SET status = %s WHERE id = %s", (new_status, invoice_id))
        conn.commit()
        log_invoice_event(
            invoice_id=invoice_id,
            event_type="status_changed",
            title="Status updated",
            details=f"Invoice {invoice_number} status changed from {old_status or 'Sent'} to {new_status}.",
            visibility="both",
        )
    else:
        conn.commit()

    cur.close()
    conn.close()

    summary["status"] = new_status
    return summary


def get_invoice_view_summary(invoice_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT first_viewed_at, last_viewed_at, COALESCE(view_count, 0)
        FROM invoices
        WHERE id = %s
        """,
        (invoice_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return {
            "first_viewed_at": None,
            "last_viewed_at": None,
            "view_count": 0,
            "has_been_viewed": False,
            "never_viewed": True,
        }

    first_viewed_at, last_viewed_at, view_count = row
    view_count = int(view_count or 0)
    has_been_viewed = bool(view_count > 0 or first_viewed_at or last_viewed_at)

    return {
        "first_viewed_at": first_viewed_at,
        "last_viewed_at": last_viewed_at,
        "view_count": view_count,
        "has_been_viewed": has_been_viewed,
        "never_viewed": not has_been_viewed,
    }


def should_record_public_invoice_view(invoice_id: int, token: str):
    session_key = f"invoice_viewed:{invoice_id}:{token}"
    now_ts = datetime.utcnow().timestamp()
    previous = session.get(session_key)

    if previous:
        try:
            previous = float(previous)
            if (now_ts - previous) < (PUBLIC_VIEW_DEDUPE_MINUTES * 60):
                return False
        except Exception:
            pass

    session[session_key] = now_ts
    session.modified = True
    return True


def record_public_invoice_view(invoice_id: int, token: str):
    if not should_record_public_invoice_view(invoice_id, token):
        return False

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT invoice_number, COALESCE(view_count, 0)
        FROM invoices
        WHERE id = %s
        """,
        (invoice_id,),
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return False

    invoice_number, current_count = row
    invoice_number = invoice_number or f"#{invoice_id}"
    now_dt = now_local()

    cur.execute(
        """
        UPDATE invoices
        SET first_viewed_at = COALESCE(first_viewed_at, %s),
            last_viewed_at = %s,
            view_count = COALESCE(view_count, 0) + 1
        WHERE id = %s
        """,
        (now_dt, now_dt, invoice_id),
    )
    conn.commit()
    cur.close()
    conn.close()

    view_summary = get_invoice_view_summary(invoice_id)
    log_invoice_event(
        invoice_id=invoice_id,
        event_type="invoice_viewed",
        title="Invoice viewed",
        details=f"Public invoice {invoice_number} was opened. Total views: {view_summary['view_count']}.",
        visibility="private",
    )
    return True


def get_invoice_collection_recommendation(status: str, payment_summary: dict, view_summary: dict, last_reminder_sent_at=None):
    if not payment_summary:
        return ""

    if payment_summary.get("is_paid_in_full"):
        return "Paid in full."

    if payment_summary.get("is_partially_paid") and payment_summary.get("balance", 0) > 0:
        return "Partial payment received. Best next step: follow up on the remaining balance."

    if payment_summary.get("is_overdue") and not view_summary.get("has_been_viewed"):
        return "Overdue and not yet viewed. Best next step: resend or remind the client."

    if payment_summary.get("is_overdue") and view_summary.get("has_been_viewed"):
        return "Viewed and overdue. Best next step: send a professional overdue reminder."

    if view_summary.get("view_count", 0) >= 1 and payment_summary.get("balance", 0) > 0:
        return "Viewed but unpaid. Best next step: send a reminder."

    if not view_summary.get("has_been_viewed"):
        return "Sent but not yet viewed. Best next step: follow up gently."

    if last_reminder_sent_at:
        return "Reminder already sent. Monitor before sending another one."

    return "Open invoice. Monitor and follow up if needed."


def get_dashboard_receivables_metrics(user_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            i.id,
            i.amount,
            i.created_at,
            i.due_date,
            COALESCE(i.view_count, 0),
            COALESCE(SUM(CASE WHEN COALESCE(p.payment_status, 'succeeded') != 'failed' THEN p.amount ELSE 0 END), 0) AS total_paid
        FROM invoices i
        LEFT JOIN payments p ON p.invoice_id = i.id
        WHERE i.user_id = %s
        GROUP BY i.id, i.amount, i.created_at, i.due_date, i.view_count
        """,
        (user_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    now = now_local()
    month_start = datetime(now.year, now.month, 1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)

    outstanding_receivables = 0.0
    overdue_receivables = 0.0
    amount_outstanding_this_month = 0.0
    viewed_but_unpaid_count = 0
    sent_not_viewed_count = 0
    paid_invoice_count = 0
    unpaid_invoice_count = 0
    payment_day_samples = []

    for invoice_id, amount, created_at, due_date, view_count, total_paid in rows:
        amount = float(amount or 0)
        total_paid = float(total_paid or 0)
        balance = max(amount - total_paid, 0.0)

        if balance > 0:
            outstanding_receivables += balance
            unpaid_invoice_count += 1

        if due_date and due_date < now and balance > 0:
            overdue_receivables += balance

        if created_at and month_start <= created_at < next_month and balance > 0:
            amount_outstanding_this_month += balance

        if view_count and balance > 0:
            viewed_but_unpaid_count += 1

        if not view_count and balance > 0:
            sent_not_viewed_count += 1

        if balance <= 0.0001:
            paid_invoice_count += 1
            summary = get_invoice_payment_summary(invoice_id)
            if created_at and summary and summary.get("last_payment_at"):
                delta = (summary["last_payment_at"] - created_at).days
                if delta >= 0:
                    payment_day_samples.append(delta)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(p.amount), 0)
        FROM payments p
        JOIN invoices i ON i.id = p.invoice_id
        WHERE i.user_id = %s
          AND COALESCE(p.payment_status, 'succeeded') != 'failed'
          AND COALESCE(p.occurred_at, p.created_at) >= %s
          AND COALESCE(p.occurred_at, p.created_at) < %s
        """,
        (user_id, month_start, next_month),
    )
    amount_collected_this_month = float(cur.fetchone()[0] or 0)
    cur.close()
    conn.close()

    total_closed = paid_invoice_count + unpaid_invoice_count
    collection_rate = round((paid_invoice_count / total_closed) * 100, 1) if total_closed > 0 else 0.0
    avg_days_to_payment = round(sum(payment_day_samples) / len(payment_day_samples), 1) if payment_day_samples else None

    reserve_pct = clean_percent(os.environ.get("DEFAULT_TAX_RESERVE_PERCENT"), DEFAULT_TAX_RESERVE_PERCENT)
    suggested_tax_reserve = round(amount_collected_this_month * (reserve_pct / 100.0), 2)
    revenue_after_reserve = round(amount_collected_this_month - suggested_tax_reserve, 2)

    return {
        "outstanding_receivables": round(outstanding_receivables, 2),
        "overdue_receivables": round(overdue_receivables, 2),
        "amount_collected_this_month": round(amount_collected_this_month, 2),
        "amount_outstanding_this_month": round(amount_outstanding_this_month, 2),
        "viewed_but_unpaid_count": viewed_but_unpaid_count,
        "sent_not_viewed_count": sent_not_viewed_count,
        "collection_rate": collection_rate,
        "avg_days_to_payment": avg_days_to_payment,
        "tax_reserve_percent": reserve_pct,
        "suggested_tax_reserve": suggested_tax_reserve,
        "revenue_after_reserve": revenue_after_reserve,
    }


def plan_allows(required_plan: str) -> bool:
    user_plan = get_plan_for_current_user()
    return PLAN_LEVELS.get(user_plan, 0) >= PLAN_LEVELS.get(required_plan, 0)


def check_invoice_quota_or_reason():
    user = get_current_user()
    user_id = user["id"]
    plan = user.get("plan") or "free"

    if not user_id:
        return False, "Please log in to create invoices."

    if plan != "free":
        return True, None

    now = now_local()
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

    if count >= 3:
        return False, "You've reached the 3 invoices / month limit on the Starter plan."

    return True, None


def get_user_payment_setup(user_id: int):
    default_state = {
        "stripe_connect_account_id": "",
        "charges_enabled": False,
        "payouts_enabled": False,
        "details_submitted": False,
        "is_connected": False,
        "is_ready": False,
        "last_status_sync": None,
        "onboarded_at": None,
        "status_label": "Not connected",
    }

    if not user_id:
        return default_state

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            stripe_connect_account_id,
            stripe_connect_charges_enabled,
            stripe_connect_payouts_enabled,
            stripe_connect_details_submitted,
            stripe_connect_last_status_sync,
            stripe_connect_onboarded_at
        FROM users
        WHERE id = %s
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        return default_state

    (
        stripe_connect_account_id,
        charges_enabled,
        payouts_enabled,
        details_submitted,
        last_status_sync,
        onboarded_at,
    ) = row

    is_connected = bool(stripe_connect_account_id)
    is_ready = bool(is_connected and charges_enabled and payouts_enabled and details_submitted)

    if is_ready:
        status_label = "Ready"
    elif is_connected:
        status_label = "Pending"
    else:
        status_label = "Not connected"

    return {
        "stripe_connect_account_id": stripe_connect_account_id or "",
        "charges_enabled": bool(charges_enabled),
        "payouts_enabled": bool(payouts_enabled),
        "details_submitted": bool(details_submitted),
        "is_connected": is_connected,
        "is_ready": is_ready,
        "last_status_sync": last_status_sync,
        "onboarded_at": onboarded_at,
        "status_label": status_label,
    }


def update_user_payment_setup_from_account(user_id: int, account):
    if not user_id or not account:
        return

    details_submitted = bool(account.get("details_submitted"))
    charges_enabled = bool(account.get("charges_enabled"))
    payouts_enabled = bool(account.get("payouts_enabled"))
    account_id = account.get("id") or ""
    onboarded_at = now_local() if details_submitted else None

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE users
        SET stripe_connect_account_id = %s,
            stripe_connect_charges_enabled = %s,
            stripe_connect_payouts_enabled = %s,
            stripe_connect_details_submitted = %s,
            stripe_connect_onboarded_at = COALESCE(stripe_connect_onboarded_at, %s),
            stripe_connect_last_status_sync = %s
        WHERE id = %s
        """,
        (
            account_id or None,
            charges_enabled,
            payouts_enabled,
            details_submitted,
            onboarded_at,
            now_local(),
            user_id,
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()


def get_or_create_stripe_connect_account(user_id: int, email: str = ""):
    payment_setup = get_user_payment_setup(user_id)
    existing_account_id = payment_setup.get("stripe_connect_account_id")

    if existing_account_id:
        try:
            account = stripe.Account.retrieve(existing_account_id)
            update_user_payment_setup_from_account(user_id, account)
            return account
        except Exception:
            logger.exception("Failed retrieving existing Stripe Connect account for user_id=%s", user_id)

    account = stripe.Account.create(
        type="express",
        email=email or None,
        metadata={
            "billbeam_user_id": str(user_id),
            "product": APP_NAME,
        },
        business_type="individual",
        capabilities={
            "card_payments": {"requested": True},
            "transfers": {"requested": True},
        },
    )
    update_user_payment_setup_from_account(user_id, account)
    return account


def sync_stripe_connect_status_for_user(user_id: int):
    payment_setup = get_user_payment_setup(user_id)
    account_id = payment_setup.get("stripe_connect_account_id")
    if not account_id:
        return payment_setup

    try:
        account = stripe.Account.retrieve(account_id)
        update_user_payment_setup_from_account(user_id, account)
        return get_user_payment_setup(user_id)
    except Exception:
        logger.exception("Failed syncing Stripe Connect status for user_id=%s", user_id)
        return payment_setup


def build_stripe_connect_return_url(lang: str = "en"):
    base_url = APP_BASE_URL or request.host_url.rstrip("/")
    return f"{base_url}/settings?payments_connected=1&lang={lang}"


def build_stripe_connect_refresh_url(lang: str = "en"):
    base_url = APP_BASE_URL or request.host_url.rstrip("/")
    return f"{base_url}/settings?payments_refresh=1&lang={lang}"


def get_business_profile():
    user = get_current_user()
    user_id = user["id"]

    if not user_id:
        return {
            "id": None,
            "business_name": DEFAULT_BUSINESS_NAME,
            "email": "",
            "phone": "",
            "website": "",
            "address": "",
            "logo_url": "",
            "brand_color": DEFAULT_BRAND_COLOR,
            "accent_color": DEFAULT_ACCENT_COLOR,
            "default_terms": "",
            "default_notes": "",
            "user_id": None,
        }

    return get_business_profile_by_user_id(user_id)

def get_business_profile_by_user_id(user_id: int):
    if not user_id:
        return None

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
            "business_name": DEFAULT_BUSINESS_NAME,
            "email": "",
            "phone": "",
            "website": "",
            "address": "",
            "logo_url": "",
            "brand_color": DEFAULT_BRAND_COLOR,
            "accent_color": DEFAULT_ACCENT_COLOR,
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
        "business_name": business_name or DEFAULT_BUSINESS_NAME,
        "email": email or "",
        "phone": phone or "",
        "website": website or "",
        "address": address or "",
        "logo_url": logo_url or "",
        "brand_color": brand_color or DEFAULT_BRAND_COLOR,
        "accent_color": accent_color or DEFAULT_ACCENT_COLOR,
        "default_terms": default_terms or "",
        "default_notes": default_notes or "",
        "user_id": user_id,
    }

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
            "business_name": DEFAULT_BUSINESS_NAME,
            "email": "",
            "phone": "",
            "website": "",
            "address": "",
            "logo_url": "",
            "brand_color": DEFAULT_BRAND_COLOR,
            "accent_color": DEFAULT_ACCENT_COLOR,
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
        "business_name": business_name or DEFAULT_BUSINESS_NAME,
        "email": email or "",
        "phone": phone or "",
        "website": website or "",
        "address": address or "",
        "logo_url": logo_url or "",
        "brand_color": brand_color or DEFAULT_BRAND_COLOR,
        "accent_color": accent_color or DEFAULT_ACCENT_COLOR,
        "default_terms": default_terms or "",
        "default_notes": default_notes or "",
        "user_id": user_id,
    }


def get_user_services(user_id: int, include_inactive: bool = False):
    if not user_id:
        return []

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if include_inactive:
            cur.execute(
                """
                SELECT id, user_id, name, description, price, is_active, created_at
                FROM services
                WHERE user_id = %s
                ORDER BY created_at DESC, id DESC
                """,
                (user_id,),
            )
        else:
            cur.execute(
                """
                SELECT id, user_id, name, description, price, is_active, created_at
                FROM services
                WHERE user_id = %s AND is_active = TRUE
                ORDER BY created_at DESC, id DESC
                """,
                (user_id,),
            )

        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    services = []
    for service_id, service_user_id, name, description, price, is_active, created_at in rows:
        services.append(
            {
                "id": service_id,
                "user_id": service_user_id,
                "name": name or "",
                "description": description or "",
                "price": float(price or 0),
                "is_active": bool(is_active),
                "created_at": created_at,
            }
        )
    return services


def get_service_by_id(service_id: int, user_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, user_id, name, description, price, is_active, created_at
            FROM services
            WHERE id = %s AND user_id = %s
            LIMIT 1
            """,
            (service_id, user_id),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row:
        return None

    return {
        "id": row[0],
        "user_id": row[1],
        "name": row[2] or "",
        "description": row[3] or "",
        "price": float(row[4] or 0),
        "is_active": bool(row[5]),
        "created_at": row[6],
    }


def create_user_service(user_id: int, name: str, description: str = "", price: float = 0.0):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO services (user_id, name, description, price, is_active, created_at)
            VALUES (%s, %s, %s, %s, TRUE, %s)
            RETURNING id
            """,
            (user_id, name.strip(), description.strip(), price, now_local()),
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        return new_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def update_user_service(service_id: int, user_id: int, name: str, description: str = "", price: float = 0.0):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE services
            SET name = %s,
                description = %s,
                price = %s
            WHERE id = %s AND user_id = %s
            """,
            (name.strip(), description.strip(), price, service_id, user_id),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def set_user_service_active(service_id: int, user_id: int, is_active: bool):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE services
            SET is_active = %s
            WHERE id = %s AND user_id = %s
            """,
            (bool(is_active), service_id, user_id),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def validate_service_form(name: str, description: str = "", price_raw=None):
    name = (name or "").strip()
    description = (description or "").strip()
    price = parse_float(price_raw, default=None)

    if not name:
        return {
            "ok": False,
            "error": "Service name is required.",
            "name": name,
            "description": description,
            "price": 0.0,
        }

    if len(name) > 120:
        return {
            "ok": False,
            "error": "Service name must be 120 characters or fewer.",
            "name": name,
            "description": description,
            "price": 0.0,
        }

    if len(description) > 1000:
        return {
            "ok": False,
            "error": "Service description must be 1000 characters or fewer.",
            "name": name,
            "description": description,
            "price": 0.0,
        }

    if price is None:
        return {
            "ok": False,
            "error": "Service price must be a valid number.",
            "name": name,
            "description": description,
            "price": 0.0,
        }

    if price < 0:
        return {
            "ok": False,
            "error": "Service price cannot be negative.",
            "name": name,
            "description": description,
            "price": 0.0,
        }

    return {
        "ok": True,
        "error": None,
        "name": name,
        "description": description,
        "price": round(float(price), 2),
    }

# =========================
# SERVICE REQUEST / BOOKING HELPERS
# =========================

SERVICE_REQUEST_STATUSES = {
    "requested",
    "approved",
    "in_progress",
    "completed",
    "cancelled",
}


def normalize_request_status(status: str) -> str:
    status = (status or "").strip().lower()
    return status if status in SERVICE_REQUEST_STATUSES else "requested"


def log_service_request_event(service_request_id, user_id, event_type, old_value=None, new_value=None, note=None):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO service_request_events
            (service_request_id, user_id, event_type, old_value, new_value, note, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                service_request_id,
                user_id,
                event_type,
                old_value,
                new_value,
                note,
                now_local(),
            ),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning("Failed to log service request event: %s", e)
    finally:
        cur.close()
        conn.close()


def find_matching_client(user_id, email):
    if not email:
        return None

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id FROM clients
            WHERE user_id = %s AND LOWER(email) = LOWER(%s)
            LIMIT 1
            """,
            (user_id, email),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        conn.close()


def serialize_service_request_row(row):
    if not row:
        return None

    return {
        "id": row[0],
        "user_id": row[1],
        "client_id": row[2],
        "service_id": row[3],
        "invoice_id": row[4],
        "status": row[5] or "requested",
        "request_type": row[6] or "request",
        "source": row[7] or "public",
        "service_title_snapshot": row[8] or "",
        "service_description_snapshot": row[9] or "",
        "service_price_snapshot": float(row[10] or 0),
        "client_name": row[11] or "",
        "client_email": row[12] or "",
        "client_phone": row[13] or "",
        "request_details": row[14] or "",
        "preferred_date_text": row[15] or "",
        "preferred_time_text": row[16] or "",
        "quantity": int(row[17] or 1),
        "intake_answers_json": row[18] or "",
        "owner_notes": row[19] or "",
        "client_notes": row[20] or "",
        "cancel_requested_by_client": bool(row[21]),
        "cancel_reason": row[22] or "",
        "approved_at": row[23],
        "in_progress_at": row[24],
        "completed_at": row[25],
        "cancelled_at": row[26],
        "converted_to_invoice_at": row[27],
        "created_at": row[28],
        "updated_at": row[29],
    }


def serialize_service_request_rows(rows):
    return [serialize_service_request_row(row) for row in (rows or [])]


def get_service_request_events_for_user(request_id, user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT event_type, old_value, new_value, note, created_at
            FROM service_request_events
            WHERE service_request_id = %s AND user_id = %s
            ORDER BY created_at DESC, id DESC
            """,
            (request_id, user_id),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    events = []
    for row in rows:
        events.append(
            {
                "event_type": row[0] or "",
                "old_value": row[1] or "",
                "new_value": row[2] or "",
                "note": row[3] or "",
                "created_at": row[4],
            }
        )
    return events


def get_service_request_counts_for_user(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT status, COUNT(*)
            FROM service_requests
            WHERE user_id = %s
            GROUP BY status
            """,
            (user_id,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    counts = {
        "all": 0,
        "requested": 0,
        "approved": 0,
        "in_progress": 0,
        "completed": 0,
        "cancelled": 0,
    }

    for status, count in rows:
        status_key = normalize_request_status(status)
        counts[status_key] = int(count or 0)
        counts["all"] += int(count or 0)

    return counts


def get_recent_service_requests_for_user(user_id, limit=5):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM service_requests
            WHERE user_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            (user_id, limit),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return serialize_service_request_rows(rows)


def link_service_request_to_invoice(request_id, user_id, invoice_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE service_requests
            SET invoice_id = %s,
                converted_to_invoice_at = %s,
                updated_at = %s
            WHERE id = %s AND user_id = %s
            """,
            (invoice_id, now_local(), now_local(), request_id, user_id),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception("Failed linking service request %s to invoice %s: %s", request_id, invoice_id, e)
        return False
    finally:
        cur.close()
        conn.close()

    log_service_request_event(
        request_id,
        user_id,
        "invoice_linked",
        new_value=str(invoice_id),
        note=f"Invoice #{invoice_id} was created from this request.",
    )
    return True


def get_invoice_summary_for_user(invoice_id, user_id):
    if not invoice_id or not user_id:
        return None

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, invoice_number, client, amount, status, created_at, due_date
            FROM invoices
            WHERE id = %s AND user_id = %s
            LIMIT 1
            """,
            (invoice_id, user_id),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row:
        return None

    return {
        "id": row[0],
        "invoice_number": row[1] or f"#{row[0]}",
        "client": row[2] or "",
        "amount": float(row[3] or 0),
        "status": row[4] or "Sent",
        "created_at": row[5],
        "due_date": row[6],
    }


def create_notification(user_id, notification_type, title, body="", link_url=""):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO notifications (
                user_id,
                notification_type,
                title,
                body,
                link_url,
                is_read,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                user_id,
                notification_type,
                title,
                body or "",
                link_url or "",
                False,
                now_local(),
            ),
        )
        notification_id = cur.fetchone()[0]
        conn.commit()
        return notification_id
    except Exception as e:
        conn.rollback()
        logger.exception("Failed creating notification for user %s: %s", user_id, e)
        return None
    finally:
        cur.close()
        conn.close()


def get_notifications_for_user(user_id, unread_only=False, limit=25):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if unread_only:
            cur.execute(
                """
                SELECT id, notification_type, title, body, link_url, is_read, created_at
                FROM notifications
                WHERE user_id = %s AND is_read = FALSE
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (user_id, limit),
            )
        else:
            cur.execute(
                """
                SELECT id, notification_type, title, body, link_url, is_read, created_at
                FROM notifications
                WHERE user_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (user_id, limit),
            )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    notifications = []
    for row in rows:
        notifications.append(
            {
                "id": row[0],
                "notification_type": row[1] or "",
                "title": row[2] or "",
                "body": row[3] or "",
                "link_url": row[4] or "",
                "is_read": bool(row[5]),
                "created_at": row[6],
            }
        )
    return notifications


def get_unread_notification_count_for_user(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM notifications
            WHERE user_id = %s AND is_read = FALSE
            """,
            (user_id,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    return int(row[0] or 0) if row else 0


def mark_notification_read(notification_id, user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE notifications
            SET is_read = TRUE
            WHERE id = %s AND user_id = %s
            """,
            (notification_id, user_id),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        logger.exception("Failed marking notification %s read for user %s: %s", notification_id, user_id, e)
        return False
    finally:
        cur.close()
        conn.close()


def create_service_request(
    user_id,
    service_id,
    client_name,
    client_email,
    client_phone=None,
    request_details=None,
    preferred_date_text=None,
    preferred_time_text=None,
    quantity=1,
):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # --- Service snapshot ---
        service_title = None
        service_description = None
        service_price = None

        if service_id:
            cur.execute(
                """
                SELECT name, description, price
                FROM services
                WHERE id = %s AND user_id = %s
                """,
                (service_id, user_id),
            )
            svc = cur.fetchone()
            if svc:
                service_title, service_description, service_price = svc

        # --- Match existing client ---
        client_id = find_matching_client(user_id, client_email)

        cur.execute(
            """
            INSERT INTO service_requests (
                user_id,
                client_id,
                service_id,
                status,
                service_title_snapshot,
                service_description_snapshot,
                service_price_snapshot,
                client_name,
                client_email,
                client_phone,
                request_details,
                preferred_date_text,
                preferred_time_text,
                quantity,
                created_at,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                user_id,
                client_id,
                service_id,
                "requested",
                service_title,
                service_description,
                service_price,
                client_name,
                client_email,
                client_phone,
                request_details,
                preferred_date_text,
                preferred_time_text,
                quantity,
                now_local(),
                now_local(),
            ),
        )

        request_id = cur.fetchone()[0]
        conn.commit()

        log_service_request_event(
            request_id,
            user_id,
            "created",
            note="Service request submitted",
        )

        create_notification(
            user_id=user_id,
            notification_type="service_request_created",
            title=f"New service request from {client_name}",
            body=(service_title or "Custom request"),
            link_url=f"/requests/{request_id}",
        )

        return request_id

    except Exception as e:
        conn.rollback()
        logger.exception("Failed to create service request: %s", e)
        return None

    finally:
        cur.close()
        conn.close()


def get_service_requests_for_user(user_id, status=None):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        if status:
            cur.execute(
                """
                SELECT *
                FROM service_requests
                WHERE user_id = %s AND status = %s
                ORDER BY created_at DESC
                """,
                (user_id, status),
            )
        else:
            cur.execute(
                """
                SELECT *
                FROM service_requests
                WHERE user_id = %s
                ORDER BY created_at DESC
                """,
                (user_id,),
            )

        return cur.fetchall()

    finally:
        cur.close()
        conn.close()


def get_service_request_by_id(request_id, user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM service_requests
            WHERE id = %s AND user_id = %s
            LIMIT 1
            """,
            (request_id, user_id),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def update_service_request_status(request_id, user_id, new_status):
    new_status = normalize_request_status(new_status)

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT status FROM service_requests
            WHERE id = %s AND user_id = %s
            """,
            (request_id, user_id),
        )
        row = cur.fetchone()
        if not row:
            return False

        old_status = row[0]

        timestamp_field = None
        if new_status == "approved":
            timestamp_field = "approved_at"
        elif new_status == "in_progress":
            timestamp_field = "in_progress_at"
        elif new_status == "completed":
            timestamp_field = "completed_at"
        elif new_status == "cancelled":
            timestamp_field = "cancelled_at"

        if timestamp_field:
            cur.execute(
                f"""
                UPDATE service_requests
                SET status = %s,
                    {timestamp_field} = %s,
                    updated_at = %s
                WHERE id = %s AND user_id = %s
                """,
                (new_status, now_local(), now_local(), request_id, user_id),
            )
        else:
            cur.execute(
                """
                UPDATE service_requests
                SET status = %s,
                    updated_at = %s
                WHERE id = %s AND user_id = %s
                """,
                (new_status, now_local(), request_id, user_id),
            )

        conn.commit()

        log_service_request_event(
            request_id,
            user_id,
            "status_changed",
            old_value=old_status,
            new_value=new_status,
        )

        return True

    except Exception as e:
        conn.rollback()
        logger.exception("Failed updating request status: %s", e)
        return False

    finally:
        cur.close()
        conn.close()


def upsert_business_profile(data: dict):
    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id FROM business_profile WHERE user_id = %s ORDER BY id ASC LIMIT 1",
        (user_id,),
    )
    row = cursor.fetchone()
    now = now_local()

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
# LANGUAGE SYNC
# -------------------------
@app.before_request
def sync_request_language():
    """
    Keep the active language stable across page loads and redirects.
    This is intentionally lightweight and additive.
    """
    resolved_lang = get_request_lang()
    session["lang"] = resolved_lang
    session.modified = True
    g.current_lang = resolved_lang


# -------------------------
# RESPONSE HARDENING
# -------------------------
@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

    path = request.path or ""
    sensitive_prefixes = (
        "/invoices",
        "/invoice/",
        "/clients",
        "/settings",
        "/search",
        "/send-email/",
        "/add-payment/",
        "/edit/",
        "/update/",
        "/delete/",
        "/pricing",
        "/billing/",
        "/manage-subscription",
        "/settings/payments",
    )

    if path.startswith(sensitive_prefixes):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"

    return response


# -------------------------
# CONTEXT PROCESSORS
# -------------------------
@app.context_processor
def inject_now():
    return {"now": now_local}


@app.context_processor
def inject_business_profile_ctx():
    return {"business_profile": get_business_profile()}


def get_business_profile_safe():
    profile = get_business_profile()
    if profile:
        return profile

    user = get_current_user()
    return {
        "id": None,
        "business_name": DEFAULT_BUSINESS_NAME,
        "email": "",
        "phone": "",
        "website": "",
        "address": "",
        "logo_url": "",
        "brand_color": DEFAULT_BRAND_COLOR,
        "accent_color": DEFAULT_ACCENT_COLOR,
        "default_terms": "",
        "default_notes": "",
        "user_id": user.get("id"),
    }


@app.context_processor
def inject_current_user_ctx():
    user = get_current_user()
    plan = normalize_plan_key(user.get("plan") or "free")
    is_authenticated = "user_id" in session and user.get("id") is not None
    payment_setup = get_user_payment_setup(user.get("id")) if user.get("id") else {
        "stripe_connect_account_id": "",
        "charges_enabled": False,
        "payouts_enabled": False,
        "details_submitted": False,
        "is_connected": False,
        "is_ready": False,
        "last_status_sync": None,
        "onboarded_at": None,
    }
    return {
        "current_user": user,
        "user_plan": plan,
        "plan_definitions": PLAN_DEFINITIONS,
        "is_authenticated": is_authenticated,
        "app_name": APP_NAME,
        "ai_notice_enabled": AI_NOTICE_ENABLED,
        "payment_setup": payment_setup,
        "is_simple_plan": is_simple(user),
        "can_email_invoices_current": can_email_invoices(user),
        "can_collect_payments_current": can_collect_payments(user),
        "can_use_ai_current": can_use_ai(user),
        "can_use_advanced_dashboard_current": can_use_advanced_dashboard(user),
        "can_use_collections_current": can_use_collections(user),
        "can_use_branding_current": can_use_branding(user),
        "t": t,
    }


@app.context_processor
def inject_service_request_dashboard_ctx():
    user = get_current_user()
    user_id = user.get("id")

    if not user_id:
        return {
            "service_request_counts": {
                "all": 0,
                "requested": 0,
                "approved": 0,
                "in_progress": 0,
                "completed": 0,
                "cancelled": 0,
            },
            "recent_service_requests": [],
            "has_service_requests": False,
        }

    counts = get_service_request_counts_for_user(user_id)
    recent_requests = get_recent_service_requests_for_user(user_id, limit=5)

    return {
        "service_request_counts": counts,
        "recent_service_requests": recent_requests,
        "has_service_requests": counts.get("all", 0) > 0,
    }


@app.context_processor
def inject_notification_ctx():
    user = get_current_user()
    user_id = user.get("id")

    if not user_id:
        return {
            "unread_notification_count": 0,
            "recent_notifications": [],
        }

    return {
        "unread_notification_count": get_unread_notification_count_for_user(user_id),
        "recent_notifications": get_notifications_for_user(user_id, unread_only=False, limit=5),
    }


# -------------------------
# HOME
# -------------------------
@app.route("/")
@login_required
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
    cursor.close()
    conn.close()

    return render_template(
    "index.html",
    clients=clients,
    lang=get_request_lang(),
)


# -------------------------
# AUTH
# -------------------------
@app.route("/register", methods=["GET", "POST"])
def register():
    error = None

    lang = get_request_lang()

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
                    user_id, _plan = row
                    session["user_id"] = user_id
                    session.permanent = True
                    session["lang"] = lang
                    return lang_redirect("invoices_page")

    return render_template("register.html", error=error, lang=lang)


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None

    lang = get_request_lang()

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
                user_id, _user_email, password_hash, _plan, is_active = row
                if not is_active:
                    error = "This account is inactive."
                elif not password_hash:
                    error = "This account cannot be logged into yet."
                elif not check_password_hash(password_hash, password):
                    error = "Invalid email or password."
                else:
                    session["user_id"] = user_id
                    session.permanent = True
                    session["lang"] = lang
                    return lang_redirect("invoices_page")

    return render_template("login.html", error=error, lang=lang)


@app.route("/logout")
def logout():
    lang = get_request_lang()
    session.clear()
    session["lang"] = lang
    session.modified = True
    return lang_redirect("login")


# -------------------------
# BILLING / MARKETING / STATIC PAGES
# -------------------------
@app.route("/pricing")
def pricing():
    lang = normalize_lang(request.args.get("lang", "en"))
    user = get_current_user()
    user_plan = normalize_plan_key(user.get("plan", "free")) if user else "free"

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
        is_pro=(user_plan in ("pro", "enterprise")),
        stripe_publishable_key=STRIPE_PUBLISHABLE_KEY,
        lang=lang,
    )


@app.route("/landing")
def landing_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("landing.html", lang=lang)


@app.route("/launch-checklist")
def launch_checklist_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("launch_checklist.html", lang=lang)


@app.route("/about")
def about():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("about.html", lang=lang)


@app.route("/help")
def help_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("help.html", lang=lang)


@app.route("/support")
def support():
    lang = normalize_lang(request.args.get("lang", "en"))
    return render_template("support.html", lang=lang)


@app.route("/contact")
def contact():
    lang = normalize_lang(request.args.get("lang", "en"))
    return render_template("contact.html", lang=lang)


@app.route("/faq")
def faq_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("faq.html", lang=lang)

# -------------------------
# SERVICE REQUESTS / OWNER VIEWS
# -------------------------
@app.route("/requests")
@login_required
def requests_page():
    user = get_current_user()
    user_id = user["id"]
    lang = get_request_lang()

    raw_status = (request.args.get("status") or "").strip().lower()
    status = normalize_request_status(raw_status) if raw_status else None

    requests_list = serialize_service_request_rows(
        get_service_requests_for_user(user_id, status=status)
    )

    return render_template(
        "requests.html",
        requests_list=requests_list,
        selected_status=status,
        lang=lang,
    )


@app.route("/requests/<int:request_id>")
@login_required
def request_detail_page(request_id):
    user = get_current_user()
    user_id = user["id"]
    lang = get_request_lang()

    service_request = serialize_service_request_row(
        get_service_request_by_id(request_id, user_id)
    )
    if not service_request:
        return lang_redirect("requests_page")

    request_events = get_service_request_events_for_user(request_id, user_id)
    linked_invoice = None
    if service_request.get("invoice_id"):
        linked_invoice = get_invoice_summary_for_user(service_request["invoice_id"], user_id)

    public_booking_url = url_for(
        "public_services_page",
        user_id=user_id,
        lang=lang,
        _external=True,
    )

    return render_template(
        "request_detail.html",
        service_request=service_request,
        request_events=request_events,
        linked_invoice=linked_invoice,
        public_booking_url=public_booking_url,
        lang=lang,
    )


@app.route("/requests/<int:request_id>/status", methods=["POST"])
@login_required
def update_request_status(request_id):
    user = get_current_user()
    user_id = user["id"]

    new_status = request.form.get("status", "")
    update_service_request_status(request_id, user_id, new_status)

    return lang_redirect("request_detail_page", request_id=request_id)


@app.route("/requests/<int:request_id>/create-invoice")
@login_required
def create_invoice_from_request(request_id):
    user = get_current_user()
    user_id = user["id"]
    lang = get_request_lang()

    service_request = serialize_service_request_row(
        get_service_request_by_id(request_id, user_id)
    )
    if not service_request:
        return lang_redirect("requests_page")

    params = {
        "lang": lang,
        "service_request_id": service_request["id"],
        "prefill_client_name": service_request["client_name"],
        "prefill_client_email": service_request["client_email"],
        "prefill_client_phone": service_request["client_phone"],
        "prefill_request_details": service_request["request_details"],
        "prefill_quantity": service_request["quantity"],
        "prefill_service_title": service_request["service_title_snapshot"],
        "prefill_service_price": service_request["service_price_snapshot"],
    }

    if service_request.get("client_id"):
        params["prefill_client_id"] = service_request["client_id"]

    return redirect(url_for("home", **params))


@app.route("/notifications")
@login_required
def notifications_page():
    user = get_current_user()
    user_id = user["id"]
    lang = get_request_lang()

    notifications = get_notifications_for_user(user_id, unread_only=False, limit=100)

    return render_template(
        "notifications.html",
        notifications=notifications,
        lang=lang,
    )


@app.route("/notifications/<int:notification_id>/read")
@login_required
def mark_notification_read_page(notification_id):
    user = get_current_user()
    user_id = user["id"]

    notifications = get_notifications_for_user(user_id, unread_only=False, limit=100)
    target = None
    for item in notifications:
        if item["id"] == notification_id:
            target = item
            break

    mark_notification_read(notification_id, user_id)

    if target and target.get("link_url"):
        link_url = target["link_url"]
        if "?" in link_url:
            return redirect(f"{link_url}&lang={get_request_lang()}")
        return redirect(f"{link_url}?lang={get_request_lang()}")

    return lang_redirect("notifications_page")


# -------------------------
# PUBLIC SERVICE REQUEST FLOW
# -------------------------
@app.route("/book/<int:user_id>")
def public_services_page(user_id):
    lang = get_request_lang()
    business_profile = get_business_profile_by_user_id(user_id)
    services = get_user_services(user_id, include_inactive=False)

    if not business_profile and not services:
        return render_template("404.html"), 404

    return render_template(
        "public_services.html",
        public_user_id=user_id,
        public_business_profile=business_profile,
        public_services=services,
        lang=lang,
    )


@app.route("/book/<int:user_id>/service/<int:service_id>", methods=["GET", "POST"])
def public_service_request_page(user_id, service_id):
    lang = get_request_lang()
    business_profile = get_business_profile_by_user_id(user_id)
    service = get_service_by_id(service_id, user_id)

    if not business_profile or not service or not service.get("is_active"):
        return render_template("404.html"), 404

    error = None
    form_data = {
        "client_name": "",
        "client_email": "",
        "client_phone": "",
        "request_details": "",
        "preferred_date_text": "",
        "preferred_time_text": "",
        "quantity": 1,
    }

    if request.method == "POST":
        form_data["client_name"] = (request.form.get("client_name") or "").strip()
        form_data["client_email"] = (request.form.get("client_email") or "").strip()
        form_data["client_phone"] = (request.form.get("client_phone") or "").strip()
        form_data["request_details"] = (request.form.get("request_details") or "").strip()
        form_data["preferred_date_text"] = (request.form.get("preferred_date_text") or "").strip()
        form_data["preferred_time_text"] = (request.form.get("preferred_time_text") or "").strip()

        quantity_raw = (request.form.get("quantity") or "1").strip()
        try:
            form_data["quantity"] = max(1, int(quantity_raw))
        except ValueError:
            form_data["quantity"] = 1

        if not form_data["client_name"]:
            error = "Client name is required."
        elif not form_data["client_email"]:
            error = "Client email is required."
        else:
            request_id = create_service_request(
                user_id=user_id,
                service_id=service_id,
                client_name=form_data["client_name"],
                client_email=form_data["client_email"],
                client_phone=form_data["client_phone"],
                request_details=form_data["request_details"],
                preferred_date_text=form_data["preferred_date_text"],
                preferred_time_text=form_data["preferred_time_text"],
                quantity=form_data["quantity"],
            )

            if request_id:
                return redirect(
                    url_for(
                        "public_request_success_page",
                        user_id=user_id,
                        request_id=request_id,
                        lang=lang,
                    )
                )

            error = "Something went wrong while submitting your request. Please try again."

    return render_template(
        "public_request_form.html",
        public_user_id=user_id,
        public_business_profile=business_profile,
        service=service,
        error=error,
        form_data=form_data,
        lang=lang,
    )


@app.route("/book/<int:user_id>/success/<int:request_id>")
def public_request_success_page(user_id, request_id):
    lang = get_request_lang()
    business_profile = get_business_profile_by_user_id(user_id)

    if not business_profile:
        return render_template("404.html"), 404

    return render_template(
        "public_request_success.html",
        public_user_id=user_id,
        public_business_profile=business_profile,
        request_id=request_id,
        lang=lang,
    )


@app.route("/changelog")
def changelog_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("changelog.html", lang=lang)


# -------------------------
# LEGAL / TRUST PAGES
# -------------------------
@app.route("/privacy")
def privacy_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("privacy.html", lang=lang)


@app.route("/terms")
def terms_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("terms.html", lang=lang)


@app.route("/ai-notice")
def ai_notice_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("ai_notice.html", lang=lang)


@app.route("/billing-policy")
def billing_policy_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("billing_policy.html", lang=lang)


@app.route("/cookies")
def cookies_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("cookies.html", lang=lang)


@app.route("/security")
def security_page():
    lang = normalize_lang(request.args.get("lang"))
    return render_template("data_security.html", lang=lang)


@app.route("/uploads/<path:filename>")
def uploaded_file(filename):
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename)


# -------------------------
# LEGAL / SUPPORT PAGES
# -------------------------
@app.route("/legal")
def legal_hub():
    return render_template(
        "legal_hub.html",
        lang=normalize_lang(request.args.get("lang", "en"))
    )


@app.route("/data-security")
def data_security():
    return render_template(
        "data_security.html",
        lang=normalize_lang(request.args.get("lang", "en"))
    )


@app.route("/app-store-privacy")
def app_store_privacy():
    return render_template(
        "app_store_privacy_note.html",
        lang=normalize_lang(request.args.get("lang", "en"))
    )


# -------------------------
# DEV / DEBUG
# -------------------------
@app.route("/debug-plan")
@login_required
def debug_plan():
    user = get_current_user()
    return {
        "id": user.get("id"),
        "email": user.get("email"),
        "plan": user.get("plan"),
    }


@app.route("/dev/force-pro")
@login_required
def dev_force_pro():
    if not IS_DEBUG_MODE:
        return "Not available outside debug/development mode.", 404

    user = get_current_user()
    user_id = user.get("id")

    if not user_id:
        return "No current user found.", 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET plan = %s WHERE id = %s", ("pro", user_id))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Force-upgraded user_id=%s to pro in debug mode", user_id)
    except Exception as e:
        logger.exception("Error updating user plan in dev_force_pro")
        return f"Error updating user plan: {e}", 500

    return f"User {user_id} is now Pro."


# -------------------------
# STRIPE CHECKOUT
# -------------------------
@app.route("/create-checkout-session", methods=["POST"])
@login_required
def create_checkout_session():
    user = get_current_user()
    user_id = user.get("id")
    user_email = user.get("email")

    if not user_id:
        return jsonify({"error": "No logged-in user found."}), 401

    payload = request.get_json(silent=True) or {}
    requested_plan = payload.get("plan") or request.form.get("plan") or request.args.get("plan") or "pro"
    normalized_plan = normalize_plan_key(requested_plan)

    if normalized_plan == "free":
        return jsonify({"error": "Starter does not require checkout."}), 400

    price_id = get_price_id_for_plan(normalized_plan)
    if not price_id:
        if normalized_plan == "simple":
            return jsonify({"error": "Missing STRIPE_PRICE_SIMPLE or STRIPE_PRICE_SIMPLE_MONTHLY configuration"}), 500
        if normalized_plan == "enterprise":
            return jsonify({"error": "Missing STRIPE_PRICE_ENTERPRISE / STRIPE_PRICE_BUSINESS configuration"}), 500
        return jsonify({"error": "Missing STRIPE_PRICE_PRO or STRIPE_PRICE_PRO_MONTHLY"}), 500

    base_url = APP_BASE_URL or request.host_url.rstrip("/")
    lang = normalize_lang(request.args.get("lang", "en"))

    try:
        checkout_session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": price_id, "quantity": 1}],
            allow_promotion_codes=True,
            success_url=f"{base_url}/billing/success?session_id={{CHECKOUT_SESSION_ID}}&lang={lang}",
            cancel_url=f"{base_url}/billing/cancel?lang={lang}",
            metadata={
                "user_id": str(user_id),
                "user_email": user_email or "",
                "plan_key": normalized_plan,
            },
            subscription_data={
                "metadata": {
                    "user_id": str(user_id),
                    "user_email": user_email or "",
                    "plan_key": normalized_plan,
                }
            },
            client_reference_id=str(user_id),
            customer_email=user_email if user_email else None,
        )
        return jsonify({"url": checkout_session.url})

    except Exception as e:
        logger.exception("Stripe create_checkout_session error")
        return jsonify({"error": str(e)}), 500


@app.route("/public/<string:token>/create-pay-session", methods=["POST"])

def create_public_invoice_pay_session(token):
    base_url = APP_BASE_URL or request.host_url.rstrip("/")
    currency = STRIPE_CURRENCY

    inv = get_invoice_by_public_token(token)
    if not inv:
        return jsonify({"error": "Invoice not found."}), 404

    owner_plan = get_user_plan_by_user_id(inv["user_id"])
    if not can_collect_payments(owner_plan):
        return jsonify({"error": "Pay Now is not enabled for this invoice."}), 403

    if inv["status"] == "Paid" or inv["balance"] <= 0:
        return jsonify({"error": "This invoice is already paid."}), 400

    amount_cents = money_to_cents(inv["balance"])
    if amount_cents < 50:
        return jsonify({"error": "Balance too small to charge via Stripe."}), 400

    invoice_label = inv["invoice_number"] or f"#{inv['invoice_id']}"
    line_desc = f"Invoice {invoice_label} — Balance Due"

    try:
        checkout_kwargs = {
            "mode": "payment",
            "payment_method_types": ["card"],
            "line_items": [
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
            "success_url": f"{base_url}/public/{token}?paid=1&session_id={{CHECKOUT_SESSION_ID}}",
            "cancel_url": f"{base_url}/public/{token}?canceled=1",
            "metadata": {
                "kind": "invoice_payment",
                "invoice_id": str(inv["invoice_id"]),
                "token": token,
                "public_token": token,
                "invoice_user_id": str(inv["user_id"]),
            },
        }

        payment_setup = get_user_payment_setup(inv["user_id"])
        destination_account = payment_setup.get("stripe_connect_account_id")
        if payment_setup.get("is_ready") and destination_account:
            checkout_kwargs["payment_intent_data"] = {
                "transfer_data": {
                    "destination": destination_account,
                },
                "metadata": {
                    "kind": "invoice_payment",
                    "invoice_id": str(inv["invoice_id"]),
                    "public_token": token,
                    "invoice_user_id": str(inv["user_id"]),
                    "connected_account_id": destination_account,
                },
            }

        checkout_session = stripe.checkout.Session.create(**checkout_kwargs)
        return jsonify({"url": checkout_session.url})

    except Exception as e:
        logger.exception("Stripe create_public_invoice_pay_session error")
        return jsonify({"error": str(e)}), 500


@app.route("/billing/success")
def billing_success():
    session_id = request.args.get("session_id")
    lang = normalize_lang(request.args.get("lang", "en"))

    if not session_id:
        return "Missing session_id", 400

    try:
        cs = stripe.checkout.Session.retrieve(session_id)

        metadata = cs.get("metadata") or {}
        user_id_str = metadata.get("user_id") or cs.get("client_reference_id")
        plan_key = normalize_plan_key(metadata.get("plan_key") or "pro")

        customer_id = cs.get("customer")
        subscription_id = cs.get("subscription")

        logger.info(
            "[BillingSuccess] session_id=%s user_id_str=%s customer_id=%s subscription_id=%s plan_key=%s",
            session_id,
            user_id_str,
            customer_id,
            subscription_id,
            plan_key,
        )

        if not user_id_str:
            logger.warning("[BillingSuccess] No user_id on session; cannot upgrade")
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
            (plan_key, customer_id, subscription_id, user_id),
        )
        conn.commit()
        updated = cursor.rowcount
        cursor.close()
        conn.close()

        logger.info("[BillingSuccess] DB updated rows=%s for user_id=%s", updated, user_id)

        if updated == 0:
            logger.warning("[BillingSuccess] No user row matched for user_id=%s", user_id)
            return redirect(url_for("pricing", lang=lang, canceled=1))

        # Re-bind the browser session to the upgraded user just in case
        session["user_id"] = user_id
        session.permanent = True
        session.modified = True

    except Exception:
        logger.exception("[BillingSuccess] error")
        return redirect(url_for("pricing", lang=lang, canceled=1))

    return redirect(
        url_for(
            "pricing",
            lang=lang,
            upgraded=1,
            refresh=int(datetime.utcnow().timestamp()),
        )
    )


@app.route("/billing/cancel")
def billing_cancel():
    lang = normalize_lang(request.args.get("lang", "en"))
    return redirect(url_for("pricing", canceled=1, lang=lang))


@app.route("/billing/manage", methods=["GET", "POST"])
@login_required
def billing_manage():
    lang = normalize_lang(request.args.get("lang", "en"))
    user = get_current_user()
    user_id = user.get("id")

    if not STRIPE_SECRET_KEY:
        logger.warning("[BillingManage] STRIPE_SECRET_KEY is not configured")
        return redirect(url_for("pricing", lang=lang))

    if not user_id:
        return redirect(url_for("login", lang=lang))

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT stripe_customer_id, stripe_subscription_id, plan
        FROM users
        WHERE id = %s
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        logger.warning("[BillingManage] No user row found for user_id=%s", user_id)
        return redirect(url_for("pricing", lang=lang))

    stripe_customer_id, stripe_subscription_id, plan = row

    if not stripe_customer_id:
        logger.info("[BillingManage] No Stripe customer found for user_id=%s", user_id)
        return redirect(url_for("pricing", lang=lang))

    return_url = f"{APP_BASE_URL or request.host_url.rstrip('/')}/pricing?lang={lang}"

    try:
        portal_session = stripe.billing_portal.Session.create(
            customer=stripe_customer_id,
            return_url=return_url,
        )
        return redirect(portal_session.url)
    except Exception:
        logger.exception("[BillingManage] Failed to create Stripe billing portal session")
        return redirect(url_for("pricing", lang=lang))


@app.route("/manage-subscription", methods=["GET"])
@login_required
def manage_subscription():
    user = get_current_user()
    user_id = user.get("id")

    if not user_id:
        return redirect(url_for("login"))

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT stripe_customer_id, plan
        FROM users
        WHERE id = %s
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        return redirect(url_for("pricing", missing_billing=1))

    stripe_customer_id, plan = row

    if not stripe_customer_id:
        return redirect(url_for("pricing", no_subscription=1))

    base_url = APP_BASE_URL or request.host_url.rstrip("/")
    lang = normalize_lang(request.args.get("lang", "en"))

    try:
        portal_session = stripe.billing_portal.Session.create(
            customer=stripe_customer_id,
            return_url=f"{base_url}/pricing?lang={lang}",
        )
        return redirect(portal_session.url)
    except Exception as e:
        logger.exception("Stripe customer portal error")
        return redirect(url_for("pricing", portal_error=1, lang=lang))


@app.route("/settings/payments/connect", methods=["GET"])
@login_required
def connect_payment_account():
    lang = normalize_lang(request.args.get("lang", "en"))
    user = get_current_user()
    user_id = user.get("id")

    if not user_id:
        return redirect(url_for("login", lang=lang))

    if not can_collect_payments(user):
        return redirect(url_for("settings", payments_unavailable=1, lang=lang))

    if not STRIPE_SECRET_KEY:
        logger.warning("[StripeConnect] STRIPE_SECRET_KEY is not configured")
        return redirect(url_for("settings", payments_error=1, lang=lang))

    try:
        account = get_or_create_stripe_connect_account(user_id, user.get("email") or "")

        account_link = stripe.AccountLink.create(
            account=account.get("id"),
            refresh_url=build_stripe_connect_refresh_url(lang),
            return_url=build_stripe_connect_return_url(lang),
            type="account_onboarding",
        )

        logger.info(
            "[StripeConnect] Started onboarding for user_id=%s account_id=%s",
            user_id,
            account.get("id"),
        )

        return redirect(account_link.url)

    except Exception as e:
        logger.exception("[StripeConnect] Failed to start onboarding for user_id=%s: %s", user_id, e)
        return redirect(url_for("settings", payments_error=1, lang=lang))


@app.route("/settings/payments/dashboard", methods=["GET"])
@login_required
def payment_account_dashboard():
    lang = normalize_lang(request.args.get("lang", "en"))
    user = get_current_user()
    user_id = user.get("id")
    if not can_collect_payments(user):
        return redirect(url_for("settings", payments_unavailable=1, lang=lang))
    payment_setup = sync_stripe_connect_status_for_user(user_id)
    account_id = payment_setup.get("stripe_connect_account_id")

    if not STRIPE_SECRET_KEY or not account_id:
        return redirect(url_for("settings", payments_missing=1, lang=lang))

    try:
        login_link = stripe.Account.create_login_link(account_id)
        return redirect(login_link.url)
    except Exception:
        logger.exception("Failed to create Stripe Connect dashboard login for user_id=%s", user_id)
        return redirect(url_for("settings", payments_error=1, lang=lang))


# -------------------------
# SERVICES / LISTINGS
# -------------------------
@app.route("/services", methods=["GET"])
@login_required
def services_page():
    current_user = get_current_user()
    user_id = current_user["id"]
    lang = get_request_lang()

    services = get_user_services(user_id, include_inactive=True)

    edit_service_id = request.args.get("edit")
    edit_service = None
    if edit_service_id:
        try:
            edit_service = get_service_by_id(int(edit_service_id), user_id)
        except (TypeError, ValueError):
            edit_service = None

    return render_template(
        "services.html",
        lang=lang,
        services=services,
        edit_service=edit_service,
        service_error=request.args.get("service_error") or "",
        service_success=request.args.get("service_success") or "",
    )


@app.route("/services/create", methods=["POST"])
@login_required
def create_service_route():
    current_user = get_current_user()
    user_id = current_user["id"]
    lang = get_request_lang()

    validation = validate_service_form(
        request.form.get("name"),
        request.form.get("description"),
        request.form.get("price"),
    )

    if not validation["ok"]:
        return redirect(
            lang_url_for(
                "services_page",
                service_error=validation["error"],
            )
        )

    try:
        create_user_service(
            user_id=user_id,
            name=validation["name"],
            description=validation["description"],
            price=validation["price"],
        )
    except Exception:
        logger.exception("Failed creating service for user_id=%s", user_id)
        return redirect(
            lang_url_for(
                "services_page",
                service_error="Failed to create service.",
            )
        )

    return redirect(
        lang_url_for(
            "services_page",
            service_success="Service created successfully.",
        )
    )


@app.route("/services/<int:service_id>/update", methods=["POST"])
@login_required
def update_service_route(service_id):
    current_user = get_current_user()
    user_id = current_user["id"]
    lang = get_request_lang()

    existing = get_service_by_id(service_id, user_id)
    if not existing:
        return redirect(
            lang_url_for(
                "services_page",
                service_error="Service not found.",
            )
        )

    validation = validate_service_form(
        request.form.get("name"),
        request.form.get("description"),
        request.form.get("price"),
    )

    if not validation["ok"]:
        return redirect(
            lang_url_for(
                "services_page",
                edit=service_id,
                service_error=validation["error"],
            )
        )

    try:
        updated = update_user_service(
            service_id=service_id,
            user_id=user_id,
            name=validation["name"],
            description=validation["description"],
            price=validation["price"],
        )
        if not updated:
            return redirect(
                lang_url_for(
                    "services_page",
                    service_error="Service not found.",
                )
            )
    except Exception:
        logger.exception("Failed updating service_id=%s for user_id=%s", service_id, user_id)
        return redirect(
            lang_url_for(
                "services_page",
                edit=service_id,
                service_error="Failed to update service.",
            )
        )

    return redirect(
        lang_url_for(
            "services_page",
            service_success="Service updated successfully.",
        )
    )


@app.route("/services/<int:service_id>/deactivate", methods=["POST"])
@login_required
def deactivate_service_route(service_id):
    current_user = get_current_user()
    user_id = current_user["id"]

    try:
        updated = set_user_service_active(service_id, user_id, False)
        if not updated:
            return redirect(
                lang_url_for(
                    "services_page",
                    service_error="Service not found.",
                )
            )
    except Exception:
        logger.exception("Failed deactivating service_id=%s for user_id=%s", service_id, user_id)
        return redirect(
            lang_url_for(
                "services_page",
                service_error="Failed to deactivate service.",
            )
        )

    return redirect(
        lang_url_for(
            "services_page",
            service_success="Service removed from active listings.",
        )
    )


@app.route("/services/<int:service_id>/activate", methods=["POST"])
@login_required
def activate_service_route(service_id):
    current_user = get_current_user()
    user_id = current_user["id"]

    try:
        updated = set_user_service_active(service_id, user_id, True)
        if not updated:
            return redirect(
                lang_url_for(
                    "services_page",
                    service_error="Service not found.",
                )
            )
    except Exception:
        logger.exception("Failed activating service_id=%s for user_id=%s", service_id, user_id)
        return redirect(
            lang_url_for(
                "services_page",
                service_error="Failed to activate service.",
            )
        )

    return redirect(
        lang_url_for(
            "services_page",
            service_success="Service activated successfully.",
        )
    )


# -------------------------
# PREVIEW
# -------------------------
@app.route("/preview", methods=["GET", "POST"])
@login_required
def preview_invoice():
    lang = normalize_lang(request.args.get("lang", "en"))

    if request.method == "GET":
        return redirect(url_for("home", lang=lang))

    selected_client_id = request.form.get("client_id")
    new_client_name = (request.form.get("new_client_name") or "").strip()
    new_client_email = (request.form.get("new_client_email") or "").strip()
    new_client_company = (request.form.get("new_client_company") or "").strip()
    new_client_phone = (request.form.get("new_client_phone") or "").strip()
    new_client_address = (request.form.get("new_client_address") or "").strip()
    new_client_notes = (request.form.get("new_client_notes") or "").strip()

    notes = request.form.get("invoice_notes") or ""
    terms = request.form.get("invoice_terms") or "Payment due within 30 days."
    template_style = request.form.get("template_style") or "modern"
    signature_data = request.form.get("signature_data") or ""

    descriptions = request.form.getlist("description")
    amounts = request.form.getlist("amount")

    created_at = now_local()
    due_date = created_at + timedelta(days=30)

    items = []
    total = 0.0

    for desc, amt in zip(descriptions, amounts):
        desc = (desc or "").strip()
        amt_val = parse_float(amt, default=None)
        if desc and amt_val is not None and amt_val > 0:
            total += amt_val
            items.append((desc, amt_val))

    user = get_current_user()
    user_id = user["id"]

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
                db_name, db_email, db_company, db_phone, db_address, db_notes = row
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

    profile = get_business_profile_safe()
    business_name = profile.get("business_name") or DEFAULT_BUSINESS_NAME
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
        signature_data=signature_data,
    )


# -------------------------
# SAVE INVOICE
# -------------------------
@app.route("/save", methods=["POST"])
@login_required
def save():
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
    new_client_name = (request.form.get("new_client_name") or "").strip()
    new_client_email = (request.form.get("new_client_email") or "").strip()
    new_client_company = (request.form.get("new_client_company") or "").strip()
    new_client_phone = (request.form.get("new_client_phone") or "").strip()
    new_client_address = (request.form.get("new_client_address") or "").strip()
    new_client_notes = (request.form.get("new_client_notes") or "").strip()
    service_request_id_raw = (request.form.get("service_request_id") or "").strip()

    notes = request.form.get("invoice_notes") or ""
    terms = request.form.get("invoice_terms") or "Payment due within 30 days."
    template_style = request.form.get("template_style") or "modern"
    signature_data = request.form.get("signature_data") or None

    descriptions = request.form.getlist("description")
    amounts = request.form.getlist("amount")

    created_at = now_local()
    status = "Sent"
    due_date = created_at + timedelta(days=30)

    total = 0.0
    cleaned_items = []

    for desc, amt in zip(descriptions, amounts):
        desc = (desc or "").strip()
        amt_val = parse_float(amt, default=None)
        if desc and amt_val is not None and amt_val > 0:
            total += amt_val
            cleaned_items.append((desc, amt_val))

    if not cleaned_items:
        return render_template(
            "upgrade_gate.html",
            title="Invoice needs at least one line item",
            reason="Please add at least one valid line item with an amount greater than zero.",
            required_plan="free",
            plans=PLAN_DEFINITIONS,
        )

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
        client_name_for_invoice = (request.form.get("client") or "Unknown client").strip() or "Unknown client"

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM invoices
        WHERE user_id = %s
        """,
        (user_id,),
    )
    user_invoice_count = cursor.fetchone()[0] or 0
    invoice_number = f"INV-{user_invoice_count + 1:05d}"

    cursor.execute(
        """
        INSERT INTO invoices (
            client,
            amount,
            created_at,
            status,
            invoice_number,
            due_date,
            notes,
            terms,
            template_style,
            client_id,
            user_id,
            signature_data
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            client_name_for_invoice,
            total,
            created_at,
            status,
            invoice_number,
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

    for desc, amt in cleaned_items:
        cursor.execute(
            "INSERT INTO invoice_items (invoice_id, description, amount) VALUES (%s, %s, %s)",
            (invoice_id, desc, amt),
        )

    conn.commit()
    cursor.close()
    conn.close()

    log_invoice_event(
        invoice_id=invoice_id,
        event_type="invoice_created",
        title="Invoice created",
        details=f"Invoice {invoice_number} was created for {client_name_for_invoice}.",
        visibility="both",
    )

    if service_request_id_raw:
        try:
            service_request_id = int(service_request_id_raw)
            link_service_request_to_invoice(service_request_id, user_id, invoice_id)
        except ValueError:
            pass

    return render_template(
        "saved.html",
        invoice_id=invoice_id,
        inv_label=invoice_number,
        client=client_name_for_invoice,
        client_name=client_name_for_invoice,
        amount=total,
        lang=normalize_lang(request.args.get("lang", "en")),
    )


# -------------------------
# INVOICES DASHBOARD
# -------------------------
@app.route("/invoices")
@login_required
def invoices_page():
    update_overdue_statuses()

    lang = normalize_lang(request.args.get("lang") or get_current_user().get("language") or "en")

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

    now = now_local()
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

    status_chart_labels = ["Paid", "Sent", "Overdue"]
    status_chart_values = [
        paid_count,
        status_distribution.get("Sent", 0),
        overdue_count,
    ]

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
            pct = round((total_float / top_total) * 100, 1) if top_total > 0 else 0.0
            top_clients.append([name, total_float, inv_count, pct])

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

    if status_filter in ALLOWED_STATUSES:
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
    cursor.close()
    conn.close()

    invoices = []
    for row in filtered_rows:
        row_list = list(row)
        row_list[2] = float(row_list[2])
        row_list[7] = float(row_list[7])
        invoices.append(row_list)

    filtered_count = len(invoices)

    dashboard_metrics = get_dashboard_receivables_metrics(user_id) if can_use_advanced_dashboard(current_user) else None

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
        dashboard_metrics=dashboard_metrics,
        lang=lang,
    )


# -------------------------
# GLOBAL SEARCH
# -------------------------
@app.route("/search")
@login_required
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

        cursor.close()
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
@login_required
def settings():
    user = get_current_user()
    user_id = user["id"]
    lang = normalize_lang(request.args.get("lang") or user.get("language") or "en")

    profile = get_business_profile()
    feedback_message = None
    feedback_type = None
    editing_service = None

    if request.method == "POST":
        form_type = (request.form.get("form_type") or "profile").strip().lower()

        if form_type == "password":
            current_password = request.form.get("current_password") or ""
            new_password = request.form.get("new_password") or ""
            confirm_password = request.form.get("confirm_password") or ""

            if not current_password or not new_password or not confirm_password:
                feedback_message = "Please fill out all password fields."
                feedback_type = "error"
            elif len(new_password) < 8:
                feedback_message = "New password must be at least 8 characters long."
                feedback_type = "error"
            elif new_password != confirm_password:
                feedback_message = "New password and confirmation do not match."
                feedback_type = "error"
            else:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT password_hash FROM users WHERE id = %s",
                    (user_id,),
                )
                row = cursor.fetchone()

                if not row or not row[0]:
                    feedback_message = "Unable to update password for this account."
                    feedback_type = "error"
                    cursor.close()
                    conn.close()
                elif not check_password_hash(row[0], current_password):
                    feedback_message = "Your current password is incorrect."
                    feedback_type = "error"
                    cursor.close()
                    conn.close()
                else:
                    new_password_hash = generate_password_hash(new_password)
                    cursor.execute(
                        "UPDATE users SET password_hash = %s WHERE id = %s",
                        (new_password_hash, user_id),
                    )
                    conn.commit()
                    cursor.close()
                    conn.close()

                    feedback_message = "Password updated successfully."
                    feedback_type = "success"

        elif form_type == "service_add":
            service_name = (request.form.get("service_name") or "").strip()
            service_description = (request.form.get("service_description") or "").strip()
            service_price = parse_float(request.form.get("service_price"), default=0.0)

            if not service_name:
                feedback_message = "Service name is required."
                feedback_type = "error"
            else:
                create_user_service(
                    user_id=user_id,
                    name=service_name,
                    description=service_description,
                    price=service_price,
                )
                feedback_message = "Service added successfully."
                feedback_type = "success"

        elif form_type == "service_update":
            service_id_raw = (request.form.get("service_id") or "").strip()
            service_name = (request.form.get("service_name") or "").strip()
            service_description = (request.form.get("service_description") or "").strip()
            service_price = parse_float(request.form.get("service_price"), default=0.0)

            try:
                service_id = int(service_id_raw)
            except ValueError:
                service_id = None

            if not service_id:
                feedback_message = "Invalid service selected."
                feedback_type = "error"
            elif not service_name:
                feedback_message = "Service name is required."
                feedback_type = "error"
            else:
                updated = update_user_service(
                    service_id=service_id,
                    user_id=user_id,
                    name=service_name,
                    description=service_description,
                    price=service_price,
                )
                if updated:
                    feedback_message = "Service updated successfully."
                    feedback_type = "success"
                else:
                    feedback_message = "Service could not be updated."
                    feedback_type = "error"

        elif form_type == "service_toggle":
            service_id_raw = (request.form.get("service_id") or "").strip()
            next_active_raw = (request.form.get("next_active") or "").strip().lower()

            try:
                service_id = int(service_id_raw)
            except ValueError:
                service_id = None

            next_active = next_active_raw in ("1", "true", "yes", "on")

            if not service_id:
                feedback_message = "Invalid service selected."
                feedback_type = "error"
            else:
                updated = set_user_service_active(
                    service_id=service_id,
                    user_id=user_id,
                    is_active=next_active,
                )
                if updated:
                    feedback_message = "Service status updated successfully."
                    feedback_type = "success"
                else:
                    feedback_message = "Service status could not be updated."
                    feedback_type = "error"

        else:
            business_name = (request.form.get("business_name") or "").strip()
            email = (request.form.get("email") or "").strip()
            phone = (request.form.get("phone") or "").strip()
            website = (request.form.get("website") or "").strip()
            address = (request.form.get("address") or "").strip()
            logo_url = (request.form.get("logo_url") or "").strip()
            brand_color = (request.form.get("brand_color") or "").strip() or DEFAULT_BRAND_COLOR
            accent_color = (request.form.get("accent_color") or "").strip() or DEFAULT_ACCENT_COLOR
            default_terms = (request.form.get("default_terms") or "").strip()
            default_notes = (request.form.get("default_notes") or "").strip()
            language = normalize_lang(request.form.get("language") or user.get("language") or "en")

            data = {
                "business_name": business_name or DEFAULT_BUSINESS_NAME,
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

            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET language = %s WHERE id = %s",
                (language, user_id),
            )
            conn.commit()
            cursor.close()
            conn.close()

            profile = get_business_profile()
            feedback_message = "Business profile updated successfully."
            feedback_type = "success"
            lang = language

    edit_service_id = request.args.get("edit_service")
    if edit_service_id:
        try:
            editing_service = get_service_by_id(int(edit_service_id), user_id)
        except ValueError:
            editing_service = None

    services = get_user_services(user_id, include_inactive=True)

    return render_template(
        "settings.html",
        profile=profile,
        feedback_message=feedback_message,
        feedback_type=feedback_type,
        payment_setup=sync_stripe_connect_status_for_user(user_id),
        current_plan=normalize_plan_key(user.get("plan") or "free"),
        lang=lang,
        services=services,
        editing_service=editing_service,
    )


# -------------------------
# CLIENTS
# -------------------------
@app.route("/clients")
@login_required
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
    cursor.close()
    conn.close()

    return render_template("clients.html", clients=clients, lang=normalize_lang(request.args.get("lang") or get_current_user().get("language") or "en"))


@app.route("/clients/add", methods=["POST"])
@login_required
def add_client():
    name = (request.form.get("name") or "").strip()
    email = (request.form.get("email") or "").strip()
    company = (request.form.get("company") or "").strip()
    phone = (request.form.get("phone") or "").strip()
    address = (request.form.get("address") or "").strip()
    notes = (request.form.get("notes") or "").strip()

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
        (name, email or None, company or None, phone or None, address or None, notes or None, user_id),
    )
    conn.commit()
    cursor.close()
    conn.close()

    return redirect("/clients")


@app.route("/clients/delete/<int:client_id>")
@login_required
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
@login_required
def add_payment(invoice_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    user = get_current_user()
    user_id = user["id"]

    cursor.execute(
        """
        SELECT id, client, amount, status, invoice_number, due_date, client_id
        FROM invoices
        WHERE id = %s AND user_id = %s
        """,
        (invoice_id, user_id),
    )
    invoice = cursor.fetchone()

    if not invoice:
        cursor.close()
        conn.close()
        return "Invoice not found", 404

    invoice_id_db, client_name, amount, status, invoice_number, due_date, client_id = invoice
    amount_float = float(amount or 0)
    inv_label = invoice_number or f"#{invoice_id_db}"

    feedback_message = None
    feedback_type = None

    payment_summary = get_invoice_payment_summary(invoice_id_db) or {}
    existing_balance = float(payment_summary.get("balance") or 0)

    if request.method == "POST":
        pay_amount = parse_float(request.form.get("amount"), default=0.0)
        method = (request.form.get("method") or "").strip()
        note = (request.form.get("note") or "").strip()
        payment_date_raw = (request.form.get("payment_date") or "").strip()
        is_deposit = (request.form.get("is_deposit") or "").strip().lower() in ("1", "true", "yes", "on")

        occurred_at = now_local()
        if payment_date_raw:
            try:
                occurred_at = datetime.strptime(payment_date_raw, "%Y-%m-%d")
            except ValueError:
                occurred_at = now_local()

        if pay_amount <= 0:
            feedback_message = "Payment amount must be greater than zero."
            feedback_type = "error"
        elif existing_balance <= 0.0001:
            feedback_message = "This invoice is already paid in full."
            feedback_type = "error"
        elif pay_amount > (existing_balance + 0.009):
            feedback_message = f"Payment exceeds the remaining balance of {format_currency(existing_balance)}."
            feedback_type = "error"
        else:
            cursor.execute(
                """
                INSERT INTO payments (
                    invoice_id,
                    amount,
                    method,
                    note,
                    payment_source,
                    payment_status,
                    occurred_at,
                    recorded_by_user_id,
                    is_deposit,
                    is_final_payment
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    invoice_id_db,
                    pay_amount,
                    method or None,
                    note or None,
                    "manual",
                    "succeeded",
                    occurred_at,
                    user_id,
                    bool(is_deposit),
                    False,
                ),
            )

            cursor.execute(
                """
                UPDATE invoices
                SET last_payment_recorded_at = %s,
                    last_collection_action_at = %s
                WHERE id = %s
                """,
                (occurred_at, now_local(), invoice_id_db),
            )

            conn.commit()

            payment_summary = sync_invoice_status(invoice_id_db) or get_invoice_payment_summary(invoice_id_db) or {}
            total_paid = float(payment_summary.get("total_paid") or 0)
            balance = float(payment_summary.get("balance") or 0)

            method_label = normalize_method_label(method)
            details = f"Recorded payment of {format_currency(pay_amount)} via {method_label}."
            if note:
                details += f" Note: {note}"

            log_invoice_event(
                invoice_id=invoice_id_db,
                event_type="manual_payment_added",
                title="Payment recorded",
                details=details,
                visibility="both",
            )

            if balance > 0.0001:
                log_invoice_event(
                    invoice_id=invoice_id_db,
                    event_type="partial_payment_received",
                    title="Partial payment received",
                    details=f"Total paid is now {format_currency(total_paid)}. Remaining balance: {format_currency(balance)}.",
                    visibility="both",
                )
            else:
                log_invoice_event(
                    invoice_id=invoice_id_db,
                    event_type="final_payment_received",
                    title="Final payment received",
                    details=f"Invoice {inv_label} is now paid in full.",
                    visibility="both",
                )

            cursor.execute(
                """
                SELECT c.email
                FROM invoices i
                LEFT JOIN clients c ON i.client_id = c.id
                WHERE i.id = %s
                """,
                (invoice_id_db,),
            )
            email_row = cursor.fetchone()
            client_email = email_row[0] if email_row else None

            if client_email and plan_allows("pro"):
                if balance > 0.0001:
                    send_invoice_notification_email(invoice_id_db, client_email, "partial_payment_confirmation")
                else:
                    send_invoice_notification_email(invoice_id_db, client_email, "paid_in_full_confirmation")

            if balance > 0.0001:
                feedback_message = (
                    f"Recorded payment of {format_currency(pay_amount)} on invoice {inv_label}. "
                    f"Remaining balance: {format_currency(balance)}."
                )
            else:
                feedback_message = f"Recorded final payment. Invoice {inv_label} is now paid in full."

            feedback_type = "success"

    payment_summary = get_invoice_payment_summary(invoice_id_db) or {}
    status = derive_invoice_display_status(payment_summary)

    cursor.execute(
        """
        SELECT
            amount,
            method,
            note,
            COALESCE(occurred_at, created_at),
            COALESCE(payment_source, 'manual'),
            COALESCE(payment_status, 'succeeded')
        FROM payments
        WHERE invoice_id = %s
        ORDER BY COALESCE(occurred_at, created_at) DESC, id DESC
        """,
        (invoice_id_db,),
    )
    payments = cursor.fetchall()

    cursor.close()
    conn.close()

    total_paid = float(payment_summary.get("total_paid") or 0)
    balance = float(payment_summary.get("balance") or 0)
    percent_paid = float(payment_summary.get("percent_paid") or 0)

    return render_template(
        "add_payment.html",
        invoice_id=invoice_id_db,
        client_name=client_name,
        amount=amount_float,
        status=status,
        invoice_number=invoice_number,
        inv_label=inv_label,
        due_date=due_date,
        payments=payments,
        total_paid=total_paid,
        balance=balance,
        percent_paid=percent_paid,
        payment_summary=payment_summary,
        feedback_message=feedback_message,
        feedback_type=feedback_type,
    )


# -------------------------
# EDIT / UPDATE / DELETE INVOICE
# -------------------------
@app.route("/edit/<int:invoice_id>")
@login_required
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
        c.close()
        conn.close()
        return "Invoice not found", 404

    c.execute(
        "SELECT description, amount FROM invoice_items WHERE invoice_id = %s",
        (invoice_id,),
    )
    items = c.fetchall()

    c.close()
    conn.close()

    return render_template("edit.html", invoice_id=invoice_id, client=invoice[1], items=items)


@app.route("/update/<int:invoice_id>", methods=["POST"])
@login_required
def update(invoice_id):
    client_name = (request.form.get("client") or "").strip() or "Unknown client"
    descriptions = request.form.getlist("description")
    amounts = request.form.getlist("amount")

    total = 0.0
    cleaned_items = []

    for desc, amt in zip(descriptions, amounts):
        desc = (desc or "").strip()
        amt_val = parse_float(amt, default=None)
        if desc and amt_val is not None and amt_val > 0:
            total += amt_val
            cleaned_items.append((desc, amt_val))

    if not cleaned_items:
        return redirect(f"/edit/{invoice_id}")

    current_user = get_current_user()
    user_id = current_user["id"]

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        "SELECT invoice_number FROM invoices WHERE id = %s AND user_id = %s",
        (invoice_id, user_id),
    )
    existing = c.fetchone()
    if not existing:
        c.close()
        conn.close()
        return "Invoice not found", 404

    invoice_number = existing[0] or f"#{invoice_id}"

    c.execute(
        "UPDATE invoices SET client = %s, amount = %s WHERE id = %s AND user_id = %s",
        (client_name, total, invoice_id, user_id),
    )

    c.execute("DELETE FROM invoice_items WHERE invoice_id = %s", (invoice_id,))

    for desc, amt in cleaned_items:
        c.execute(
            "INSERT INTO invoice_items (invoice_id, description, amount) VALUES (%s, %s, %s)",
            (invoice_id, desc, amt),
        )

    conn.commit()
    c.close()
    conn.close()

    log_invoice_event(
        invoice_id=invoice_id,
        event_type="invoice_updated",
        title="Invoice updated",
        details=f"Invoice {invoice_number} was updated.",
        visibility="private",
    )

    sync_invoice_status(invoice_id)

    return redirect("/invoices")


@app.route("/update-status/<int:invoice_id>/<string:new_status>")
@login_required
def update_status(invoice_id, new_status):
    if new_status not in ALLOWED_STATUSES:
        return "Invalid status", 400

    conn = get_db_connection()
    c = conn.cursor()

    current_user = get_current_user()
    user_id = current_user["id"]

    c.execute(
        "SELECT invoice_number FROM invoices WHERE id = %s AND user_id = %s",
        (invoice_id, user_id),
    )
    existing = c.fetchone()
    if not existing:
        c.close()
        conn.close()
        return "Invoice not found", 404

    invoice_number = existing[0] or f"#{invoice_id}"

    c.close()
    conn.close()

    if new_status == "Paid":
        success, err = mark_invoice_paid(
            invoice_id=invoice_id,
            user_id=user_id,
            note="Marked as paid manually from invoice status control.",
        )
        if not success:
            return err or "Failed to mark invoice as paid.", 400

        log_invoice_event(
            invoice_id=invoice_id,
            event_type="status_changed",
            title="Status updated",
            details=f"Invoice {invoice_number} status changed to Paid.",
            visibility="both",
        )
        return redirect("/invoices")

    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        "UPDATE invoices SET status = %s WHERE id = %s AND user_id = %s",
        (new_status, invoice_id, user_id),
    )

    conn.commit()
    c.close()
    conn.close()

    log_invoice_event(
        invoice_id=invoice_id,
        event_type="status_changed",
        title="Status updated",
        details=f"Invoice {invoice_number} status changed to {new_status}.",
        visibility="both",
    )

    return redirect("/invoices")


@app.route("/delete/<int:invoice_id>")
@login_required
def delete(invoice_id):
    lang = normalize_lang(request.args.get("lang", "en"))

    conn = get_db_connection()
    c = conn.cursor()

    current_user = get_current_user()
    user_id = current_user["id"]

    c.execute(
        """
        SELECT id, client, amount, invoice_number
        FROM invoices
        WHERE id = %s AND user_id = %s
        """,
        (invoice_id, user_id),
    )
    row = c.fetchone()

    if not row:
        c.close()
        conn.close()
        return "Invoice not found", 404

    _invoice_id, client_name, amount, invoice_number = row
    amount_float = float(amount or 0)
    inv_label = invoice_number or f"#{invoice_id}"

    c.execute("DELETE FROM invoice_items WHERE invoice_id = %s", (invoice_id,))
    c.execute("DELETE FROM invoices WHERE id = %s", (invoice_id,))

    conn.commit()
    c.close()
    conn.close()

    return render_template(
        "deleted.html",
        invoice_id=invoice_id,
        inv_label=inv_label,
        client_name=client_name,
        amount=amount_float,
        lang=lang,
    )


# -------------------------
# PDF GENERATION
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
        c.close()
        conn.close()
        return None, "Invoice not found"

    (
        client_name,
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

    profile = get_business_profile_safe()
    business_name = profile.get("business_name") or DEFAULT_BUSINESS_NAME

    c.execute(
        "SELECT description, amount FROM invoice_items WHERE invoice_id = %s",
        (invoice_id,),
    )
    items = c.fetchall()
    c.close()
    conn.close()

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=LETTER)
    page_width, page_height = LETTER

    header_bar_color = (21 / 255, 27 / 255, 84 / 255)
    accent_color = header_bar_color
    title_text = business_name

    if template_style == "minimal":
        header_bar_color = (0.18, 0.20, 0.24)
        accent_color = (0.6, 0.6, 0.65)
    elif template_style == "bold":
        header_bar_color = (0.97, 0.45, 0.09)
        accent_color = (0.97, 0.45, 0.09)
    elif template_style == "doodle":
        header_bar_color = (0.33, 0.27, 0.96)
        accent_color = (0.33, 0.27, 0.96)

        pdf.setFillColorRGB(0.93, 0.95, 1.0)
        pdf.circle(60, page_height - 120, 26, fill=1, stroke=0)
        pdf.setFillColorRGB(0.96, 0.92, 1.0)
        pdf.circle(page_width - 80, page_height - 200, 30, fill=1, stroke=0)
        pdf.setFillColorRGB(0.90, 0.96, 0.98)
        pdf.rect(page_width - 150, 40, 120, 60, fill=1, stroke=0)

    pdf.setFillColorRGB(*header_bar_color)
    pdf.rect(0, page_height - 60, page_width, 60, fill=1, stroke=0)

    pdf.setFillColorRGB(1, 1, 1)
    pdf.setFont("Helvetica-Bold", 22)
    pdf.drawString(72, page_height - 40, title_text)

    pdf.setFillColorRGB(0.1, 0.1, 0.15)
    pdf.setFont("Helvetica", 11)

    inv_label = invoice_number or f"#{invoice_id}"
    y = page_height - 90

    pdf.drawString(72, y, f"Invoice: {inv_label}")
    y -= 16
    pdf.drawString(72, y, f"Client: {client_name}")
    y -= 16

    if created_at:
        pdf.drawString(72, y, f"Created: {created_at.strftime('%Y-%m-%d %I:%M %p')}")
        y -= 16
    if due_date:
        pdf.drawString(72, y, f"Due: {due_date.strftime('%Y-%m-%d')}")
        y -= 16

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
    pdf.drawRightString(right_box_left + 170, right_box_top - 30, f"${amount_float:,.2f}")

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
        pdf.drawRightString(page_width - 72, y, f"${amt_float:,.2f}")
        y -= 16

        if y < 120:
            pdf.showPage()
            page_width, page_height = LETTER
            y = page_height - 100
            pdf.setFont("Helvetica", 10)
            pdf.setFillColorRGB(0.1, 0.1, 0.15)

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

    if signature_data:
        try:
            if signature_data.startswith("data:image"):
                _, b64_data = signature_data.split(",", 1)
            else:
                b64_data = signature_data

            sig_bytes = base64.b64decode(b64_data)
            sig_buf = io.BytesIO(sig_bytes)
            sig_img = ImageReader(sig_buf)

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
                mask="auto",
            )

            y -= sig_box_height + 16
        except Exception:
            logger.warning("Failed to render signature for invoice_id=%s", invoice_id)

    if y < 80:
        pdf.showPage()
        page_width, page_height = LETTER
        y = page_height - 120

    pdf.setStrokeColorRGB(0.85, 0.87, 0.9)
    pdf.line(72, y, page_width - 72, y)
    y -= 24

    pdf.setFont("Helvetica-Bold", 12)
    pdf.setFillColorRGB(*accent_color)
    pdf.drawRightString(page_width - 72, y, f"Total Due: ${amount_float:,.2f}")

    pdf.setFont("Helvetica", 8)
    pdf.setFillColorRGB(0.45, 0.45, 0.45)
    pdf.drawCentredString(
        page_width / 2,
        20,
        "Created with BillBeam • Modern invoicing made simple • billbeam.app"
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
    paid_flag = request.args.get("paid")
    session_id = request.args.get("session_id")

    if paid_flag and session_id:
        try:
            session_obj = stripe.checkout.Session.retrieve(session_id)

            session_mode = (session_obj.get("mode") or "").lower()
            payment_status = (session_obj.get("payment_status") or "").lower()
            metadata = session_obj.get("metadata") or {}

            metadata_token = metadata.get("token") or metadata.get("public_token")
            invoice_id_str = metadata.get("invoice_id")

            logger.info(
                "[PublicInvoiceFallback] session_id=%s mode=%s payment_status=%s metadata=%s",
                session_id,
                session_mode,
                payment_status,
                metadata,
            )

            if (
                session_mode == "payment"
                and payment_status == "paid"
                and metadata_token == token
                and invoice_id_str
            ):
                _record_invoice_payment_from_checkout_session(session_obj)
            else:
                logger.info("[PublicInvoiceFallback] Session did not qualify for invoice payment sync")

        except Exception:
            logger.exception("[PublicInvoiceFallback] error verifying Stripe session")

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
        record_public_invoice_view(invoice_id, token)

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
        SELECT
            amount,
            method,
            note,
            COALESCE(occurred_at, created_at),
            COALESCE(payment_source, 'manual'),
            COALESCE(payment_status, 'succeeded')
        FROM payments
        WHERE invoice_id = %s
        ORDER BY COALESCE(occurred_at, created_at) DESC, id DESC
        """,
        (invoice_id,),
    )
    payments = cursor.fetchall()

    payment_summary = get_invoice_payment_summary(invoice_id) or {}
    total_paid = float(payment_summary.get("total_paid") or 0)
    balance = float(payment_summary.get("balance") or 0)
    percent_paid = float(payment_summary.get("percent_paid") or 0)
    paid_at = payment_summary.get("last_payment_at")
    view_summary = get_invoice_view_summary(invoice_id)

    invoice_public_summary = get_invoice_by_public_token(token)
    owner_user_id = invoice_public_summary["user_id"] if invoice_public_summary else None
    owner_plan = get_user_plan_by_user_id(owner_user_id) if owner_user_id else "free"

    is_paid_in_full = payment_summary.get("is_paid_in_full", False)
    show_pay_button = can_collect_payments(owner_plan) and balance > 0.0001 and not is_paid_in_full

    owner_services = get_user_services(owner_user_id, include_inactive=False) if owner_user_id else []
    show_portal_branding = can_use_branding(owner_plan)

    if amount_float > 0 and total_paid >= amount_float and status != "Paid":
        try:
            cursor.execute(
                "UPDATE invoices SET status = 'Paid' WHERE id = %s",
                (invoice_id,),
            )
            conn.commit()
            status = "Paid"
            logger.info("[PublicInvoice] Safety-synced invoice %s to Paid", invoice_id)
        except Exception as e:
            logger.warning("[PublicInvoice] Failed safety sync for invoice %s: %s", invoice_id, e)

    if is_paid_in_full:
        status = "Paid"
        balance = 0.0

    cursor.close()
    conn.close()

    pdf_url = f"/history-pdf/{invoice_id}"
    invoice_events = get_invoice_events(invoice_id, public_only=True)

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
        paid_at=paid_at,
        is_paid_in_full=is_paid_in_full,
        pdf_url=pdf_url,
        is_public_view=True,
        public_token=token,
        signature_data=signature_data,
        invoice_events=invoice_events,
        percent_paid=percent_paid,
        payment_summary=payment_summary,
        view_summary=view_summary,
        show_pay_button=show_pay_button,
        owner_plan=owner_plan,
        services=owner_services,
        show_portal_branding=show_portal_branding,
        owner_user_id=owner_user_id,
        collection_recommendation=get_invoice_collection_recommendation(
            status,
            payment_summary,
            view_summary,
            None,
        ),
    )


@app.route("/invoice/<int:invoice_id>")
@login_required
def invoice_detail(invoice_id):
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
        SELECT
            amount,
            method,
            note,
            COALESCE(occurred_at, created_at),
            COALESCE(payment_source, 'manual'),
            COALESCE(payment_status, 'succeeded')
        FROM payments
        WHERE invoice_id = %s
        ORDER BY COALESCE(occurred_at, created_at) DESC, id DESC
        """,
        (invoice_id,),
    )
    payments = cursor.fetchall()

    payment_summary = get_invoice_payment_summary(invoice_id) or {}
    total_paid = float(payment_summary.get("total_paid") or 0)
    balance = float(payment_summary.get("balance") or 0)
    percent_paid = float(payment_summary.get("percent_paid") or 0)
    view_summary = get_invoice_view_summary(invoice_id)
    owner_plan = get_user_plan_by_user_id(user_id)
    show_pay_button = can_collect_payments(owner_plan) and balance > 0.0001
    owner_services = get_user_services(user_id, include_inactive=False)
    show_portal_branding = can_use_branding(owner_plan)

    cursor.execute(
        """
        SELECT last_reminder_sent_at
        FROM invoices
        WHERE id = %s
        """,
        (invoice_id,),
    )
    reminder_row = cursor.fetchone()
    last_reminder_sent_at = reminder_row[0] if reminder_row else None

    cursor.close()
    conn.close()

    pdf_url = f"/history-pdf/{invoice_id}"
    invoice_events = get_invoice_events(invoice_id, public_only=False)

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
        is_public_view=False,
        public_token=None,
        signature_data=signature_data,
        invoice_events=invoice_events,
        paid_at=payment_summary.get("last_payment_at"),
        percent_paid=percent_paid,
        payment_summary=payment_summary,
        view_summary=view_summary,
        last_reminder_sent_at=last_reminder_sent_at,
        show_pay_button=show_pay_button,
        owner_plan=owner_plan,
        services=owner_services,
        show_portal_branding=show_portal_branding,
        owner_user_id=user_id,
        collection_recommendation=get_invoice_collection_recommendation(
            status,
            payment_summary,
            view_summary,
            last_reminder_sent_at,
        ),
    )


# -------------------------
# EMAIL
# -------------------------
def mark_invoice_last_emailed(invoice_id: int, to_email: str, is_reminder: bool = False):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        now_dt = now_local()

        if is_reminder:
            cur.execute(
                """
                UPDATE invoices
                SET last_emailed_at = %s,
                    last_emailed_to = %s,
                    last_reminder_sent_at = %s,
                    last_collection_action_at = %s
                WHERE id = %s
                """,
                (now_dt, to_email, now_dt, now_dt, invoice_id),
            )
        else:
            cur.execute(
                """
                UPDATE invoices
                SET last_emailed_at = %s,
                    last_emailed_to = %s
                WHERE id = %s
                """,
                (now_dt, to_email, invoice_id),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to update invoice email timestamps for invoice_id=%s", invoice_id)
    finally:
        cur.close()
        conn.close()

def send_email_via_resend(to_email: str, subject: str, body_text: str, pdf_bytes: bytes, filename: str):
    api_key = os.environ.get("RESEND_API_KEY")
    resend_from = os.environ.get("RESEND_FROM")

    if not api_key:
        return False, "Resend configuration missing: RESEND_API_KEY is not set."

    if not resend_from:
        return False, (
            "Resend configuration missing: RESEND_FROM is not set. "
            "Set RESEND_FROM to something like 'BillBeam <billing@billbeam.com>'."
        )

    if "gmail.com" in resend_from.lower():
        return False, (
            "Resend cannot send from a gmail.com address. "
            f"Current RESEND_FROM value is: '{resend_from}'. "
            "Use your verified domain, for example 'BillBeam <billing@yourdomain.com>'."
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


def send_invoice_email(invoice_id: int, to_email: str, subject: str, body_text: str, email_type: str = "invoice"):
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
            is_reminder = email_type == "reminder"
            mark_invoice_last_emailed(invoice_id, to_email, is_reminder=is_reminder)

            event_type = "reminder_sent" if is_reminder else "invoice_emailed"
            event_title = "Reminder sent" if is_reminder else "Invoice emailed"

            log_invoice_event(
                invoice_id=invoice_id,
                event_type=event_type,
                title=event_title,
                details=f"{event_title} to {to_email}.",
                visibility="private",
            )
            return True, None
        return False, api_err

    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    smtp_from = os.environ.get("SMTP_FROM") or smtp_user

    if not smtp_host or not smtp_from:
        return False, (
            "No email provider available. Configure Resend (RESEND_API_KEY & RESEND_FROM) "
            "or SMTP (SMTP_HOST & SMTP_FROM)."
        )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = to_email

    body_text += """

—
Created with BillBeam
Modern invoicing made simple
https://billbeam.app
"""

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

    is_reminder = email_type == "reminder"
    mark_invoice_last_emailed(invoice_id, to_email, is_reminder=is_reminder)

    event_type = "reminder_sent" if is_reminder else "invoice_emailed"
    event_title = "Reminder sent" if is_reminder else "Invoice emailed"

    log_invoice_event(
        invoice_id=invoice_id,
        event_type=event_type,
        title=event_title,
        details=f"{event_title} to {to_email}.",
        visibility="private",
    )

    return True, None


def build_invoice_email_defaults(invoice_id: int, email_type: str = "invoice"):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            i.id,
            i.client,
            i.amount,
            i.created_at,
            i.status,
            i.invoice_number,
            i.due_date,
            i.last_emailed_at,
            i.last_emailed_to,
            i.last_reminder_sent_at,
            c.email,
            i.public_token
        FROM invoices i
        LEFT JOIN clients c ON i.client_id = c.id
        WHERE i.id = %s
        """,
        (invoice_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

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
        last_reminder_sent_at,
        client_email,
        public_token_db,
    ) = row

    profile = get_business_profile_safe()
    business_name = profile.get("business_name") or DEFAULT_BUSINESS_NAME
    amount_float = float(amount or 0)
    inv_label = invoice_number or f"#{invoice_id_db}"

    token = public_token_db or get_or_create_public_token(invoice_id_db)
    base_url = APP_BASE_URL or request.url_root.rstrip("/")
    public_url = f"{base_url}/public/{token}"

    payment_summary = get_invoice_payment_summary(invoice_id_db) or {}
    balance = float(payment_summary.get("balance") or 0)
    total_paid = float(payment_summary.get("total_paid") or 0)

    default_to_email = last_emailed_to or client_email or ""

    if email_type == "reminder":
        subject = f"Friendly reminder: Invoice {inv_label} from {business_name}"
        if payment_summary.get("is_overdue"):
            subject = f"Payment reminder: Invoice {inv_label} is overdue"
        if total_paid > 0 and balance > 0:
            subject = f"Remaining balance reminder: Invoice {inv_label}"

        message = (
            f"Hi {client_name},\n\n"
            f"This is a friendly reminder regarding invoice {inv_label} from {business_name}.\n"
            f"Original invoice total: {format_currency(amount_float)}\n"
            f"Total paid so far: {format_currency(total_paid)}\n"
            f"Remaining balance: {format_currency(balance)}\n"
            + (f"Due date: {due_date.strftime('%Y-%m-%d')}\n" if due_date else "")
            + f"\nYou can view and pay the invoice here:\n{public_url}\n\n"
            + "Please let us know if you have any questions.\n\n"
            + f"— {business_name}"
        )
    elif email_type == "partial_payment_confirmation":
        subject = f"Payment received for invoice {inv_label}"
        message = (
            f"Hi {client_name},\n\n"
            f"We received your payment for invoice {inv_label}.\n"
            f"Invoice total: {format_currency(amount_float)}\n"
            f"Total paid so far: {format_currency(total_paid)}\n"
            f"Remaining balance: {format_currency(balance)}\n"
            f"\nYou can view the invoice anytime here:\n{public_url}\n\n"
            f"Thank you.\n\n— {business_name}"
        )
    elif email_type == "paid_in_full_confirmation":
        subject = f"Invoice {inv_label} paid in full"
        message = (
            f"Hi {client_name},\n\n"
            f"Thank you. Invoice {inv_label} has been paid in full.\n"
            f"Amount received: {format_currency(total_paid or amount_float)}\n"
            f"\nYou can view the invoice here:\n{public_url}\n\n"
            f"We appreciate your business.\n\n— {business_name}"
        )
    else:
        subject = f"Invoice {inv_label} from {business_name}"
        message = (
            f"Hi {client_name},\n\n"
            f"Please find attached your invoice {inv_label} for {format_currency(amount_float)} from {business_name}.\n"
            + (f"Due date: {due_date.strftime('%Y-%m-%d')}\n" if due_date else "")
            + f"\nYou can also view this invoice online here:\n{public_url}\n\n"
            + "Thank you for your business.\n\n"
            + f"— {business_name}"
        )

    return {
        "invoice_id": invoice_id_db,
        "client_name": client_name,
        "amount": amount_float,
        "created_at": created_at,
        "status": status,
        "invoice_number": invoice_number,
        "inv_label": inv_label,
        "due_date": due_date,
        "last_emailed_at": last_emailed_at,
        "last_emailed_to": last_emailed_to,
        "last_reminder_sent_at": last_reminder_sent_at,
        "client_email": client_email,
        "public_url": public_url,
        "public_token": token,
        "default_to_email": default_to_email,
        "default_subject": subject,
        "default_message": message,
        "payment_summary": payment_summary,
    }


def send_invoice_notification_email(invoice_id: int, to_email: str, email_type: str):
    defaults = build_invoice_email_defaults(invoice_id, email_type=email_type)
    if not defaults or not to_email:
        return False, "Missing invoice email defaults or recipient."

    return send_invoice_email(
        invoice_id=invoice_id,
        to_email=to_email,
        subject=defaults["default_subject"],
        body_text=defaults["default_message"],
        email_type=email_type,
    )


@app.route("/send-email/<int:invoice_id>", methods=["GET", "POST"])
@login_required
def send_email_view(invoice_id):
    if not can_email_invoices(get_current_user()):
        return render_template(
            "upgrade_gate.html",
            title="Upgrade to email invoices",
            reason="Email delivery with PDF attachments is available on the Simple plan and above.",
            required_plan="simple",
            plans=PLAN_DEFINITIONS,
        )

    profile = get_business_profile_safe()
    business_name = profile.get("business_name") or DEFAULT_BUSINESS_NAME

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
    cursor.close()
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

    base_url = APP_BASE_URL or request.url_root.rstrip("/")
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
            success, err = send_invoice_email(invoice_id_db, to_email, subject, message_body)
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


@app.route("/send-reminder/<int:invoice_id>", methods=["GET", "POST"])
@login_required
def send_reminder_view(invoice_id):
    if not can_use_collections(get_current_user()):
        return render_template(
            "upgrade_gate.html",
            title="Upgrade to send reminders",
            reason="Professional reminder workflows are available on the Pro plan and above.",
            required_plan="pro",
            plans=PLAN_DEFINITIONS,
        )

    current_user = get_current_user()
    user_id = current_user["id"]

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM invoices WHERE id = %s AND user_id = %s",
        (invoice_id, user_id),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        return "Invoice not found", 404

    defaults = build_invoice_email_defaults(invoice_id, email_type="reminder")
    if not defaults:
        return "Invoice not found", 404

    feedback_message = None
    feedback_type = None

    if request.method == "POST":
        to_email = (request.form.get("to_email") or "").strip()
        subject = request.form.get("subject") or defaults["default_subject"]
        message_body = request.form.get("message") or defaults["default_message"]

        if not to_email:
            feedback_message = "Recipient email is required."
            feedback_type = "error"
        else:
            success, err = send_invoice_email(
                invoice_id,
                to_email,
                subject,
                message_body,
                email_type="reminder",
            )
            if success:
                feedback_message = f"Reminder sent for invoice {defaults['inv_label']}."
                feedback_type = "success"
                defaults["default_to_email"] = to_email
            else:
                feedback_message = err or "Failed to send reminder."
                feedback_type = "error"

    return render_template(
        "send_email.html",
        invoice_id=defaults["invoice_id"],
        client_name=defaults["client_name"],
        amount=defaults["amount"],
        created_at=defaults["created_at"],
        status=defaults["status"],
        invoice_number=defaults["invoice_number"],
        inv_label=defaults["inv_label"],
        due_date=defaults["due_date"],
        last_emailed_at=defaults["last_emailed_at"],
        last_emailed_to=defaults["last_emailed_to"],
        default_to_email=defaults["default_to_email"],
        default_subject=defaults["default_subject"],
        default_message=defaults["default_message"],
        feedback_message=feedback_message,
        feedback_type=feedback_type,
        public_url=defaults["public_url"],
        email_mode="reminder",
        payment_summary=defaults["payment_summary"],
        lang=request.args.get("lang", "en"),
    )


# -------------------------
# AI HELPERS
# -------------------------
def get_ai_kpi_summary_for_user(user_id: int) -> str:
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

    now = now_local()
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

    growth_pct = round((monthly_revenue / total_revenue) * 100, 1) if total_revenue > 0 else 0.0

    dashboard_metrics = get_dashboard_receivables_metrics(user_id)

    return (
        f"Total invoices: {total_invoices}, "
        f"total revenue: ${total_revenue:,.2f}, "
        f"this month revenue: ${monthly_revenue:,.2f} "
        f"({growth_pct}% of all-time), "
        f"status counts → Paid: {paid_count}, Sent: {sent_count}, Overdue: {overdue_count}. "
        f"Outstanding receivables: ${dashboard_metrics['outstanding_receivables']:,.2f}, "
        f"overdue receivables: ${dashboard_metrics['overdue_receivables']:,.2f}, "
        f"viewed but unpaid: {dashboard_metrics['viewed_but_unpaid_count']}, "
        f"sent not viewed: {dashboard_metrics['sent_not_viewed_count']}, "
        f"suggested tax reserve this month: ${dashboard_metrics['suggested_tax_reserve']:,.2f}."
    )


@app.route("/ai-helper", methods=["POST"])
@login_required
def ai_helper():
    if not ai_client or not OPENAI_API_KEY:
        return {"error": "AI helper is not configured on the server."}, 500

    if not can_use_ai(get_current_user()):
        return {"error": "AI helper is available on the Pro plan and above."}, 403

    data = request.get_json() or {}
    question = (data.get("question") or "").strip()
    page = (data.get("page") or "").strip()

    user_lang = normalize_lang(data.get("lang") or request.args.get("lang") or "en")

    if not question:
        return {"error": "Missing question."}, 400

    user = get_current_user()
    user_plan = user.get("plan") or "free"
    user_id = user.get("id")
    is_pro = PLAN_LEVELS.get(user_plan, 0) >= PLAN_LEVELS.get("pro", 0)

    kpi_summary = ""
    if user_id:
        try:
            kpi_summary = get_ai_kpi_summary_for_user(user_id)
        except Exception:
            kpi_summary = ""

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

    model_name = AI_MODEL_PRO if is_pro else AI_MODEL_FREE
    max_output_tokens = 600 if is_pro else 220

    try:
        resp = ai_client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": app_context},
                {"role": "user", "content": question},
            ],
            temperature=0.4,
            max_tokens=max_output_tokens,
        )
        answer = (resp.choices[0].message.content or "").strip()
        return {"answer": answer}
    except Exception as e:
        return {"error": f"AI error: {e}"}, 500


@app.route("/api/ai-assistant", methods=["POST"])
@login_required
def api_ai_assistant():
    data = request.get_json() or {}
    user_message = (data.get("message") or "").strip()
    lang = normalize_lang(data.get("lang") or request.args.get("lang", "en"))
    page = data.get("page") or ""
    extra = data.get("extra_context") or {}

    if not user_message:
        return jsonify({"error": "No message provided."}), 400

    if lang == "es":
        system_prompt = (
            "Eres BillBeam Assistant, un asistente amable y claro dentro de BillBeam, "
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
            "You are BillBeam Assistant, a warm, practical AI living inside BillBeam, "
            "an invoicing app for freelancers and small businesses. "
            "Your job is to help users:\n"
            "- Understand their invoice dashboard and metrics (revenue, statuses, top clients).\n"
            "- Draft invoice notes, payment reminders, and follow-up emails.\n"
            "- Decide how to structure line items (services, hours, products) and payment terms.\n"
            "- Suggest best practices in a short, calm, encouraging tone.\n"
            "Keep answers concise (a few short paragraphs max). "
            "Do not fabricate specific user numbers; if you'd need live data, say you can't see it directly "
            "and point them to where in BillBeam they can check. "
            "Never make up details about specific clients or actual payments."
        )

    context_hint_parts = []
    if page:
        context_hint_parts.append(f"User is currently on the page: {page}.")
    if extra:
        context_hint_parts.append(f"Extra context: {extra}")

    context_hint = "\n".join(context_hint_parts)
    user_content = user_message
    if context_hint:
        user_content = context_hint + "\n\nUser question:\n" + user_message

    if not client:
        return jsonify({"error": "AI client is not configured."}), 500

    try:
        completion = client.chat.completions.create(
            model=AI_MODEL_FREE,
            temperature=0.4,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
        )
        reply = completion.choices[0].message.content
        return jsonify({"reply": reply})
    except Exception:
        logger.exception("AI error in /api/ai-assistant")
        msg = (
            "Lo siento, el asistente tuvo un problema. Intenta de nuevo en un momento."
            if lang == "es"
            else "Sorry, the assistant ran into a problem. Try again in a moment."
        )
        return jsonify({"error": msg}), 500


# -------------------------
# STRIPE WEBHOOK HELPERS
# -------------------------
def _record_invoice_payment_from_checkout_session(session_obj):
    metadata = session_obj.get("metadata") or {}
    invoice_id_str = metadata.get("invoice_id")
    token = metadata.get("token") or metadata.get("public_token")

    checkout_session_id = session_obj.get("id")
    payment_intent_id = session_obj.get("payment_intent")

    amount_total = session_obj.get("amount_total") or 0
    amount_paid = float(amount_total) / 100.0

    logger.info(
        "[Stripe] invoice payment checkout.session.completed invoice_id=%s token=%s cs=%s pi=%s amount_paid=%s",
        invoice_id_str,
        token,
        checkout_session_id,
        payment_intent_id,
        amount_paid,
    )

    if not invoice_id_str:
        logger.warning("[Stripe] Missing invoice_id in metadata for invoice payment")
        return

    try:
        invoice_id = int(invoice_id_str)
    except ValueError:
        logger.warning("[Stripe] Bad invoice_id in metadata: %s", invoice_id_str)
        return

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        if checkout_session_id:
            cur.execute(
                "SELECT id FROM payments WHERE stripe_checkout_session_id = %s LIMIT 1",
                (checkout_session_id,),
            )
            existing = cur.fetchone()
            if existing:
                logger.info("[Stripe] Payment already recorded for checkout session %s", checkout_session_id)
                conn.commit()
                return

        if payment_intent_id:
            cur.execute(
                "SELECT id FROM payments WHERE stripe_payment_intent_id = %s LIMIT 1",
                (payment_intent_id,),
            )
            existing = cur.fetchone()
            if existing:
                logger.info("[Stripe] Payment already recorded for payment intent %s", payment_intent_id)
                conn.commit()
                return

        cur.execute(
            """
            INSERT INTO payments (
                invoice_id,
                amount,
                method,
                note,
                stripe_payment_intent_id,
                stripe_checkout_session_id,
                payment_source,
                payment_status,
                occurred_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                invoice_id,
                amount_paid,
                "Stripe",
                "Stripe Checkout",
                payment_intent_id,
                checkout_session_id,
                "stripe",
                "succeeded",
                now_local(),
            ),
        )

        if payment_intent_id:
            cur.execute(
                "UPDATE invoices SET stripe_last_payment_intent_id = %s WHERE id = %s",
                (payment_intent_id, invoice_id),
            )

        cur.execute("SELECT amount, invoice_number FROM invoices WHERE id = %s", (invoice_id,))
        inv = cur.fetchone()

        if inv:
            inv_total = float(inv[0] or 0)
            invoice_number = inv[1] or f"#{invoice_id}"

            cur.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE invoice_id = %s",
                (invoice_id,),
            )
            total_paid = float(cur.fetchone()[0] or 0)

            cur.execute(
                """
                UPDATE invoices
                SET last_payment_recorded_at = %s,
                    last_collection_action_at = %s
                WHERE id = %s
                """,
                (now_local(), now_local(), invoice_id),
            )

            conn.commit()
            summary = sync_invoice_status(invoice_id) or get_invoice_payment_summary(invoice_id) or {}
            balance = float(summary.get("balance") or 0)
            total_paid_now = float(summary.get("total_paid") or 0)

            log_invoice_event(
                invoice_id=invoice_id,
                event_type="stripe_payment",
                title="Online payment received",
                details=f"Stripe payment received for invoice {invoice_number}: {format_currency(amount_paid)}.",
                visibility="both",
            )

            if balance > 0.0001:
                log_invoice_event(
                    invoice_id=invoice_id,
                    event_type="partial_payment_received",
                    title="Partial payment received",
                    details=f"Total paid is now {format_currency(total_paid_now)}. Remaining balance: {format_currency(balance)}.",
                    visibility="both",
                )
            else:
                log_invoice_event(
                    invoice_id=invoice_id,
                    event_type="final_payment_received",
                    title="Final payment received",
                    details=f"Invoice {invoice_number} is now paid in full.",
                    visibility="both",
                )

            cur.execute(
                """
                SELECT c.email
                FROM invoices i
                LEFT JOIN clients c ON i.client_id = c.id
                WHERE i.id = %s
                """,
                (invoice_id,),
            )
            email_row = cur.fetchone()
            client_email = email_row[0] if email_row else None

            if client_email:
                if balance > 0.0001:
                    send_invoice_notification_email(invoice_id, client_email, "partial_payment_confirmation")
                else:
                    send_invoice_notification_email(invoice_id, client_email, "paid_in_full_confirmation")
        else:
            conn.commit()

    except Exception:
        conn.rollback()
        logger.exception("[Stripe] Error recording invoice payment")
    finally:
        cur.close()
        conn.close()


def _handle_subscription_checkout_completed(session_obj):
    metadata = session_obj.get("metadata") or {}
    user_id_str = metadata.get("user_id") or session_obj.get("client_reference_id")
    plan_key = normalize_plan_key(metadata.get("plan_key") or "pro")

    stripe_customer_id = session_obj.get("customer")
    stripe_subscription_id = session_obj.get("subscription")

    logger.info(
        "[Stripe] subscription checkout.session.completed user_id_str=%s customer=%s sub=%s plan_key=%s",
        user_id_str,
        stripe_customer_id,
        stripe_subscription_id,
        plan_key,
    )

    if not user_id_str:
        logger.warning("[Stripe] Missing user_id for subscription checkout")
        return

    try:
        user_id = int(user_id_str)
    except ValueError:
        logger.warning("[Stripe] Bad user_id in metadata: %s", user_id_str)
        return

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            UPDATE users
            SET plan = %s,
                stripe_customer_id = COALESCE(%s, stripe_customer_id),
                stripe_subscription_id = COALESCE(%s, stripe_subscription_id)
            WHERE id = %s
            """,
            (plan_key, stripe_customer_id, stripe_subscription_id, user_id),
        )
        conn.commit()
        logger.info("[Stripe] Upgraded user %s to %s (rows_updated=%s)", user_id, plan_key, cur.rowcount)
    except Exception:
        conn.rollback()
        logger.exception("[Stripe] DB error upgrading user %s", user_id)
    finally:
        cur.close()
        conn.close()


@app.route("/stripe/webhook", methods=["POST"])
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get("Stripe-Signature")

    if not STRIPE_WEBHOOK_SECRET:
        logger.warning("[Stripe] Webhook called but STRIPE_WEBHOOK_SECRET is not set")
        return "Webhook secret not configured", 500

    try:
        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=STRIPE_WEBHOOK_SECRET,
        )
    except ValueError:
        logger.warning("[Stripe] Invalid payload")
        return "Invalid payload", 400
    except stripe.error.SignatureVerificationError:
        logger.warning("[Stripe] Invalid signature")
        return "Invalid signature", 400

    event_type = event.get("type")
    logger.info("[Stripe] Received event: %s", event_type)

    if event_type == "checkout.session.completed":
        session_obj = event["data"]["object"]
        mode = (session_obj.get("mode") or "").lower()

        if mode == "payment":
            _record_invoice_payment_from_checkout_session(session_obj)
            return "OK", 200

        if mode == "subscription":
            _handle_subscription_checkout_completed(session_obj)
            return "OK", 200

        logger.info("[Stripe] checkout.session.completed with unsupported mode=%s", mode)
        return "OK", 200

    if event_type in ("customer.subscription.updated", "customer.subscription.deleted"):
        sub = event["data"]["object"]
        customer_id = sub.get("customer")
        status = (sub.get("status") or "").lower()
        sub_metadata = sub.get("metadata") or {}
        paid_plan = normalize_plan_key(sub_metadata.get("plan_key") or "pro")
        new_plan = paid_plan if status in ("active", "trialing") else "free"

        logger.info(
            "[Stripe] Subscription sync customer=%s sub=%s status=%s => plan=%s",
            customer_id,
            sub.get("id"),
            status,
            new_plan,
        )

        if not customer_id:
            logger.warning("[Stripe] Missing customer_id on subscription event")
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

            logger.info("[Stripe] Subscription sync rows_updated=%s for customer_id=%s", updated, customer_id)
        except Exception:
            logger.exception("[Stripe] subscription sync error")

    return "OK", 200


# -------------------------
# HEALTHCHECK
# -------------------------
@app.route("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "app": APP_NAME,
            "timezone": str(APP_TIMEZONE),
            "stripe_configured": bool(STRIPE_SECRET_KEY),
            "ai_configured": bool(OPENAI_API_KEY),
        }
    ), 200


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, "static"),
        "favicon.ico",
        mimetype="image/vnd.microsoft.icon",
    )


@app.route("/ios/activate-subscription", methods=["POST"])
@login_required
def ios_activate_subscription():
    user = get_current_user()
    user_id = user.get("id")

    if not user_id:
        return jsonify({"error": "Not authenticated."}), 401

    data = request.get_json(silent=True) or {}

    product_id = (
        data.get("product_id")
        or data.get("productId")
        or ""
    ).strip()

    transaction_id = (
        data.get("transaction_id")
        or data.get("transactionId")
        or ""
    ).strip()

    original_transaction_id = (
        data.get("original_transaction_id")
        or data.get("originalTransactionId")
        or ""
    ).strip()

    if not product_id:
        return jsonify({"error": "Missing productId."}), 400

    new_plan = get_plan_for_apple_product_id(product_id)
    if not new_plan:
        return jsonify({"error": "Unknown productId."}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE users
            SET plan = %s,
                apple_product_id = %s,
                apple_transaction_id = %s,
                apple_original_transaction_id = %s,
                apple_last_purchase_at = %s
            WHERE id = %s
            """,
            (
                new_plan,
                product_id,
                transaction_id or None,
                original_transaction_id or None,
                now_local(),
                user_id,
            ),
        )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(
            "[iOS Activate Subscription] user_id=%s upgraded to %s via product_id=%s transaction_id=%s original_transaction_id=%s",
            user_id,
            new_plan,
            product_id,
            transaction_id,
            original_transaction_id,
        )

        session["user_id"] = user_id
        session.permanent = True
        session.modified = True

        return jsonify(
            {
                "success": True,
                "plan": new_plan,
                "productId": product_id,
            }
        ), 200

    except Exception as e:
        logger.exception("[iOS Activate Subscription] error for user_id=%s", user_id)
        return jsonify({"error": f"Server error: {e}"}), 500


@app.route("/api/account-plan", methods=["GET"])
@login_required
def api_account_plan():
    user = get_current_user()
    return jsonify(
        {
            "authenticated": bool(user.get("id")),
            "plan": normalize_plan_key(user.get("plan") or "free"),
            "email": user.get("email") or "",
        }
    ), 200


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)