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
import time
import uuid

import httpx
import jwt

import psycopg2
import stripe
from openai import OpenAI
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

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

@app.context_processor
def inject_unread_message_count():
    try:
        user = get_current_user()
        if not user:
            return {"unread_message_count": 0}

        user_id = user["id"]

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM messages m
            JOIN conversations c ON m.conversation_id = c.id
            WHERE (c.business_user_id = %s OR c.client_user_id = %s)
              AND m.sender_user_id != %s
              AND COALESCE(m.is_read, FALSE) = FALSE
            """,
            (user_id, user_id, user_id),
        )

        count = cur.fetchone()[0] or 0

        cur.close()
        conn.close()

        return {"unread_message_count": count}

    except Exception as e:
        logger.warning("Unread message count failed: %s", e)
        return {"unread_message_count": 0}

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

APPLE_IAP_SIMPLE_PRODUCT_ID = os.environ.get("APPLE_IAP_SIMPLE_PRODUCT_ID", "app.billbeam.simple.v2.monthly")
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
APNS_KEY_ID = os.environ.get("APNS_KEY_ID")
APNS_TEAM_ID = os.environ.get("APNS_TEAM_ID")
APNS_BUNDLE_ID = os.environ.get("APNS_BUNDLE_ID", "com.billbeam.app")
APNS_AUTH_KEY = os.environ.get("APNS_AUTH_KEY")
APNS_USE_SANDBOX = os.environ.get("APNS_USE_SANDBOX", "false").lower() in ("1", "true", "yes", "on")

_APNS_JWT_CACHE = {
    "token": None,
    "created_at": 0,
}

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

# Render persistent disk mount path
PERSISTENT_STORAGE_ROOT = os.environ.get("PERSISTENT_STORAGE_ROOT", "/var/data").rstrip("/")
LOGO_UPLOAD_ROOT = os.path.join(PERSISTENT_STORAGE_ROOT, "uploads", "logos")
SERVICE_IMAGE_UPLOAD_ROOT = os.path.join(PERSISTENT_STORAGE_ROOT, "uploads", "services")

os.makedirs(LOGO_UPLOAD_ROOT, exist_ok=True)
os.makedirs(SERVICE_IMAGE_UPLOAD_ROOT, exist_ok=True)

app.config["UPLOAD_FOLDER"] = LOGO_UPLOAD_ROOT
app.config["MAX_LOGO_UPLOAD_BYTES"] = int(os.environ.get("MAX_LOGO_UPLOAD_BYTES", str(2 * 1024 * 1024)))
app.config["MAX_SERVICE_IMAGE_UPLOAD_BYTES"] = int(
    os.environ.get("MAX_SERVICE_IMAGE_UPLOAD_BYTES", str(4 * 1024 * 1024))
)

ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp"}
ALLOWED_IMAGE_MIMETYPES = {"image/png", "image/jpeg", "image/webp"}

INVOICE_IMAGE_UPLOAD_ROOT = os.path.join(PERSISTENT_STORAGE_ROOT, "uploads", "invoice_images")
SERVICE_REQUEST_PHOTO_UPLOAD_ROOT = os.path.join(PERSISTENT_STORAGE_ROOT, "uploads", "service_request_photos")
os.makedirs(SERVICE_REQUEST_PHOTO_UPLOAD_ROOT, exist_ok=True)
os.makedirs(INVOICE_IMAGE_UPLOAD_ROOT, exist_ok=True)

app.config["MAX_INVOICE_IMAGE_UPLOAD_BYTES"] = int(
    os.environ.get("MAX_INVOICE_IMAGE_UPLOAD_BYTES", str(5 * 1024 * 1024))
)


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


def allowed_logo_file(filename: str) -> bool:
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in ALLOWED_IMAGE_EXTENSIONS


def build_logo_public_url(filename: str) -> str:
    base_url = (APP_BASE_URL or request.host_url.rstrip("/")).rstrip("/")
    return f"{base_url}/uploads/logos/{filename}"


def build_service_image_public_url(filename: str) -> str:
    base_url = (APP_BASE_URL or request.host_url.rstrip("/")).rstrip("/")
    return f"{base_url}/uploads/services/{filename}"


def save_uploaded_logo(file_storage, user_id: int):
    if not file_storage or not getattr(file_storage, "filename", ""):
        return None, "No logo file was selected."

    original_name = (file_storage.filename or "").strip()
    if not allowed_logo_file(original_name):
        return None, "Logo must be a PNG, JPG, JPEG, or WEBP image."

    mimetype = (file_storage.mimetype or "").lower().strip()
    if mimetype not in ALLOWED_IMAGE_MIMETYPES:
        return None, "Unsupported logo file type."

    safe_name = secure_filename(original_name)
    ext = safe_name.rsplit(".", 1)[1].lower()

    file_storage.stream.seek(0, os.SEEK_END)
    file_size = file_storage.stream.tell()
    file_storage.stream.seek(0)

    if file_size <= 0:
        return None, "Uploaded logo file is empty."

    if file_size > app.config["MAX_LOGO_UPLOAD_BYTES"]:
        return None, "Logo file is too large. Please upload an image under 2 MB."

    filename = f"user_{user_id}_logo.{ext}"
    destination_path = os.path.join(LOGO_UPLOAD_ROOT, filename)

    try:
        file_storage.save(destination_path)
    except Exception as e:
        logger.exception("Failed saving uploaded logo for user_id=%s: %s", user_id, e)
        return None, "Could not save the uploaded logo."

    public_url = build_logo_public_url(filename)
    return public_url, None


def save_uploaded_service_image(file_storage, user_id: int, service_id: int | None = None):
    if not file_storage or not getattr(file_storage, "filename", ""):
        return None, "No service image file was selected."

    original_name = (file_storage.filename or "").strip()
    if not allowed_logo_file(original_name):
        return None, "Service image must be a PNG, JPG, JPEG, or WEBP image."

    mimetype = (file_storage.mimetype or "").lower().strip()
    if mimetype not in ALLOWED_IMAGE_MIMETYPES:
        return None, "Unsupported service image file type."

    safe_name = secure_filename(original_name)
    ext = safe_name.rsplit(".", 1)[1].lower()

    file_storage.stream.seek(0, os.SEEK_END)
    file_size = file_storage.stream.tell()
    file_storage.stream.seek(0)

    if file_size <= 0:
        return None, "Uploaded service image file is empty."

    if file_size > app.config["MAX_SERVICE_IMAGE_UPLOAD_BYTES"]:
        return None, "Service image file is too large. Please upload an image under 4 MB."

    unique_suffix = secrets.token_hex(6)
    if service_id:
        filename = f"user_{user_id}_service_{service_id}_{unique_suffix}.{ext}"
    else:
        filename = f"user_{user_id}_service_{unique_suffix}.{ext}"

    destination_path = os.path.join(SERVICE_IMAGE_UPLOAD_ROOT, filename)

    try:
        file_storage.save(destination_path)
    except Exception as e:
        logger.exception(
            "Failed saving uploaded service image for user_id=%s service_id=%s: %s",
            user_id,
            service_id,
            e,
        )
        return None, "Could not save the uploaded service image."

    public_url = build_service_image_public_url(filename)
    return public_url, None


def build_invoice_image_public_url(filename: str) -> str:
    base_url = (APP_BASE_URL or request.host_url.rstrip("/")).rstrip("/")
    return f"{base_url}/uploads/invoice_images/{filename}"


def save_uploaded_invoice_image(file_storage, user_id: int, invoice_id: int):
    if not file_storage or not getattr(file_storage, "filename", ""):
        return None, "No invoice image file was selected."

    original_name = (file_storage.filename or "").strip()
    if not allowed_logo_file(original_name):
        return None, "Invoice image must be a PNG, JPG, JPEG, or WEBP image."

    mimetype = (file_storage.mimetype or "").lower().strip()
    if mimetype not in ALLOWED_IMAGE_MIMETYPES:
        return None, "Unsupported invoice image file type."

    safe_name = secure_filename(original_name)
    ext = safe_name.rsplit(".", 1)[1].lower()

    file_storage.stream.seek(0, os.SEEK_END)
    file_size = file_storage.stream.tell()
    file_storage.stream.seek(0)

    if file_size <= 0:
        return None, "Uploaded invoice image file is empty."

    if file_size > app.config["MAX_INVOICE_IMAGE_UPLOAD_BYTES"]:
        return None, "Invoice image file is too large. Please upload an image under 5 MB."

    unique_suffix = secrets.token_hex(6)
    filename = f"user_{user_id}_invoice_{invoice_id}_{unique_suffix}.{ext}"
    destination_path = os.path.join(INVOICE_IMAGE_UPLOAD_ROOT, filename)

    try:
        file_storage.save(destination_path)
    except Exception as e:
        logger.exception(
            "Failed saving uploaded invoice image for user_id=%s invoice_id=%s: %s",
            user_id,
            invoice_id,
            e,
        )
        return None, "Could not save the uploaded invoice image."

    public_url = build_invoice_image_public_url(filename)
    return public_url, None


def get_invoice_image_urls(invoice_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT image_url
        FROM invoice_images
        WHERE invoice_id = %s
        ORDER BY created_at ASC, id ASC
        """,
        (invoice_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows if row and row[0]]


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
            image_url TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        "ALTER TABLE services ADD COLUMN IF NOT EXISTS pricing_type TEXT DEFAULT 'fixed';"
    )
    cursor.execute(
        "ALTER TABLE services ADD COLUMN IF NOT EXISTS duration_minutes INTEGER;"
    )
    cursor.execute(
        "ALTER TABLE services ADD COLUMN IF NOT EXISTS category TEXT;"
    )
    cursor.execute(
        "ALTER TABLE services ADD COLUMN IF NOT EXISTS location_required BOOLEAN DEFAULT FALSE;"
    )
    cursor.execute(
        "ALTER TABLE services ADD COLUMN IF NOT EXISTS materials_included TEXT;"
    )
    cursor.execute(
        "ALTER TABLE services ADD COLUMN IF NOT EXISTS photo_required BOOLEAN DEFAULT FALSE;"
    )
    cursor.execute(
        "ALTER TABLE services ADD COLUMN IF NOT EXISTS availability_notes TEXT;"
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
        CREATE TABLE IF NOT EXISTS notification_preferences (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL UNIQUE,
            notifications_enabled BOOLEAN DEFAULT TRUE,
            in_app_enabled BOOLEAN DEFAULT TRUE,
            push_enabled BOOLEAN DEFAULT FALSE,
            email_enabled BOOLEAN DEFAULT TRUE,
            business_request_alerts BOOLEAN DEFAULT TRUE,
            client_request_updates BOOLEAN DEFAULT TRUE,
            invoice_alerts BOOLEAN DEFAULT TRUE,
            payment_alerts BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS notification_preferences_user_idx
        ON notification_preferences(user_id);
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS business_followers (
            id SERIAL PRIMARY KEY,
            client_user_id INTEGER NOT NULL,
            business_user_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(client_user_id, business_user_id)
        );
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS business_followers_client_idx
        ON business_followers(client_user_id);
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS business_followers_business_idx
        ON business_followers(business_user_id);
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

    cursor.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS image_url TEXT;")

    cursor.execute("ALTER TABLE service_requests ADD COLUMN IF NOT EXISTS client_user_id INTEGER;")

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS service_requests_client_user_idx
        ON service_requests(client_user_id);
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_images (
            id SERIAL PRIMARY KEY,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            image_url TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS invoice_images_invoice_idx
        ON invoice_images(invoice_id, created_at ASC);
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

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            sender_user_id INTEGER NOT NULL,
            message_text TEXT,
            attachment_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS messages_conversation_idx
        ON messages(conversation_id, created_at DESC);
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS conversations (
            id SERIAL PRIMARY KEY,
            business_user_id INTEGER NOT NULL,
            client_user_id INTEGER NOT NULL,
            service_request_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS conversation_participants (
            id SERIAL PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS conversations_business_idx
        ON conversations(business_user_id);
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS conversations_client_idx
        ON conversations(client_user_id);
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS messages_conversation_idx
        ON messages(conversation_id, created_at DESC);
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS conversation_participants_conv_idx
        ON conversation_participants(conversation_id);
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


# -------------------------
# MESSAGING HELPERS
# -------------------------

def get_or_create_conversation(business_user_id, client_user_id, service_request_id=None):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # -------------------------
        # 1. TRY TO FIND EXISTING
        # -------------------------
        cur.execute(
            """
            SELECT id
            FROM conversations
            WHERE business_user_id = %s
              AND client_user_id = %s
              AND (
                    service_request_id = %s
                    OR (service_request_id IS NULL AND %s IS NULL)
                  )
            LIMIT 1
            """,
            (business_user_id, client_user_id, service_request_id, service_request_id),
        )

        row = cur.fetchone()

        if row:
            return row[0]

        # -------------------------
        # 2. CREATE NEW IF NOT FOUND
        # -------------------------
        cur.execute(
            """
            INSERT INTO conversations (
                business_user_id,
                client_user_id,
                service_request_id,
                created_at
            )
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (
                business_user_id,
                client_user_id,
                service_request_id,
                now_local(),
            ),
        )

        new_id = cur.fetchone()[0]
        conn.commit()

        return new_id

    except Exception as e:
        conn.rollback()
        logger.exception("get_or_create_conversation failed: %s", e)
        return None

    finally:
        cur.close()
        conn.close()


def send_message(conversation_id: int, sender_user_id: int, message_text: str):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # -------------------------
        # GET CONVERSATION PARTICIPANTS
        # -------------------------
        cur.execute(
            """
            SELECT business_user_id, client_user_id
            FROM conversations
            WHERE id = %s
            """,
            (conversation_id,),
        )
        convo = cur.fetchone()

        if not convo:
            logger.warning("send_message: conversation not found")
            return False

        business_user_id, client_user_id = convo

        # -------------------------
        # USE NEW PIPELINE
        # -------------------------
        result = send_message_in_conversation(
            business_user_id=business_user_id,
            client_user_id=client_user_id,
            sender_user_id=sender_user_id,
            message_text=message_text,
        )

        if not result:
            return False

        # -------------------------
        # KEEP CONVERSATION FRESH (ORDERING FIX)
        # -------------------------
        try:
            cur.execute(
                """
                UPDATE conversations
                SET created_at = %s
                WHERE id = %s
                """,
                (now_local(), conversation_id),
            )
            conn.commit()
        except Exception as e:
            logger.warning("Conversation timestamp update failed: %s", e)

        # -------------------------
        # TRIGGER PUSH NOTIFICATION
        # -------------------------
        try:
            recipient_id = (
                client_user_id
                if sender_user_id == business_user_id
                else business_user_id
            )

            if recipient_id and recipient_id != sender_user_id:
                create_notification_if_enabled(
                    user_id=recipient_id,
                    category="messages",
                    notification_type="new_message",
                    title="New message",
                    body=(message_text or "")[:100],
                    link_url=f"/messages?open={conversation_id}",
                )

        except Exception as e:
            logger.exception("Notification failed: %s", e)

        return True

    except Exception as e:
        conn.rollback()
        logger.exception("Send message failed: %s", e)
        return False

    finally:
        cur.close()
        conn.close()


def get_conversation_messages(conversation_id: int):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT sender_user_id, message_text, created_at
        FROM messages
        WHERE conversation_id = %s
        ORDER BY created_at ASC
        """,
        (conversation_id,),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    messages = []
    for sender_user_id, message_text, created_at in rows:
        messages.append({
            "sender_user_id": sender_user_id,
            "message_text": message_text,
            "created_at": created_at,
        })

    return messages


def get_unread_message_count(user_id: int) -> int:
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM messages m
        JOIN conversations c ON m.conversation_id = c.id
        WHERE
            (
                (c.business_user_id = %s AND m.sender_user_id = c.client_user_id)
                OR
                (c.client_user_id = %s AND m.sender_user_id = c.business_user_id)
            )
            AND COALESCE(m.is_read, FALSE) = FALSE
        """,
        (user_id, user_id, user_id),
    )

    count = cur.fetchone()[0] or 0

    cur.close()
    conn.close()

    return int(count)


# =========================
# CLIENT ACCOUNT SYSTEM
# =========================

@app.route("/client/register", methods=["GET", "POST"])
def client_register():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()

        if not email or not password:
            return render_template("client_register.html", error="Email and password required")

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        if cur.fetchone():
            cur.close()
            conn.close()
            return render_template("client_register.html", error="Account already exists")

        password_hash = generate_password_hash(password)

        cur.execute(
            """
            INSERT INTO users (email, password_hash, plan, is_active)
            VALUES (%s, %s, 'free', TRUE)
            RETURNING id
            """,
            (email, password_hash),
        )
        user_id = cur.fetchone()[0]

        conn.commit()
        cur.close()
        conn.close()

        session["user_id"] = user_id
        return redirect(url_for("client_dashboard"))

    return render_template("client_register.html")


@app.route("/client/login", methods=["GET", "POST"])
def client_login():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, password_hash FROM users WHERE email = %s",
            (email,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row or not check_password_hash(row[1], password):
            return render_template("client_login.html", error="Invalid login")

        session["user_id"] = row[0]
        return redirect(url_for("client_dashboard"))

    return render_template("client_login.html")


@app.route("/client/dashboard")
@login_required
def client_dashboard():
    user = get_current_user()
    client_user_id = user["id"]
    lang = get_request_lang()

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # -------------------------
        # FOLLOWED BUSINESSES
        # -------------------------
        cur.execute(
            """
            SELECT
                bf.business_user_id,
                COALESCE(bp.business_name, u.email, 'Business') AS business_name,
                bp.logo_url,
                bp.email,
                bp.phone,
                bp.website,
                bp.address,
                bf.created_at
            FROM business_followers bf
            LEFT JOIN business_profile bp
                ON bp.user_id = bf.business_user_id
            LEFT JOIN users u
                ON u.id = bf.business_user_id
            WHERE bf.client_user_id = %s
            ORDER BY COALESCE(bp.business_name, u.email, 'Business') ASC
            """,
            (client_user_id,),
        )
        followed_rows = cur.fetchall()

        # -------------------------
        # SERVICE REQUESTS
        # -------------------------
        cur.execute(
            """
            SELECT
                sr.id,
                sr.user_id,
                COALESCE(bp.business_name, u.email, 'Business') AS business_name,
                bp.logo_url,
                sr.service_title_snapshot,
                sr.service_price_snapshot,
                sr.status,
                sr.request_details,
                sr.preferred_date_text,
                sr.preferred_time_text,
                sr.quantity,
                sr.created_at,
                sr.updated_at,
                (
                    SELECT COUNT(*)
                    FROM service_request_messages srm
                    WHERE srm.service_request_id = sr.id
                ) AS message_count,
                (
                    SELECT srm.message_body
                    FROM service_request_messages srm
                    WHERE srm.service_request_id = sr.id
                    ORDER BY srm.created_at DESC, srm.id DESC
                    LIMIT 1
                ) AS latest_message_body
            FROM service_requests sr
            LEFT JOIN business_profile bp
                ON bp.user_id = sr.user_id
            LEFT JOIN users u
                ON u.id = sr.user_id
            WHERE sr.client_user_id = %s
               OR LOWER(COALESCE(sr.client_email, '')) = LOWER(%s)
            ORDER BY sr.created_at DESC, sr.id DESC
            LIMIT 100
            """,
            (client_user_id, user.get("email") or ""),
        )
        request_rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    # -------------------------
    # FORMAT FOLLOWED BUSINESSES
    # -------------------------
    followed_businesses = []
    for row in followed_rows:
        followed_businesses.append(
            {
                "business_user_id": row[0],
                "business_name": row[1] or "Business",
                "logo_url": row[2] or "",
                "email": row[3] or "",
                "phone": row[4] or "",
                "website": row[5] or "",
                "address": row[6] or "",
                "followed_at": row[7],
                "services_url": url_for("public_services_page", user_id=row[0], lang=lang),
                "profile_url": url_for("business_profile", user_id=row[0], lang=lang),
            }
        )

    # -------------------------
    # FORMAT SERVICE REQUESTS
    # -------------------------
    client_requests = []
    for row in request_rows:
        req = {
            "id": row[0],
            "business_user_id": row[1],
            "business_name": row[2] or "Business",
            "business_logo_url": row[3] or "",
            "service_title_snapshot": row[4] or "",
            "service_price_snapshot": float(row[5] or 0),
            "status": row[6] or "requested",
            "request_details": row[7] or "",
            "preferred_date_text": row[8] or "",
            "preferred_time_text": row[9] or "",
            "quantity": int(row[10] or 1),
            "created_at": row[11],
            "updated_at": row[12],
            "message_count": int(row[13] or 0),
            "latest_message_body": row[14] or "",
        }

        # attach conversation id (SAFE)
        try:
            conversation_id = get_or_create_conversation(
                business_user_id=req["business_user_id"],
                client_user_id=client_user_id,
                service_request_id=req["id"],
            )
            req["conversation_id"] = conversation_id
        except Exception as e:
            logger.warning("Conversation attach failed: %s", e)
            req["conversation_id"] = None

        client_requests.append(req)

    # -------------------------
    # REFRESH USER FROM DB (FIX STALE PROFILE DATA)
    # -------------------------
    fresh_user = get_current_user()

    return render_template(
        "client_dashboard.html",
        user=fresh_user,
        lang=lang,
        followed_businesses=followed_businesses,
        client_requests=client_requests,
    )


# -------------------------
# MESSAGING ROUTES
# -------------------------

@app.route("/api/conversation/get-or-create", methods=["POST"])
@login_required
def api_get_or_create_conversation():
    user = get_current_user()
    client_user_id = user["id"]

    business_user_id = request.json.get("business_user_id")
    service_request_id = request.json.get("service_request_id")

    if not business_user_id:
        return jsonify({"error": "Missing business_user_id"}), 400

    conversation_id = get_or_create_conversation(
        business_user_id=business_user_id,
        client_user_id=client_user_id,
        service_request_id=service_request_id,
    )

    if not conversation_id:
        return jsonify({"error": "Failed to create conversation"}), 500

    return jsonify({"conversation_id": conversation_id})


@app.route("/api/conversation/send-message", methods=["POST"])
@login_required
def api_send_message():
    user = get_current_user()
    sender_user_id = user["id"]

    conversation_id = request.json.get("conversation_id")
    message_text = (request.json.get("message_text") or "").strip()

    if not conversation_id or not message_text:
        return jsonify({"error": "Missing data"}), 400

    success = send_message(
        conversation_id=conversation_id,
        sender_user_id=sender_user_id,
        message_text=message_text,
    )

    if not success:
        return jsonify({"error": "Message failed"}), 500

    return jsonify({
        "success": True,
        "message": {
            "conversation_id": conversation_id,
            "sender_user_id": sender_user_id,
            "message_text": message_text
        }
    })


@app.route("/messages")
@login_required
def messages_page():
    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            c.id,
            c.business_user_id,
            c.client_user_id,
            c.created_at,
            bu.email,
            cu.email,
            m.message_text,
            m.created_at,
            COALESCE(um.unread_count, 0) AS unread_count
        FROM conversations c

        LEFT JOIN users bu ON bu.id = c.business_user_id
        LEFT JOIN users cu ON cu.id = c.client_user_id

        LEFT JOIN LATERAL (
            SELECT message_text, created_at
            FROM messages
            WHERE conversation_id = c.id
            ORDER BY created_at DESC
            LIMIT 1
        ) m ON TRUE

        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS unread_count
            FROM messages m2
            WHERE m2.conversation_id = c.id
              AND m2.sender_user_id != %s
              AND COALESCE(m2.is_read, FALSE) = FALSE
        ) um ON TRUE

        WHERE c.business_user_id = %s
           OR c.client_user_id = %s

        ORDER BY COALESCE(m.created_at, c.created_at) DESC
        """,
        (user_id, user_id, user_id),
    )

    rows = cur.fetchall()

    # --- MARK CURRENT CONVERSATION AS READ (SAFE) ---
    conversation_id = request.args.get("conversation_id")

    if conversation_id:
        cur.execute(
            """
            UPDATE messages
            SET is_read = TRUE
            WHERE conversation_id = %s
              AND sender_user_id != %s
              AND COALESCE(is_read, FALSE) = FALSE
            """,
            (conversation_id, user_id),
        )
        conn.commit()

    conversations = []

    for row in rows:
        convo_id = row[0]
        business_id = row[1]
        client_id = row[2]

        business_email = row[4] or "Business"
        client_email = row[5] or "Client"

        if user_id == business_id:
            display_name = client_email
        else:
            display_name = business_email

        conversations.append({
            "id": convo_id,
            "display_name": display_name,
            "business_user_id": business_id,
            "client_user_id": client_id,
            "created_at": row[3],
            "last_message": row[6] or "",
            "last_message_time": row[7],
            "unread_count": row[8] or 0,
        })

    cur.close()
    conn.close()

    # -------------------------
    # ACTIVE CONVERSATION FIX (CLICK ISSUE)
    # -------------------------
    open_conversation_id = request.args.get("open")
    active_messages = []

    if open_conversation_id:
        try:
            open_conversation_id = int(open_conversation_id)

            conn = get_db_connection()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT id
                FROM conversations
                WHERE id = %s
                  AND (business_user_id = %s OR client_user_id = %s)
                """,
                (open_conversation_id, user_id, user_id),
            )
            valid = cur.fetchone()

            if valid:
                cur.execute(
                    """
                    SELECT id, sender_user_id, message_text, created_at
                    FROM messages
                    WHERE conversation_id = %s
                    ORDER BY created_at ASC
                    """,
                    (open_conversation_id,),
                )

                rows = cur.fetchall()

                active_messages = [
                    {
                        "id": r[0],
                        "sender_user_id": r[1],
                        "message_text": r[2],
                        "created_at": r[3],
                    }
                    for r in rows
                ]

            cur.close()
            conn.close()

        except Exception as e:
            logger.warning("Active conversation load failed: %s", e)

    return render_template(
        "messages.html",
        conversations=conversations,
        active_messages=active_messages,
        open_conversation_id=open_conversation_id
    )

@app.route("/api/conversation/delete/<int:conversation_id>", methods=["POST"])
@login_required
def delete_conversation(conversation_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # delete messages first
        cur.execute(
            "DELETE FROM messages WHERE conversation_id = %s",
            (conversation_id,)
        )

        # delete conversation
        cur.execute(
            "DELETE FROM conversations WHERE id = %s",
            (conversation_id,)
        )

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.exception("Delete conversation failed: %s", e)

    finally:
        cur.close()
        conn.close()

    return jsonify({"success": True})


@app.route("/api/conversation/mark-read/<int:conversation_id>", methods=["POST"])
@login_required
def mark_conversation_read(conversation_id):
    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            UPDATE messages
            SET is_read = TRUE
            WHERE conversation_id = %s
              AND sender_user_id != %s
              AND COALESCE(is_read, FALSE) = FALSE
            """,
            (conversation_id, user_id),
        )

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.exception("Mark read failed: %s", e)

    finally:
        cur.close()
        conn.close()

    return jsonify({"success": True})


@app.route("/api/messages/<int:conversation_id>", methods=["GET"])
@login_required
def get_messages(conversation_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    user = get_current_user()
    user_id = user["id"]

    # -------------------------
    # VERIFY USER IS IN CONVERSATION
    # -------------------------
    cursor.execute(
        """
        SELECT id
        FROM conversations
        WHERE id = %s
          AND (business_user_id = %s OR client_user_id = %s)
        """,
        (conversation_id, user_id, user_id),
    )
    convo = cursor.fetchone()

    if not convo:
        cursor.close()
        conn.close()
        return jsonify({"error": "Unauthorized"}), 403

    # -------------------------
    # MARK MESSAGES AS READ (DO THIS FIRST)
    # -------------------------
    cursor.execute(
        """
        UPDATE messages
        SET is_read = TRUE
        WHERE conversation_id = %s
          AND sender_user_id != %s
        """,
        (conversation_id, user_id),
    )

    conn.commit()

    # -------------------------
    # FETCH MESSAGES
    # -------------------------
    cursor.execute(
        """
        SELECT id, conversation_id, sender_user_id, message_text, created_at
        FROM messages
        WHERE conversation_id = %s
        ORDER BY created_at ASC
        """,
        (conversation_id,)
    )

    rows = cursor.fetchall()

    messages = [
        {
            "id": r[0],
            "conversation_id": r[1],
            "sender_user_id": r[2],
            "message_text": r[3],
            "created_at": r[4].isoformat() if r[4] else None,
        }
        for r in rows
    ]

    cursor.close()
    conn.close()

    return jsonify({"messages": messages})


@app.route("/api/create-test-convo")
@login_required
def create_test_convo():
    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cur = conn.cursor()

    # create conversation with self (for testing)
    cur.execute(
        """
        INSERT INTO conversations (business_user_id, client_user_id)
        VALUES (%s, %s)
        RETURNING id
        """,
        (user_id, user_id)
    )

    convo_id = cur.fetchone()[0]

    # add first message
    cur.execute(
        """
        INSERT INTO messages (conversation_id, sender_user_id, message_text)
        VALUES (%s, %s, %s)
        """,
        (convo_id, user_id, "Test message working 🚀")
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "ok"}


@app.route("/client/request/<int:request_id>/update", methods=["POST"])
@login_required
def client_update_request(request_id):
    user = get_current_user()
    user_id = user["id"]
    user_email = (user.get("email") or "").strip().lower()
    lang = get_request_lang()

    request_details = (request.form.get("request_details") or "").strip()
    preferred_date_text = (request.form.get("preferred_date_text") or "").strip()
    preferred_time_text = (request.form.get("preferred_time_text") or "").strip()
    quantity_raw = (request.form.get("quantity") or "1").strip()

    try:
        quantity = max(1, int(quantity_raw))
    except ValueError:
        quantity = 1

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT id, user_id, status, service_title_snapshot
            FROM service_requests
            WHERE id = %s
              AND (
                    client_user_id = %s
                    OR LOWER(COALESCE(client_email, '')) = LOWER(%s)
                  )
            LIMIT 1
            """,
            (request_id, user_id, user_email),
        )
        row = cur.fetchone()

        if not row:
            cur.close()
            conn.close()
            return redirect(url_for("client_dashboard", lang=lang))

        _, business_user_id, status, service_title = row
        status = (status or "requested").strip().lower()

        if status not in ("requested", "approved"):
            cur.close()
            conn.close()
            return redirect(url_for("client_dashboard", lang=lang))

        cur.execute(
            """
            UPDATE service_requests
            SET request_details = %s,
                preferred_date_text = %s,
                preferred_time_text = %s,
                quantity = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (
                request_details,
                preferred_date_text,
                preferred_time_text,
                quantity,
                now_local(),
                request_id,
            ),
        )

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.exception("Client failed updating service request %s: %s", request_id, e)
        return redirect(url_for("client_dashboard", lang=lang))
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    create_notification_if_enabled(
        user_id=business_user_id,
        category="business_request_alerts",
        notification_type="service_request_updated_by_client",
        title="Request updated",
        body=f"{service_title or 'A request'} was updated by the client. Review the latest details.",
        link_url=f"/requests/{request_id}",
    )

    log_service_request_event(
        request_id,
        business_user_id,
        "client_updated",
        note="Client updated request details.",
    )

    return redirect(url_for("client_dashboard", lang=lang))


@app.route("/client/request/<int:request_id>/message", methods=["POST"])
@login_required
def client_send_request_message(request_id):
    user = get_current_user()
    client_user_id = user["id"]
    client_email = (user.get("email") or "").strip().lower()
    lang = get_request_lang()

    message_body = (request.form.get("message_body") or "").strip()
    if not message_body:
        return redirect(url_for("client_dashboard", lang=lang))

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT id, user_id, service_title_snapshot
            FROM service_requests
            WHERE id = %s
              AND (
                    client_user_id = %s
                    OR LOWER(COALESCE(client_email, '')) = LOWER(%s)
                  )
            LIMIT 1
            """,
            (request_id, client_user_id, client_email),
        )
        row = cur.fetchone()

        if not row:
            return redirect(url_for("client_dashboard", lang=lang))

        _, business_user_id, service_title = row

        # -------------------------
        # CREATE CONVERSATION + SEND MESSAGE (ONLY ONCE)
        # -------------------------
        conversation_id = get_or_create_conversation(
            business_user_id=business_user_id,
            client_user_id=client_user_id,
            service_request_id=request_id,
        )

        if conversation_id:
            send_message(
                conversation_id=conversation_id,
                sender_user_id=client_user_id,
                message_text=message_body,
            )

        # -------------------------
        # UPDATE REQUEST TIMESTAMP
        # -------------------------
        cur.execute(
            """
            UPDATE service_requests
            SET updated_at = %s
            WHERE id = %s
            """,
            (now_local(), request_id),
        )

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.exception("Client message send failed for request_id=%s: %s", request_id, e)
        return redirect(url_for("client_dashboard", lang=lang))

    finally:
        cur.close()
        conn.close()

    create_notification_if_enabled(
        user_id=business_user_id,
        category="business_request_alerts",
        notification_type="request_message",
        title="New message on request",
        body=f"{service_title or 'A request'}: {message_body[:90]}",
        link_url=f"/requests/{request_id}#messages",
    )

    log_service_request_event(
        request_id,
        business_user_id,
        "client_message",
        note=message_body[:240],
    )

    return redirect(url_for("client_dashboard", lang=lang))

@app.route("/client/request/<int:request_id>/cancel", methods=["POST"])
@login_required
def client_cancel_request(request_id):
    user = get_current_user()
    user_id = user["id"]
    user_email = (user.get("email") or "").strip().lower()
    lang = get_request_lang()

    cancel_reason = (request.form.get("cancel_reason") or "").strip()

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT id, user_id, status, service_title_snapshot
            FROM service_requests
            WHERE id = %s
              AND (
                    client_user_id = %s
                    OR LOWER(COALESCE(client_email, '')) = LOWER(%s)
                  )
            LIMIT 1
            """,
            (request_id, user_id, user_email),
        )
        row = cur.fetchone()

        if not row:
            cur.close()
            conn.close()
            return redirect(url_for("client_dashboard", lang=lang))

        _, business_user_id, status, service_title = row
        status = (status or "requested").strip().lower()

        if status in ("completed", "cancelled"):
            cur.close()
            conn.close()
            return redirect(url_for("client_dashboard", lang=lang))

        cur.execute(
            """
            UPDATE service_requests
            SET status = 'cancelled',
                cancel_requested_by_client = TRUE,
                cancel_reason = %s,
                cancelled_at = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (
                cancel_reason,
                now_local(),
                now_local(),
                request_id,
            ),
        )

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.exception("Client failed cancelling service request %s: %s", request_id, e)
        return redirect(url_for("client_dashboard", lang=lang))
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    cancel_body_parts = [f"{service_title or 'A request'} was cancelled by the client."]

    if cancel_reason:
        clean_cancel_reason = " ".join(str(cancel_reason).split())
        cancel_body_parts.append(f"Reason: {clean_cancel_reason[:120]}")

    create_notification_if_enabled(
        user_id=business_user_id,
        category="business_request_alerts",
        notification_type="service_request_cancelled_by_client",
        title=f"Client cancelled {service_title or 'a request'}",
        body=" ".join(cancel_body_parts),
        link_url=f"/requests/{request_id}",
    )

    log_service_request_event(
        request_id,
        business_user_id,
        "client_cancelled",
        note=cancel_reason or "Client cancelled the request.",
    )

    return redirect(url_for("client_dashboard", lang=lang))


# =========================
# BUSINESS DISCOVERY
# =========================

@app.route("/business-search")
def business_search():
    query = (request.args.get("q") or "").strip()
    results = []

    if query:
        results = search_businesses_by_name(query)

    return render_template("business_search.html", results=results, query=query)


@app.route("/business/<int:user_id>")
def business_profile(user_id):
    profile = get_business_profile_by_user_id(user_id)
    services = get_user_services(user_id)

    return render_template(
        "business_profile.html",
        profile=profile,
        services=services,
        business_user_id=user_id,
    )


# =========================
# FOLLOW SYSTEM
# =========================

@app.route("/follow/<int:user_id>", methods=["POST"])
@login_required
def follow_business(user_id):
    client = get_current_user()
    client_id = client["id"]
    client_email = (client.get("email") or "").strip()

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO business_followers (client_user_id, business_user_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (client_id, user_id),
        )

        followed_now = cur.rowcount > 0
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    if followed_now:
        follower_label = client_email or f"Client #{client_id}"
        create_notification_if_enabled(
            user_id=user_id,
            category="business_request_alerts",
            notification_type="business_followed",
            title="New client follower",
            body=f"{follower_label} followed your business and can now access your public profile.",
            link_url=f"/business/{user_id}",
        )

    return redirect(url_for("business_profile", user_id=user_id))


@app.route("/unfollow/<int:user_id>", methods=["POST"])
@login_required
def unfollow_business(user_id):
    client = get_current_user()
    client_id = client["id"]

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        DELETE FROM business_followers
        WHERE client_user_id = %s AND business_user_id = %s
        """,
        (client_id, user_id),
    )

    conn.commit()
    cur.close()
    conn.close()

    return redirect(url_for("business_profile", user_id=user_id))


# =========================
# SERVICE REQUEST (CLIENT SIDE)
# =========================

@app.route("/request-service/<int:service_id>", methods=["POST"])
@login_required
def request_service(service_id):
    client_user = get_current_user()
    lang = get_request_lang()

    client_name = (
        request.form.get("client_name")
        or request.form.get("name")
        or ""
    ).strip()

    client_email = (
        request.form.get("client_email")
        or request.form.get("email")
        or client_user.get("email")
        or ""
    ).strip()

    client_phone = (
        request.form.get("client_phone")
        or request.form.get("phone")
        or ""
    ).strip()

    request_details = (
        request.form.get("request_details")
        or request.form.get("details")
        or request.form.get("notes")
        or request.form.get("message")
        or request.form.get("description")
        or ""
    ).strip()

    preferred_date_text = (
        request.form.get("preferred_date_text")
        or request.form.get("preferred_date")
        or ""
    ).strip()

    preferred_time_text = (
        request.form.get("preferred_time_text")
        or request.form.get("preferred_time")
        or ""
    ).strip()

    quantity_raw = (
        request.form.get("quantity")
        or "1"
    ).strip()

    try:
        quantity = max(1, int(quantity_raw))
    except ValueError:
        quantity = 1

    logger.info(
        "[ClientRequestSubmit] service_id=%s client_name=%s client_email=%s client_phone=%s request_details=%s preferred_date=%s preferred_time=%s quantity=%s form_keys=%s",
        service_id,
        client_name,
        client_email,
        client_phone,
        request_details,
        preferred_date_text,
        preferred_time_text,
        quantity,
        list(request.form.keys()),
    )

    if not client_name or not client_email:
        return redirect(request.referrer or url_for("client_dashboard", lang=lang))

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT user_id
            FROM services
            WHERE id = %s AND is_active = TRUE
            """,
            (service_id,),
        )
        row = cur.fetchone()

        if not row:
            return redirect(request.referrer or url_for("client_dashboard", lang=lang))

        business_user_id = row[0]

    finally:
        cur.close()
        conn.close()

    request_id = create_service_request(
        user_id=business_user_id,
        service_id=service_id,
        client_name=client_name,
        client_email=client_email,
        client_phone=client_phone,
        request_details=request_details,
        preferred_date_text=preferred_date_text,
        preferred_time_text=preferred_time_text,
        quantity=quantity,
        client_user_id=client_user["id"],
    )

    logger.info(
        "[ClientRequestCreated] request_id=%s service_id=%s request_details=%s",
        request_id,
        service_id,
        request_details,
    )

    if not request_id:
        return redirect(request.referrer or url_for("client_dashboard", lang=lang))

    return redirect(url_for("client_dashboard", lang=lang))


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
        SELECT user_id, invoice_number, COALESCE(view_count, 0)
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

    owner_user_id, invoice_number, current_count = row
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

    create_notification_if_enabled(
        user_id=owner_user_id,
        category="invoice_alerts",
        notification_type="invoice_viewed",
        title=f"Invoice {invoice_number} was viewed",
        body=f"Your client opened the invoice link. Total views: {view_summary['view_count']}.",
        link_url=f"/invoices/{invoice_id}",
    )

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


# =========================
# BILLBEAM CONVERSION + CLIENT SYSTEM HELPERS
# =========================

# -------------------------
# TRIAL / FIRST 3 INVOICES SYSTEM
# -------------------------
def get_user_invoice_count_this_month(user_id: int) -> int:
    if not user_id:
        return 0

    now = now_local()
    month_start = datetime(now.year, now.month, 1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM invoices
        WHERE user_id = %s
          AND created_at >= %s
          AND created_at < %s
        """,
        (user_id, month_start, next_month),
    )
    count = cur.fetchone()[0] or 0
    cur.close()
    conn.close()

    return int(count)


def is_user_in_trial_full_experience(user_id: int) -> bool:
    """
    FULL experience for first 3 invoices
    """
    return get_user_invoice_count_this_month(user_id) < 3


def should_prompt_upgrade(user_id: int) -> bool:
    """
    After 3 invoices → soft prompt (NOT blocking)
    """
    return get_user_invoice_count_this_month(user_id) >= 3


# -------------------------
# FEATURE ACCESS (NON-BREAKING OVERRIDES)
# -------------------------
def can_use_branding_full(user_or_plan=None) -> bool:
    user = get_current_user()
    if user.get("id") and is_user_in_trial_full_experience(user["id"]):
        return True
    return can_use_branding(user_or_plan)


def can_send_full_experience(user_or_plan=None) -> bool:
    user = get_current_user()
    if user.get("id") and is_user_in_trial_full_experience(user["id"]):
        return True
    return can_email_invoices(user_or_plan)


# -------------------------
# BUSINESS DISCOVERY HELPERS
# -------------------------
def search_businesses_by_name(query: str, limit: int = 20):
    query = (query or "").strip().lower()
    if not query:
        return []

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT bp.user_id, bp.business_name, bp.logo_url
        FROM business_profile bp
        WHERE LOWER(bp.business_name) LIKE %s
        ORDER BY bp.business_name ASC
        LIMIT %s
        """,
        (f"%{query}%", limit),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    results = []
    for user_id, name, logo_url in rows:
        results.append({
            "user_id": user_id,
            "business_name": name or "",
            "logo_url": logo_url or "",
        })

    return results


# -------------------------
# FOLLOW SYSTEM HELPERS
# -------------------------
def is_following_business(client_user_id: int, business_user_id: int) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM business_followers
        WHERE client_user_id = %s AND business_user_id = %s
        LIMIT 1
        """,
        (client_user_id, business_user_id),
    )
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists


# -------------------------
# SERVICE REQUEST HELPERS (SAFE WRAPPERS)
# -------------------------
def create_service_request_safe(data: dict):
    """
    Wrapper so routes stay clean later
    """
    return create_service_request(**data)


# -------------------------
# REMOTE SIGNATURE HELPERS (HOOKS ONLY)
# -------------------------
def invoice_requires_signature(invoice: dict) -> bool:
    """
    Determine if invoice needs signature
    (future-safe hook)
    """
    return not bool(invoice.get("signature_data"))


def should_show_signature_pad(invoice: dict) -> bool:
    """
    Used on public invoice page
    """
    return invoice_requires_signature(invoice)


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
            "logo_url": None,
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
        "logo_url": logo_url if logo_url else None,
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
                SELECT id, user_id, name, description, price, pricing_type, duration_minutes, image_url, is_active, created_at
                FROM services
                WHERE user_id = %s
                ORDER BY created_at DESC, id DESC
                """,
                (user_id,),
            )
        else:
            cur.execute(
                """
                SELECT id, user_id, name, description, price, pricing_type, duration_minutes, image_url, is_active, created_at
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
    for service_id, service_user_id, name, description, price, pricing_type, duration_minutes, image_url, is_active, created_at in rows:
        services.append(
            {
                "id": service_id,
                "user_id": service_user_id,
                "name": name or "",
                "description": description or "",
                "price": float(price or 0),
                "pricing_type": pricing_type or "fixed",
                "duration_minutes": duration_minutes,
                "image_url": image_url or "",
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
            SELECT 
                id,
                user_id,
                name,
                description,
                price,
                image_url,
                is_active,
                created_at,
                pricing_type,
                duration_minutes,
                availability_notes
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
        "image_url": row[5] or "",
        "is_active": bool(row[6]),
        "created_at": row[7],
        "pricing_type": row[8] or "fixed",
        "duration_minutes": row[9],
        "availability_notes": row[10] or "",
    }


def create_user_service(
    user_id: int,
    name: str,
    description: str = "",
    price: float = 0.0,
    image_url: str = "",
    pricing_type: str = "fixed",
    duration_minutes: int | None = None,
    category: str = "",
    location_required: bool = False,
    materials_included: str = "",
    photo_required: bool = False,
    availability_notes: str = "",
):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO services (
                user_id,
                name,
                description,
                price,
                image_url,
                pricing_type,
                duration_minutes,
                category,
                location_required,
                materials_included,
                photo_required,
                availability_notes,
                is_active,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
            RETURNING id
            """,
            (
                user_id,
                name.strip(),
                description.strip(),
                price,
                image_url.strip(),
                pricing_type,
                duration_minutes,
                category.strip(),
                location_required,
                materials_included.strip(),
                photo_required,
                availability_notes.strip(),
                now_local(),
            ),
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


def update_user_service(
    service_id,
    user_id,
    name,
    description,
    price,
    pricing_type=None,
    duration_minutes=None,
    image_url=None,
    category=None,
    availability_notes=None,
    location_required=None,
    **kwargs
):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 🔍 DEBUG BEFORE QUERY
        print("DEBUG INPUT:", {
            "service_id": service_id,
            "user_id": user_id,
            "name": name,
            "description": description,
            "price": price,
            "pricing_type": pricing_type,
            "duration_minutes": duration_minutes
        })

        cursor.execute(
            """
            UPDATE services
            SET name = %s,
                description = %s,
                price = %s,
                pricing_type = %s,
                duration_minutes = COALESCE(%s, duration_minutes),
                image_url = COALESCE(%s, image_url),
                category = COALESCE(%s, category),
                availability_notes = COALESCE(%s, availability_notes),
                location_required = COALESCE(%s, location_required)
            WHERE id = %s AND user_id = %s
            """,
            (
                name,
                description,
                price,
                pricing_type,
                duration_minutes,
                image_url,
                category,
                availability_notes,
                location_required,
                service_id,
                user_id,
            ),
        )

        conn.commit()

        # 🔍 DEBUG AFTER QUERY
        print("DEBUG RESULT:", {
            "rowcount": cursor.rowcount
        })

        return cursor.rowcount > 0

    except Exception as e:
        print("UPDATE ERROR:", e)
        conn.rollback()
        return False

    finally:
        cursor.close()
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
        # Allow flexible pricing types later (hourly / custom quote)
        # Default to 0.0 so existing flow does not break
        price = 0.0

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


def get_service_request_photos(request_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT image_url
            FROM service_request_photos
            WHERE request_id = %s
            ORDER BY created_at ASC, id ASC
            """,
            (request_id,),
        )
        rows = cur.fetchall()
        return [row[0] for row in rows if row and row[0]]

    finally:
        cur.close()
        conn.close()


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


def get_active_device_tokens_for_user(user_id):
    if not user_id:
        return []

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT platform, device_token
            FROM user_device_tokens
            WHERE user_id = %s
              AND is_active = TRUE
            ORDER BY last_seen_at DESC, id DESC
            """,
            (user_id,),
        )
        rows = cur.fetchall()

        return [
            {
                "platform": row[0],
                "device_token": row[1],
            }
            for row in rows
            if row and row[0] and row[1]
        ]

    finally:
        cur.close()
        conn.close()


def get_apns_auth_token():
    if not APNS_KEY_ID or not APNS_TEAM_ID or not APNS_AUTH_KEY:
        logger.warning("APNs is not configured. Missing APNS_KEY_ID, APNS_TEAM_ID, or APNS_AUTH_KEY.")
        return None

    now_ts = int(time.time())

    cached_token = _APNS_JWT_CACHE.get("token")
    cached_created_at = int(_APNS_JWT_CACHE.get("created_at") or 0)

    if cached_token and (now_ts - cached_created_at) < 45 * 60:
        return cached_token

    auth_key = APNS_AUTH_KEY.strip().replace("\\n", "\n")

    try:
        token = jwt.encode(
            {
                "iss": APNS_TEAM_ID,
                "iat": now_ts,
            },
            auth_key,
            algorithm="ES256",
            headers={
                "alg": "ES256",
                "kid": APNS_KEY_ID,
            },
        )

        _APNS_JWT_CACHE["token"] = token
        _APNS_JWT_CACHE["created_at"] = now_ts

        return token

    except Exception as e:
        logger.exception("Failed creating APNs auth token: %s", e)
        return None


def send_apns_push_to_token(device_token, title, body="", link_url="", notification_type=""):
    token = get_apns_auth_token()
    if not token:
        return False

    clean_device_token = (device_token or "").strip().replace(" ", "")
    if not clean_device_token:
        return False

    apns_host = "api.sandbox.push.apple.com" if APNS_USE_SANDBOX else "api.push.apple.com"
    apns_url = f"https://{apns_host}/3/device/{clean_device_token}"

    payload = {
        "aps": {
            "alert": {
                "title": title or APP_NAME,
                "body": body or "",
            },
            "sound": "default",
        },
        "link_url": link_url or "/notifications",
        "notification_type": notification_type or "",
    }

    headers = {
        "authorization": f"bearer {token}",
        "apns-topic": APNS_BUNDLE_ID,
        "apns-push-type": "alert",
        "apns-priority": "10",
    }

    try:
        with httpx.Client(http2=True, timeout=10.0) as client:
            response = client.post(apns_url, headers=headers, json=payload)

        if 200 <= response.status_code < 300:
            logger.info(
                "[APNsPushSuccess] token_suffix=%s title=%s link_url=%s",
                clean_device_token[-8:],
                title,
                link_url,
            )
            return True

        logger.warning(
            "[APNsPushFailed] status=%s response=%s token_suffix=%s",
            response.status_code,
            response.text,
            clean_device_token[-8:],
        )
        return False

    except Exception as e:
        logger.exception("APNs push request failed: %s", e)
        return False


def send_push_notification(user_id, title, body="", link_url="", notification_type=""):
    tokens = get_active_device_tokens_for_user(user_id)

    if not tokens:
        logger.info("[PushNotificationSkipped] user_id=%s no active device tokens", user_id)
        return False

    sent_any = False

    for token_row in tokens:
        platform = (token_row.get("platform") or "").strip().lower()
        device_token = token_row.get("device_token") or ""

        if platform == "ios":
            sent = send_apns_push_to_token(
                device_token=device_token,
                title=title,
                body=body or "",
                link_url=link_url or "/notifications",
                notification_type=notification_type or "",
            )
            sent_any = sent_any or sent
        else:
            logger.info(
                "[PushNotificationSkipped] unsupported platform=%s user_id=%s",
                platform,
                user_id,
            )

    return sent_any


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

    except Exception as e:
        conn.rollback()
        logger.exception("Failed creating notification for user %s: %s", user_id, e)
        return None
    finally:
        cur.close()
        conn.close()

    try:
        send_push_notification(
            user_id=user_id,
            title=title,
            body=body or "",
            link_url=link_url or "",
            notification_type=notification_type,
        )
    except Exception as e:
        logger.exception("Push notification hook failed for user %s: %s", user_id, e)

    return notification_id


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


def mark_all_notifications_read(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE notifications
            SET is_read = TRUE
            WHERE user_id = %s AND is_read = FALSE
            """,
            (user_id,),
        )
        conn.commit()
        return int(cur.rowcount or 0)
    except Exception as e:
        conn.rollback()
        logger.exception("Failed marking all notifications read for user %s: %s", user_id, e)
        return 0
    finally:
        cur.close()
        conn.close()


def ensure_notification_preferences(user_id: int):
    if not user_id:
        return None

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO notification_preferences (
                user_id,
                notifications_enabled,
                in_app_enabled,
                push_enabled,
                email_enabled,
                business_request_alerts,
                client_request_updates,
                invoice_alerts,
                payment_alerts,
                created_at,
                updated_at
            )
            VALUES (%s, TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, TRUE, TRUE, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            (user_id, now_local(), now_local()),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception("Failed ensuring notification preferences for user %s: %s", user_id, e)
        return None
    finally:
        cur.close()
        conn.close()

    return get_notification_preferences(user_id)


def get_notification_preferences(user_id: int):
    if not user_id:
        return {
            "user_id": None,
            "notifications_enabled": True,
            "in_app_enabled": True,
            "push_enabled": False,
            "email_enabled": True,
            "business_request_alerts": True,
            "client_request_updates": True,
            "invoice_alerts": True,
            "payment_alerts": True,
        }

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                user_id,
                notifications_enabled,
                in_app_enabled,
                push_enabled,
                email_enabled,
                business_request_alerts,
                client_request_updates,
                invoice_alerts,
                payment_alerts
            FROM notification_preferences
            WHERE user_id = %s
            LIMIT 1
            """,
            (user_id,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row:
        return ensure_notification_preferences(user_id)

    return {
        "user_id": row[0],
        "notifications_enabled": bool(row[1]),
        "in_app_enabled": bool(row[2]),
        "push_enabled": bool(row[3]),
        "email_enabled": bool(row[4]),
        "business_request_alerts": bool(row[5]),
        "client_request_updates": bool(row[6]),
        "invoice_alerts": bool(row[7]),
        "payment_alerts": bool(row[8]),
    }


def notification_category_enabled(user_id: int, category: str) -> bool:
    prefs = get_notification_preferences(user_id)
    if not prefs:
        return True

    if not prefs.get("notifications_enabled", True):
        return False

    if not prefs.get("in_app_enabled", True):
        return False

    category_map = {
        "business_request_alerts": "business_request_alerts",
        "client_request_updates": "client_request_updates",
        "invoice_alerts": "invoice_alerts",
        "payment_alerts": "payment_alerts",
    }

    pref_key = category_map.get((category or "").strip())
    if not pref_key:
        return True

    return bool(prefs.get(pref_key, True))


def create_notification_if_enabled(
    user_id,
    category,
    notification_type,
    title,
    body="",
    link_url="",
):
    if not user_id:
        return None

    # Critical business workflow notifications should never disappear silently.
    critical_types = {
        "service_request_created",
        "service_request_status_updated",
        "service_request_updated_by_client",
        "service_request_cancelled_by_client",
        "request_message",
    }

    if notification_type not in critical_types:
        if not notification_category_enabled(user_id, category):
            return None

    return create_notification(
        user_id=user_id,
        notification_type=notification_type,
        title=title,
        body=body,
        link_url=link_url,
    )


def register_user_device_token(user_id, platform, device_token, device_name="", app_version=""):
    if not user_id or not device_token:
        return False

    platform = (platform or "").strip().lower()
    if platform not in ("ios", "android"):
        return False

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO user_device_tokens (
                user_id,
                platform,
                device_token,
                device_name,
                app_version,
                is_active,
                last_seen_at,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, TRUE, %s, %s)
            ON CONFLICT (user_id, platform, device_token)
            DO UPDATE SET
                device_name = EXCLUDED.device_name,
                app_version = EXCLUDED.app_version,
                is_active = TRUE,
                last_seen_at = EXCLUDED.last_seen_at
            """,
            (
                user_id,
                platform,
                device_token,
                device_name or "",
                app_version or "",
                now_local(),
                now_local(),
            ),
        )

        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        logger.exception("Failed registering device token for user_id=%s platform=%s: %s", user_id, platform, e)
        return False

    finally:
        cur.close()
        conn.close()


def update_notification_preferences(user_id: int, updates: dict):
    if not user_id:
        return None

    ensure_notification_preferences(user_id)

    allowed_keys = {
        "notifications_enabled",
        "in_app_enabled",
        "push_enabled",
        "email_enabled",
        "business_request_alerts",
        "client_request_updates",
        "invoice_alerts",
        "payment_alerts",
    }

    clean_updates = {}
    for key, value in (updates or {}).items():
        if key in allowed_keys:
            clean_updates[key] = bool(value)

    if not clean_updates:
        return get_notification_preferences(user_id)

    set_clauses = []
    values = []

    for key, value in clean_updates.items():
        set_clauses.append(f"{key} = %s")
        values.append(value)

    set_clauses.append("updated_at = %s")
    values.append(now_local())
    values.append(user_id)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE notification_preferences
            SET {", ".join(set_clauses)}
            WHERE user_id = %s
            """,
            tuple(values),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception("Failed updating notification preferences for user %s: %s", user_id, e)
        return None
    finally:
        cur.close()
        conn.close()

    return get_notification_preferences(user_id)


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
    client_user_id=None,
    uploaded_files=None,  # ✅ NEW (SAFE)
):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        service_title = None
        service_description = None
        service_price = None
        service_pricing_type = None
        service_duration = None

        if service_id:
            cur.execute(
                """
                SELECT name, description, price, pricing_type, duration_minutes
                FROM services
                WHERE id = %s AND user_id = %s
                """,
                (service_id, user_id),
            )
            svc = cur.fetchone()
            if svc:
                (
                    service_title,
                    service_description,
                    service_price,
                    service_pricing_type,
                    service_duration,
                ) = svc

        client_id = find_matching_client(user_id, client_email)

        cur.execute(
            """
            INSERT INTO service_requests (
                user_id,
                client_user_id,
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
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                user_id,
                client_user_id,
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

        # ✅ SAFE PHOTO HANDLING
        photo_count = 0
        if uploaded_files:
            try:
                photo_count = len(uploaded_files)
                save_service_request_photos(request_id, uploaded_files)
            except Exception as e:
                logger.exception("Photo upload failed: %s", e)

        notification_service_label = service_title or "Custom request"
        notification_client_name = client_name or "A client"

        notification_body_parts = []

        if quantity and int(quantity or 1) > 1:
            notification_body_parts.append(f"Quantity: {quantity}")

        if preferred_date_text:
            notification_body_parts.append(f"Preferred date: {preferred_date_text}")

        if preferred_time_text:
            notification_body_parts.append(f"Preferred time: {preferred_time_text}")

        if photo_count > 0:
            notification_body_parts.append(
                f"{photo_count} photo{'s' if photo_count != 1 else ''} attached"
            )

        if request_details:
            clean_details = " ".join(str(request_details).split())
            notification_body_parts.append(clean_details[:120])

        create_notification_if_enabled(
            user_id=user_id,
            category="business_request_alerts",
            notification_type="service_request_created",
            title=f"{notification_client_name} requested {notification_service_label}",
            body=" • ".join(notification_body_parts) or "Open the request to review the details.",
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


def save_service_request_photos(request_id, files):
    if not files:
        return

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        for file in files:
            if not file or not file.filename:
                continue

            filename = secure_filename(file.filename)
            unique_name = f"{uuid.uuid4().hex}_{filename}"

            file_path = os.path.join(SERVICE_REQUEST_PHOTO_UPLOAD_ROOT, unique_name)
            file.save(file_path)

            # public path for browser
            public_url = f"/uploads/service_request_photos/{unique_name}"

            cur.execute(
                """
                INSERT INTO service_request_photos (request_id, image_url, created_at)
                VALUES (%s, %s, %s)
                """,
                (request_id, public_url, now_local()),
            )

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.exception("Failed saving request photos: %s", e)

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
            SELECT
                id,
                user_id,
                client_id,
                service_id,
                invoice_id,
                status,
                request_type,
                source,
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
                intake_answers_json,
                owner_notes,
                client_notes,
                cancel_requested_by_client,
                cancel_reason,
                approved_at,
                in_progress_at,
                completed_at,
                cancelled_at,
                converted_to_invoice_at,
                created_at,
                updated_at
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


def get_service_request_messages(request_id, user_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT sender_role, message_body, created_at
            FROM service_request_messages
            WHERE service_request_id = %s
            ORDER BY created_at ASC, id ASC
            """,
            (request_id,),
        )
        rows = cur.fetchall()

        messages = []
        for row in rows:
            messages.append(
                {
                    "sender_role": row[0] or "client",
                    "message_body": row[1] or "",
                    "created_at": row[2],
                }
            )
        return messages

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
            SELECT status, client_email, client_name, service_title_snapshot
            FROM service_requests
            WHERE id = %s AND user_id = %s
            """,
            (request_id, user_id),
        )
        row = cur.fetchone()
        if not row:
            return False

        old_status, client_email, client_name, service_title_snapshot = row

        timestamp_field = None
        if new_status == "approved":
            timestamp_field = "approved_at"
        elif new_status == "in_progress":
            timestamp_field = "in_progress_at"
        elif new_status == "completed":
            timestamp_field = "completed_at"
        elif new_status == "cancelled":
            timestamp_field = "cancelled_at"

        now_dt = now_local()

        if timestamp_field:
            cur.execute(
                f"""
                UPDATE service_requests
                SET status = %s,
                    {timestamp_field} = %s,
                    updated_at = %s
                WHERE id = %s AND user_id = %s
                """,
                (new_status, now_dt, now_dt, request_id, user_id),
            )
        else:
            cur.execute(
                """
                UPDATE service_requests
                SET status = %s,
                    updated_at = %s
                WHERE id = %s AND user_id = %s
                """,
                (new_status, now_dt, request_id, user_id),
            )

        conn.commit()

        log_service_request_event(
            request_id,
            user_id,
            "status_changed",
            old_value=old_status,
            new_value=new_status,
        )

        client_user_id = None
        if client_email:
            cur.execute(
                """
                SELECT id
                FROM users
                WHERE LOWER(email) = LOWER(%s)
                LIMIT 1
                """,
                (client_email,),
            )
            client_user_row = cur.fetchone()
            if client_user_row:
                client_user_id = client_user_row[0]

        status_label_map = {
            "requested": "Requested",
            "approved": "Approved",
            "in_progress": "In Progress",
            "completed": "Completed",
            "cancelled": "Cancelled",
        }
        status_label = status_label_map.get(new_status, new_status.replace("_", " ").title())
        service_label = (service_title_snapshot or "Your request").strip() if service_title_snapshot else "Your request"

        if old_status != new_status and client_user_id:
            create_notification_if_enabled(
                user_id=client_user_id,
                category="client_request_updates",
                notification_type="service_request_status_updated",
                title=f"{service_label} is now {status_label}",
                body=f"Your request status was updated to {status_label}.",
                link_url=f"/client/dashboard?open_messages={request_id}",
            )


        if old_status != new_status and client_email:
            email_success, email_error = send_client_service_request_status_email(
                business_user_id=user_id,
                request_id=request_id,
                client_email=client_email,
                client_name=client_name,
                service_title=service_title_snapshot,
                new_status=new_status,
            )

            if not email_success:
                logger.warning(
                    "Request status updated but client email failed for request_id=%s email=%s error=%s",
                    request_id,
                    client_email,
                    email_error,
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
    try:
        profile = get_business_profile()

        if not profile:
            return {"business_profile": {}}

        # Ensure all keys exist (prevents template crashes / missing logo)
        safe_profile = {
            "business_name": profile.get("business_name"),
            "logo_url": profile.get("logo_url"),
            "brand_color": profile.get("brand_color"),
            "accent_color": profile.get("accent_color"),
        }

        return {"business_profile": safe_profile}

    except Exception as e:
        logger.warning("Business profile context failed: %s", e)
        return {"business_profile": {}}


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


def get_outbound_email_branding():
    profile = get_business_profile_safe()
    business_name = (profile.get("business_name") or DEFAULT_BUSINESS_NAME).strip() or DEFAULT_BUSINESS_NAME
    business_email = (profile.get("email") or "").strip()
    business_website = (profile.get("website") or "").strip()

    return {
        "business_name": business_name,
        "business_email": business_email,
        "business_website": business_website,
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

    services = get_user_services(user_id, include_inactive=False)

    return render_template(
        "index.html",
        clients=clients,
        services=services,
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

    # -------------------------
    # ENSURE THREAD EXISTS (SAFE FALLBACK)
    # -------------------------
    try:
        get_or_create_conversation(
            business_user_id=user_id,
            client_user_id=service_request.get("client_id"),
            service_request_id=request_id,
        )
    except Exception as e:
        logger.warning("Conversation auto-create failed: %s", e)

    # ✅ BACK TO NORMAL FLOW (THIS IS THE FIX)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT request_details, preferred_date_text, preferred_time_text
            FROM service_requests
            WHERE id = %s AND user_id = %s
            LIMIT 1
            """,
            (request_id, user_id),
        )
        detail_row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if detail_row:
        service_request["request_details"] = detail_row[0] or ""
        service_request["preferred_date_text"] = detail_row[1] or ""
        service_request["preferred_time_text"] = detail_row[2] or ""

    request_events = get_service_request_events_for_user(request_id, user_id)
    request_photos = get_service_request_photos(request_id)
    request_messages = get_service_request_messages(request_id, user_id)

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
        request_photos=request_photos,
        request_messages=request_messages,
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


@app.route("/requests/<int:request_id>/message", methods=["POST"])
@login_required
def send_request_message(request_id):
    user = get_current_user()
    user_id = user["id"]

    message_body = (request.form.get("message_body") or "").strip()
    if not message_body:
        return lang_redirect("request_detail_page", request_id=request_id)

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # verify ownership
        cur.execute(
            """
            SELECT client_email
            FROM service_requests
            WHERE id = %s AND user_id = %s
            """,
            (request_id, user_id),
        )
        row = cur.fetchone()
        if not row:
            return lang_redirect("requests_page")

        client_email = row[0]

        # -------------------------
        # RESOLVE CLIENT USER FIRST (FIX)
        # -------------------------
        client_user_id = None
        if client_email:
            cur.execute(
                """
                SELECT id FROM users
                WHERE LOWER(email) = LOWER(%s)
                LIMIT 1
                """,
                (client_email,),
            )
            client_row = cur.fetchone()
            if client_row:
                client_user_id = client_row[0]

        # -------------------------
        # SEND INTO MAIN MESSAGING SYSTEM (FIXED)
        # -------------------------
        conversation_id = get_or_create_conversation(
            business_user_id=user_id,
            client_user_id=client_user_id,
            service_request_id=request_id,
        )

        if conversation_id and client_user_id:
            send_message(
                conversation_id=conversation_id,
                sender_user_id=user_id,
                message_text=message_body,
            )

            # -------------------------
            # MARK UNREAD FOR CLIENT
            # -------------------------
            increment_unread_count(
                conversation_id,
                client_user_id
            )

        conn.commit()

        # notify client if exists
        if client_email:
            cur.execute(
                """
                SELECT id FROM users
                WHERE LOWER(email) = LOWER(%s)
                LIMIT 1
                """,
                (client_email,),
            )
            client_row = cur.fetchone()

            if client_row:
                client_user_id = client_row[0]

                create_notification_if_enabled(
                    user_id=client_user_id,
                    category="client_request_updates",
                    notification_type="request_message",
                    title="New message on your request",
                    body=message_body[:120],
                    link_url=f"/client/dashboard?open_messages={request_id}",
                )

        return lang_redirect("request_detail_page", request_id=request_id)

    except Exception as e:
        conn.rollback()
        logger.exception("Message send failed: %s", e)
        return lang_redirect("request_detail_page", request_id=request_id)

    finally:
        cur.close()
        conn.close()


@app.route("/notifications")
@login_required
def notifications_page():
    user = get_current_user()
    user_id = user["id"]
    lang = get_request_lang()

    notification_preferences = ensure_notification_preferences(user_id)
    notifications = get_notifications_for_user(user_id, unread_only=False, limit=100)

    return render_template(
        "notifications.html",
        notifications=notifications,
        notification_preferences=notification_preferences,
        lang=lang,
    )


@app.route("/notifications/<int:notification_id>/read")
@login_required
def mark_notification_read_page(notification_id):
    user = get_current_user()
    user_id = user["id"]
    lang = get_request_lang()

    notifications = get_notifications_for_user(user_id, unread_only=False, limit=100)
    target = None

    for item in notifications:
        if item["id"] == notification_id:
            target = item
            break

    mark_notification_read(notification_id, user_id)

    if target and target.get("link_url"):
        link_url = (target.get("link_url") or "").strip()

        if link_url:
            fragment = ""
            if "#" in link_url:
                link_url, fragment_part = link_url.split("#", 1)
                fragment = f"#{fragment_part}"

            separator = "&" if "?" in link_url else "?"
            return redirect(f"{link_url}{separator}lang={lang}{fragment}")

    return lang_redirect("notifications_page")


@app.route("/notifications/read-all", methods=["POST"])
@login_required
def mark_all_notifications_read_page():
    user = get_current_user()
    user_id = user["id"]

    mark_all_notifications_read(user_id)
    return lang_redirect("notifications_page")


@app.route("/notifications/preferences", methods=["POST"])
@login_required
def update_notification_preferences_page():
    user = get_current_user()
    user_id = user["id"]

    updates = {
        "notifications_enabled": request.form.get("notifications_enabled") == "on",
        "in_app_enabled": request.form.get("in_app_enabled") == "on",
        "email_enabled": request.form.get("email_enabled") == "on",
        "business_request_alerts": request.form.get("business_request_alerts") == "on",
        "client_request_updates": request.form.get("client_request_updates") == "on",
        "invoice_alerts": request.form.get("invoice_alerts") == "on",
        "payment_alerts": request.form.get("payment_alerts") == "on",
    }

    update_notification_preferences(user_id, updates)
    return lang_redirect("notifications_page")


@app.route("/api/notifications/summary")
@login_required
def api_notifications_summary():
    user = get_current_user()
    user_id = user["id"]

    unread_count = get_unread_notification_count_for_user(user_id)
    latest_notifications = get_notifications_for_user(user_id, unread_only=False, limit=1)

    latest = latest_notifications[0] if latest_notifications else None

    return jsonify(
        {
            "unread_count": unread_count,
            "latest": latest,
        }
    )


@app.route("/api/messages/unread-count", methods=["GET"])
@login_required
def api_unread_messages_count():
    user = get_current_user()
    user_id = user["id"]

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM messages m
            JOIN conversations c ON m.conversation_id = c.id
            WHERE
                (c.business_user_id = %s OR c.client_user_id = %s)
                AND m.sender_user_id != %s
                AND COALESCE(m.is_read, FALSE) = FALSE
            """,
            (user_id, user_id, user_id),
        )

        count = cur.fetchone()[0] or 0
        return jsonify({"unread_count": count})

    except Exception as e:
        logger.exception("Unread messages count failed: %s", e)
        return jsonify({"unread_count": 0})

    finally:
        cur.close()
        conn.close()


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

    current_client_user = get_current_user()
    client_user_id = current_client_user.get("id") if current_client_user and current_client_user.get("id") else None
    client_email_fallback = (current_client_user.get("email") or "").strip() if current_client_user else ""

    error = None
    form_data = {
        "client_name": "",
        "client_email": client_email_fallback,
        "client_phone": "",
        "request_details": "",
        "preferred_date_text": "",
        "preferred_time_text": "",
        "quantity": 1,
    }

    if request.method == "POST":
        form_data["client_name"] = (request.form.get("client_name") or "").strip()
        form_data["client_email"] = (request.form.get("client_email") or client_email_fallback or "").strip()
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
                client_user_id=client_user_id,
                uploaded_files=request.files.getlist("request_photos"),
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


# -------------------------
# APPLE IAP ACTIVATION
# -------------------------
@app.route("/api/apple/activate-subscription", methods=["POST"])
@login_required
def api_apple_activate_subscription():
    user = get_current_user()
    user_id = user.get("id")

    if not user_id:
        return jsonify({"error": "No logged-in user found."}), 401

    payload = request.get_json(silent=True) or {}

    product_id = (payload.get("product_id") or payload.get("productId") or "").strip()
    transaction_id = (payload.get("transaction_id") or payload.get("transactionId") or "").strip()
    original_transaction_id = (
        payload.get("original_transaction_id")
        or payload.get("originalTransactionId")
        or transaction_id
        or ""
    ).strip()

    new_plan = get_plan_for_apple_product_id(product_id)

    if new_plan not in ("simple", "pro", "enterprise"):
        return jsonify({"error": "Unknown Apple product ID."}), 400

    if not transaction_id:
        return jsonify({"error": "Missing Apple transaction ID. Plan was not activated."}), 400

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
            "[Apple IAP Activate] user_id=%s upgraded to %s product_id=%s transaction_id=%s",
            user_id,
            new_plan,
            product_id,
            transaction_id,
        )

        session["user_id"] = user_id
        session.permanent = True
        session.modified = True

        return jsonify(
            {
                "success": True,
                "plan": new_plan,
                "product_id": product_id,
            }
        ), 200

    except Exception as e:
        logger.exception("[Apple IAP Activate] failed for user_id=%s", user_id)
        return jsonify({"error": f"Server error: {e}"}), 500


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

    uploaded_service_image = request.files.get("service_image")
    uploaded_service_image_url = ""

    if uploaded_service_image and (uploaded_service_image.filename or "").strip():
        uploaded_service_image_url, image_error = save_uploaded_service_image(
            uploaded_service_image,
            user_id,
            None,
        )
        if image_error:
            return redirect(
                lang_url_for(
                    "services_page",
                    service_error=image_error,
                )
            )

    try:
        create_user_service(
            user_id=user_id,
            name=validation["name"],
            description=validation["description"],
            price=validation["price"],
            image_url=uploaded_service_image_url,
            pricing_type=request.form.get("pricing_type") or "fixed",
            duration_minutes=int(request.form.get("duration_minutes") or 0) or None,
            category=request.form.get("category") or "",
            location_required=bool(request.form.get("location_required")),
            materials_included=request.form.get("materials_included") or "",
            photo_required=bool(request.form.get("photo_required")),
            availability_notes=request.form.get("availability_notes") or "",
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

    uploaded_service_image = request.files.get("service_image")
    final_service_image_url = None

    if uploaded_service_image and (uploaded_service_image.filename or "").strip():
        final_service_image_url, image_error = save_uploaded_service_image(
            uploaded_service_image,
            user_id,
            service_id,
        )
        if image_error:
            return redirect(
                lang_url_for(
                    "services_page",
                    edit=service_id,
                    service_error=image_error,
                )
            )

    try:
        duration_raw = request.form.get("duration_minutes")
        duration_value = int(duration_raw) if duration_raw and duration_raw.isdigit() else None

        updated = update_user_service(
            service_id=service_id,
            user_id=user_id,
            name=validation["name"],
            description=validation["description"],
            price=validation["price"],
            image_url=final_service_image_url,
            pricing_type=request.form.get("pricing_type") or "fixed",
            duration_minutes=duration_value,
            category=request.form.get("category") or "",
            location_required=True if request.form.get("location_required") else False,
            materials_included=request.form.get("materials_included") or "",
            photo_required=True if request.form.get("photo_required") else False,
            availability_notes=request.form.get("availability_notes") or "",
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
    signature_data = request.form.get("signature_data") or None

    uploaded_invoice_files = [
        f for f in request.files.getlist("attachments")
        if f and getattr(f, "filename", "").strip()
    ]

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

    uploaded_invoice_files = [
        f for f in request.files.getlist("attachments")
        if f and getattr(f, "filename", "").strip()
    ]

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

    invoice_image_errors = []

    for uploaded_file in uploaded_invoice_files:
        image_url, image_error = save_uploaded_invoice_image(uploaded_file, user_id, invoice_id)
        if image_error:
            invoice_image_errors.append(image_error)
            continue

        cursor.execute(
            """
            INSERT INTO invoice_images (invoice_id, image_url)
            VALUES (%s, %s)
            """,
            (invoice_id, image_url),
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
@app.route("/uploads/invoice_images/<path:filename>")
def uploaded_invoice_image(filename):
    return send_from_directory(INVOICE_IMAGE_UPLOAD_ROOT, filename)


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


@app.route("/uploads/logos/<path:filename>")
def uploaded_logo(filename):
    safe_filename = os.path.basename(filename or "")
    if not safe_filename:
        return "", 404
    return send_from_directory(LOGO_UPLOAD_ROOT, safe_filename)


@app.route("/uploads/services/<path:filename>")
def uploaded_service_image(filename):
    safe_filename = os.path.basename(filename or "")
    if not safe_filename:
        return "", 404
    return send_from_directory(SERVICE_IMAGE_UPLOAD_ROOT, safe_filename)


@app.route("/uploads/service_request_photos/<path:filename>")
def uploaded_service_request_photo(filename):
    safe_filename = os.path.basename(filename or "")
    if not safe_filename:
        return "", 404
    return send_from_directory(SERVICE_REQUEST_PHOTO_UPLOAD_ROOT, safe_filename)


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
    notification_preferences = get_notification_preferences(user_id)

    if request.method == "POST":
        form_type = (request.form.get("form_type") or "profile").strip().lower()

        # -------------------------
        # PASSWORD
        # -------------------------
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

                cursor.execute("SELECT password_hash FROM users WHERE id = %s", (user_id,))
                row = cursor.fetchone()

                if not row or not check_password_hash(row[0], current_password):
                    feedback_message = "Your current password is incorrect."
                    feedback_type = "error"
                else:
                    cursor.execute(
                        "UPDATE users SET password_hash = %s WHERE id = %s",
                        (generate_password_hash(new_password), user_id),
                    )
                    conn.commit()
                    feedback_message = "Password updated successfully."
                    feedback_type = "success"

                cursor.close()
                conn.close()

        # -------------------------
        # SERVICE ADD
        # -------------------------
        elif form_type == "service_add":
            name = (request.form.get("service_name") or "").strip()
            desc = (request.form.get("service_description") or "").strip()
            price = parse_float(request.form.get("service_price"), default=0.0)

            pricing_type = (request.form.get("pricing_type") or "fixed").strip()

            duration_minutes_raw = request.form.get("duration_minutes")
            try:
                duration_minutes = int(duration_minutes_raw) if duration_minutes_raw else None
            except:
                duration_minutes = None

            if not name:
                feedback_message = "Service name is required."
                feedback_type = "error"
            else:
                create_user_service(
                    user_id=user_id,
                    name=name,
                    description=desc,
                    price=price,
                    pricing_type=pricing_type,
                    duration_minutes=duration_minutes,
                )
                feedback_message = "Service added successfully."
                feedback_type = "success"

        # -------------------------
        # SERVICE UPDATE
        # -------------------------
        elif form_type == "service_update":
            try:
                service_id = int(request.form.get("service_id"))
            except:
                service_id = None

            name = (request.form.get("service_name") or "").strip()
            desc = (request.form.get("service_description") or "").strip()
            price = parse_float(request.form.get("service_price"), default=0.0)

            pricing_type = (request.form.get("pricing_type") or "fixed").strip()

            duration_minutes_raw = request.form.get("duration_minutes")
            try:
                duration_minutes = int(duration_minutes_raw) if duration_minutes_raw else None
            except:
                duration_minutes = None

            if not service_id:
                feedback_message = "Invalid service selected."
                feedback_type = "error"
            elif not name:
                feedback_message = "Service name is required."
                feedback_type = "error"
            else:
                success = update_user_service(
                    service_id,
                    user_id,
                    name,
                    desc,
                    price,
                    pricing_type=pricing_type,
                    duration_minutes=duration_minutes,
                )

                if success:
                    return redirect(lang_url_for("settings"))
                else:
                    feedback_message = "Service could not be updated."
                    feedback_type = "error"

        # -------------------------
        # PROFILE SAVE
        # -------------------------
        else:
            logo_url = (request.form.get("logo_url") or "").strip()

            upsert_business_profile({
                "business_name": request.form.get("business_name"),
                "email": request.form.get("email"),
                "phone": request.form.get("phone"),
                "website": request.form.get("website"),
                "address": request.form.get("address"),
                "logo_url": logo_url,
                "brand_color": request.form.get("brand_color"),
                "accent_color": request.form.get("accent_color"),
            })

            feedback_message = "Business profile updated successfully."
            feedback_type = "success"

    services = get_user_services(user_id, include_inactive=True)

    return render_template(
        "settings.html",
        profile=profile,
        feedback_message=feedback_message,
        feedback_type=feedback_type,
        services=services,
        editing_service=editing_service,
        notification_preferences=notification_preferences,
        lang=lang,
    )
    if edit_id:
        try:
            editing_service = get_service_by_id(int(edit_id), user_id)
        except:
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
        notification_preferences=notification_preferences,
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
                create_notification_if_enabled(
                    user_id=user_id,
                    category="payment_alerts",
                    notification_type="partial_payment_received",
                    title=f"Payment recorded for {inv_label}",
                    body=f"{format_currency(pay_amount)} was recorded. Remaining balance: {format_currency(balance)}.",
                    link_url=f"/invoices/{invoice_id_db}",
                )

                log_invoice_event(
                    invoice_id=invoice_id_db,
                    event_type="partial_payment_received",
                    title="Partial payment received",
                    details=f"Total paid is now {format_currency(total_paid)}. Remaining balance: {format_currency(balance)}.",
                    visibility="both",
                )
            else:
                create_notification_if_enabled(
                    user_id=user_id,
                    category="payment_alerts",
                    notification_type="final_payment_received",
                    title=f"Invoice {inv_label} is paid in full",
                    body=f"Final payment recorded: {format_currency(pay_amount)}.",
                    link_url=f"/invoices/{invoice_id_db}",
                )

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
    invoice_events = get_invoice_events(invoice_id, public_only=False)
    invoice_image_urls = get_invoice_image_urls(invoice_id)

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
        invoice_image_urls=invoice_image_urls,
        percent_paid=percent_paid,
        payment_summary=payment_summary,
        view_summary=view_summary,
        show_pay_button=show_pay_button,
        business_profile=get_business_profile_by_user_id(owner_user_id) if owner_user_id else get_business_profile(),
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

    token = get_or_create_public_token(invoice_id)
    owner_user_id = user_id
    owner_plan = get_user_plan_by_user_id(owner_user_id) if owner_user_id else "free"

    is_paid_in_full = payment_summary.get("is_paid_in_full", False)
    paid_at = payment_summary.get("last_payment_at")
    show_pay_button = can_collect_payments(owner_plan) and balance > 0.0001 and not is_paid_in_full

    owner_services = get_user_services(owner_user_id, include_inactive=False) if owner_user_id else []
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
        business_profile=get_business_profile_by_user_id(owner_user_id) if owner_user_id else get_business_profile(),
        owner_plan=owner_plan,
        services=owner_services,
        show_portal_branding=show_portal_branding,
        owner_user_id=owner_user_id,
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

    branding = get_outbound_email_branding()
    business_name = branding["business_name"]
    business_email = branding["business_email"]
    business_website = branding["business_website"]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{business_name} <{smtp_from}>"
    msg["To"] = to_email

    footer_lines = ["", "—", business_name]

    if business_email:
        footer_lines.append(business_email)

    if business_website:
        footer_lines.append(business_website)

    footer_lines.append("Powered by BillBeam")

    body_text += "\n" + "\n".join(footer_lines)

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


def send_basic_email_via_resend(to_email: str, subject: str, body_text: str):
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


def send_client_service_request_status_email(
    business_user_id: int,
    request_id: int,
    client_email: str,
    client_name: str,
    service_title: str,
    new_status: str,
):
    client_email = (client_email or "").strip()
    if not client_email:
        return False, "Client email missing."

    business_profile = get_business_profile_by_user_id(business_user_id) or {}
    business_name = (
        (business_profile.get("business_name") or DEFAULT_BUSINESS_NAME).strip()
        or DEFAULT_BUSINESS_NAME
    )
    business_email = (business_profile.get("email") or "").strip()
    business_website = (business_profile.get("website") or "").strip()

    client_name = (client_name or "there").strip()
    service_title = (service_title or "your request").strip()

    status_label_map = {
        "requested": "received",
        "approved": "approved",
        "in_progress": "in progress",
        "completed": "completed",
        "cancelled": "cancelled",
    }
    status_label = status_label_map.get(new_status, new_status.replace("_", " "))

    subject = f"{business_name} updated your request"
    body_lines = [
        f"Hi {client_name},",
        "",
        f"{business_name} updated the status of your request #{request_id}.",
        f"Service: {service_title}",
        f"New status: {status_label.title()}",
        "",
    ]

    if new_status == "approved":
        body_lines.append(
            "Good news — your request has been approved and is ready for the next step."
        )
    elif new_status == "in_progress":
        body_lines.append(
            "Your request is now in progress."
        )
    elif new_status == "completed":
        body_lines.append(
            "Your request has been marked as completed."
        )
    elif new_status == "cancelled":
        body_lines.append(
            "Your request has been marked as cancelled. If you have questions, reply to this email or contact the business directly."
        )
    else:
        body_lines.append(
            "This is just a quick update to keep you in the loop."
        )

    footer_lines = ["", "—", business_name]

    if business_email:
        footer_lines.append(business_email)

    if business_website:
        footer_lines.append(business_website)

    footer_lines.append("Powered by BillBeam")

    body_text = "\n".join(body_lines + footer_lines)

    if os.environ.get("RESEND_API_KEY"):
        return send_basic_email_via_resend(
            to_email=client_email,
            subject=subject,
            body_text=body_text,
        )

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
    msg["From"] = f"{business_name} <{smtp_from}>"
    msg["To"] = client_email
    msg.set_content(body_text)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
    except Exception as e:
        return False, f"Error sending email (connection or SMTP error): {e}"

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


@app.route("/api/device-token", methods=["POST"])
@login_required
def api_register_device_token():
    user = get_current_user()
    user_id = user["id"]

    data = request.get_json(silent=True) or request.form or {}

    platform = (data.get("platform") or "").strip().lower()
    device_token = (data.get("device_token") or "").strip()
    device_name = (data.get("device_name") or "").strip()
    app_version = (data.get("app_version") or "").strip()

    ok = register_user_device_token(
        user_id=user_id,
        platform=platform,
        device_token=device_token,
        device_name=device_name,
        app_version=app_version,
    )

    return jsonify({"ok": bool(ok)})


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

        cur.execute("SELECT user_id, amount, invoice_number FROM invoices WHERE id = %s", (invoice_id,))
        inv = cur.fetchone()

        if inv:
            owner_user_id = inv[0]
            inv_total = float(inv[1] or 0)
            invoice_number = inv[2] or f"#{invoice_id}"

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
                create_notification_if_enabled(
                    user_id=owner_user_id,
                    category="payment_alerts",
                    notification_type="partial_payment_received",
                    title=f"Partial payment received for {invoice_number}",
                    body=f"Total paid is now {format_currency(total_paid_now)}. Remaining balance: {format_currency(balance)}.",
                    link_url=f"/invoices/{invoice_id}",
                )

                log_invoice_event(
                    invoice_id=invoice_id,
                    event_type="partial_payment_received",
                    title="Partial payment received",
                    details=f"Total paid is now {format_currency(total_paid_now)}. Remaining balance: {format_currency(balance)}.",
                    visibility="both",
                )
            else:
                create_notification_if_enabled(
                    user_id=owner_user_id,
                    category="payment_alerts",
                    notification_type="final_payment_received",
                    title=f"Invoice {invoice_number} paid in full",
                    body="Stripe payment received and this invoice is now fully paid.",
                    link_url=f"/invoices/{invoice_id}",
                )

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


def ensure_messages_is_read_column():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name='messages' AND column_name='is_read'
            """
        )
        exists = cur.fetchone()

        if not exists:
            cur.execute(
                """
                ALTER TABLE messages
                ADD COLUMN is_read BOOLEAN DEFAULT FALSE
                """
            )
            conn.commit()
            print("✅ Added is_read column to messages table")

    except Exception as e:
        conn.rollback()
        logger.exception("Failed to ensure is_read column: %s", e)

    finally:
        cur.close()
        conn.close()


# Run once on startup
ensure_messages_is_read_column()


def increment_unread_count(conversation_id, user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE conversations
            SET unread_count = COALESCE(unread_count, 0) + 1
            WHERE id = %s
            """,
            (conversation_id,),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning("Unread increment failed: %s", e)
    finally:
        cur.close()
        conn.close()


# -------------------------
# MESSAGING CORE (STEP 1 FIX)
# -------------------------
def get_or_create_conversation(business_user_id: int, client_user_id: int):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Try existing conversation
        cur.execute(
            """
            SELECT id
            FROM conversations
            WHERE business_user_id = %s
              AND client_user_id = %s
            LIMIT 1
            """,
            (business_user_id, client_user_id),
        )
        row = cur.fetchone()

        if row:
            return row[0]

        # Create new conversation
        cur.execute(
            """
            INSERT INTO conversations (
                business_user_id,
                client_user_id,
                created_at,
                unread_count
            )
            VALUES (%s, %s, %s, 0)
            RETURNING id
            """,
            (business_user_id, client_user_id, now_local()),
        )

        conversation_id = cur.fetchone()[0]
        conn.commit()
        return conversation_id

    except Exception as e:
        conn.rollback()
        logger.exception("Failed to get/create conversation: %s", e)
        return None

    finally:
        cur.close()
        conn.close()


def send_message_in_conversation(
    business_user_id: int,
    client_user_id: int,
    sender_user_id: int,
    message_text: str,
):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # 1. Ensure conversation exists
        conversation_id = get_or_create_conversation(
            business_user_id,
            client_user_id,
        )

        if not conversation_id:
            logger.warning("No conversation could be created.")
            return None

        # 2. Insert message
        cur.execute(
            """
            INSERT INTO messages (
                conversation_id,
                sender_user_id,
                message_text,
                created_at,
                is_read
            )
            VALUES (%s, %s, %s, %s, FALSE)
            RETURNING id
            """,
            (
                conversation_id,
                sender_user_id,
                (message_text or "").strip(),
                now_local(),
            ),
        )

        message_id = cur.fetchone()[0]

        # 3. Increment unread count for the OTHER user
        recipient_user_id = (
            client_user_id
            if sender_user_id == business_user_id
            else business_user_id
        )

        increment_unread_count(conversation_id, recipient_user_id)

        conn.commit()
        return {
            "message_id": message_id,
            "conversation_id": conversation_id,
        }

    except Exception as e:
        conn.rollback()
        logger.exception("Failed to send message: %s", e)
        return None

    finally:
        cur.close()
        conn.close()


def get_total_unread_messages(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COALESCE(SUM(unread_count), 0)
            FROM conversations
            WHERE business_user_id = %s
               OR client_user_id = %s
            """,
            (user_id, user_id),
        )
        result = cur.fetchone()
        return result[0] if result else 0
    finally:
        cur.close()
        conn.close()


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