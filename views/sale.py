from flask import Blueprint, request, jsonify, Response, send_file, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import select, func, or_, case
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
import csv, io, os
import tempfile
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from utils.email_alert import (
    send_credit_repayment_email,
    send_customer_payment_receipt,
)
from utils.printer import print_sale_80mm
from contextlib import suppress

from models import (
    db,
    RetailSale,
    RetailSaleItem,
    CustomerPayment,
    BottleSize,
    StockBalance,
    User,
    Customer,
    Expense,
)

retail_bp = Blueprint("retail_bp", __name__)

# ---------------- Time / TZ helpers ----------------
# All timestamps are STORED in UTC, DISPLAYED as Africa/Nairobi (UTC+03:00).
TZ_KE = ZoneInfo("Africa/Nairobi")  # target output TZ (display)
UTC = timezone.utc                   # storage TZ (DB)

def _now_utc():
    """UTC now (aware). Use for all DateTime saved to DB."""
    return datetime.now(UTC)

def _today_ke() -> date:
    """Today as a Nairobi calendar date."""
    return datetime.now(TZ_KE).date()

def iso_ke(dt):
    """Serialize DB DateTime (naive → assume UTC) to Africa/Nairobi ISO string (+03:00)."""
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(TZ_KE).isoformat()

def _ke_bounds_utc(d: date):
    """
    For a Nairobi calendar date d, return (start_utc, end_utc) Datetimes in UTC
    that bound that local day. [start, end)
    """
    start_ke = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=TZ_KE)
    end_ke = start_ke + timedelta(days=1)
    return start_ke.astimezone(UTC), end_ke.astimezone(UTC)

def _date_range_utc(date_from: date | None, date_to: date | None):
    """
    Build (start_utc, end_utc) from optional Nairobi dates.
    If only date_from provided → [start_utc, +∞)
    If only date_to provided → (-∞, end_utc)
    If both → [start_utc, end_utc)
    """
    start_utc = end_utc = None
    if date_from:
        start_utc, _ = _ke_bounds_utc(date_from)
    if date_to:
        _, end_utc = _ke_bounds_utc(date_to)
    return start_utc, end_utc

# ---------------- Config / Helpers ----------------
BUSINESS_NAME = os.environ.get("BUSINESS_NAME", "Blue Bash Investments Limited")
BUSINESS_ADDR = os.environ.get("BUSINESS_ADDR", "Bulla Sumeya, Garissa-Kenya")
BUSINESS_PHONE = os.environ.get("BUSINESS_PHONE", "+254 202 447 447")
BUSINESS_EMAIL = os.environ.get("BUSINESS_EMAIL", "blueskydrinkingwater@gmail.com")
P_O_BOX = os.environ.get("P_O_BOX", "P.O.Box 101-70100, Garissa")

def _parse_date(s):
    if not s: return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try: return datetime.strptime(s, fmt).date()
        except ValueError: pass
    return None

def _to_bool(val, default=False):
    if val is None: return default
    return str(val).strip().lower() in ("1","true","t","yes","y")

def _logo_path():
    # Filesystem path for ReportLab
    return os.path.join(current_app.root_path, "static", "images", "Logo.png")

def _sales_items_query():
    """Base query matching your schema names."""
    return (
        db.session.query(
            RetailSale.id.label("sale_id"),
            RetailSale.receipt_number,
            RetailSale.date,
            RetailSale.customer_name,
            RetailSale.payment_method,
            RetailSaleItem.id.label("item_id"),
            RetailSaleItem.bottle_size_id,
            BottleSize.label.label("bottle_size_label"),
            RetailSaleItem.quantity.label("quantity_cartons"),
            RetailSaleItem.unit_price.label("unit_price_carton"),
            RetailSaleItem.total_price.label("line_total"),
            RetailSaleItem.cogs_unit_price.label("cogs_unit_price"),
            RetailSaleItem.cogs_total.label("cogs_total"),
        )
        .join(RetailSaleItem, RetailSaleItem.sale_id == RetailSale.id)
        .outerjoin(BottleSize, BottleSize.id == RetailSaleItem.bottle_size_id)
    )

def _apply_filters_items_q(q):
    """Apply date filters (Nairobi days → UTC bounds) + deleted/order to the items query."""
    date_from = _parse_date(request.args.get("date_from"))
    date_to = _parse_date(request.args.get("date_to"))
    include_deleted = _to_bool(request.args.get("include_deleted"), default=False)
    order = request.args.get("order", "asc")

    if not include_deleted:
        q = q.filter(RetailSale.is_deleted == False)  # noqa: E712

    start_utc, end_utc = _date_range_utc(date_from, date_to)
    if start_utc:
        q = q.filter(RetailSale.date >= start_utc)
    if end_utc:
        q = q.filter(RetailSale.date < end_utc)

    q = q.order_by(
        RetailSale.date.asc() if order == "asc" else RetailSale.date.desc(),
        RetailSale.id.asc(),
        RetailSaleItem.id.asc()
    )
    return q, date_from, date_to

def _chart_paths_from_bottle_totals(bottle_totals: dict[str, dict]):
    labels = []
    cartons = []
    values = []
    for k, v in bottle_totals.items():
        labels.append(str(k))
        cartons.append(int(v.get("cartons", 0) or 0))
        values.append(float(v.get("value", 0.0) or 0.0))
    if not labels:
        labels = ["No Data"]; cartons = [0]; values = [0.0]

    bar_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    bar_path = bar_tmp.name; bar_tmp.close()
    plt.figure(); plt.bar(labels, cartons); plt.xticks(rotation=30, ha="right")
    plt.title("Cartons per Bottle Size"); plt.xlabel("Bottle Size"); plt.ylabel("Cartons"); plt.tight_layout()
    plt.savefig(bar_path, dpi=150); plt.close()

    pie_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    pie_path = pie_tmp.name; pie_tmp.close()
    total_value = sum(values)
    sizes = values if total_value > 0 else [1 for _ in labels]
    plt.figure(); plt.pie(sizes, labels=labels, autopct="%1.1f%%"); plt.title("Revenue Share per Bottle Size")
    plt.tight_layout(); plt.savefig(pie_path, dpi=150); plt.close()

    return bar_path, pie_path

def _charts_row(bar_path, pie_path):
    cells = []
    if os.path.exists(bar_path):
        cells.append(RLImage(bar_path, width=250, height=170))
    else:
        cells.append(Paragraph("Bar chart unavailable", getSampleStyleSheet()["Normal"]))
    if os.path.exists(pie_path):
        cells.append(RLImage(pie_path, width=250, height=170))
    else:
        cells.append(Paragraph("Pie chart unavailable", getSampleStyleSheet()["Normal"]))

    t = Table([cells], colWidths=[270, 270])
    t.setStyle(TableStyle([
        ("ALIGN", (0,0), (-1,-1), "CENTER"),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING", (0,0), (-1,-1), 6),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
    ]))
    return t

# ------------ Config ------------
MAX_RECEIPT_RETRIES = 5
PACK_SIZES = {"500ml": 24, "1.5L": 12, "5L": 4, "1.5l": 12, "5l": 4}
SEQ_WIDTH = 3

def _to_int_nonneg(x):
    try: v = int(x)
    except (TypeError, ValueError): return None
    return v if v >= 0 else None

def _to_float_nonneg(x):
    try: v = float(x)
    except (TypeError, ValueError): return None
    return v if v >= 0 else None

def _current_user():
    uid = get_jwt_identity()
    return db.session.get(User, uid)

def _units_per_carton(label):
    if not label: return None
    return PACK_SIZES.get(label) or PACK_SIZES.get(str(label).lower())

def _get_or_create_stock(size_id: int) -> StockBalance:
    sb = db.session.execute(
        select(StockBalance).where(StockBalance.bottle_size_id == size_id)
    ).scalar_one_or_none()
    if not sb:
        sb = StockBalance(bottle_size_id=size_id, quantity_available=0, updated_at=_now_utc())
        db.session.add(sb); db.session.flush()
    return sb

def _adjust_stock(size_id: int, delta_cartons: int):
    sb = _get_or_create_stock(size_id)
    new_qty = int(sb.quantity_available or 0) + int(delta_cartons)
    if new_qty < 0:
        raise ValueError(f"Insufficient stock for size_id={size_id}")
    sb.quantity_available = new_qty
    sb.updated_at = _now_utc()

def _to_item_dict(it: RetailSaleItem):
    bs = it.bottle_size
    return {
        "id": it.id,
        "sale_id": it.sale_id,
        "bottle_size_id": it.bottle_size_id,
        "bottle_size_label": bs.label if bs else None,
        "quantity": int(it.quantity or 0),
        "unit_price": float(it.unit_price or 0),
        "total_price": float(it.total_price or 0),
    }

def _to_payment_dict(p: CustomerPayment):
    return {
        "id": p.id,
        "retail_sale_id": p.retail_sale_id,
        "amount": float(p.amount or 0),
        "payment_method": p.payment_method,
        "date": iso_ke(p.date),
        "added_by": p.added_by,
    }

def _to_sale_dict(s: RetailSale, include_items: bool = True, include_payments: bool = True):
    data = {
        "id": s.id,
        "receipt_number": s.receipt_number,
        "date": iso_ke(s.date),
        "sale_type": s.sale_type,
        "customer_id": s.customer_id,
        "customer_name": s.customer_name,
        "payment_method": s.payment_method,
        "total_amount": float(s.total_amount or 0),
        "paid_amount": float(s.paid_amount or 0),
        "balance_due": float(s.balance_due or max(0.0, (s.total_amount or 0) - (s.paid_amount or 0))),
        "notes": s.notes,
        "added_by": s.added_by,
        "is_deleted": bool(s.is_deleted),
    }
    if include_items:
        data["items"] = [_to_item_dict(it) for it in (s.items or [])]
    if include_payments:
        data["payments"] = [_to_payment_dict(p) for p in (s.payments or [])]
    return data

def _calc_totals_with_default_price(items):
    if not items:
        raise ValueError("items cannot be empty")

    total_amount = 0.0
    normalized = []
    for idx, it in enumerate(items, start=1):
        bs_id = it.get("bottle_size_id")
        qty = it.get("quantity")
        if not bs_id or qty is None:
            raise ValueError(f"Item #{idx}: bottle_size_id and quantity are required")
        try: qty = int(qty)
        except Exception: raise ValueError(f"Item #{idx}: quantity must be an integer")
        if qty <= 0:
            raise ValueError(f"Item #{idx}: quantity must be > 0")

        unit_price = it.get("unit_price")
        if unit_price is None:
            bs = BottleSize.query.get(bs_id)
            if not bs: raise ValueError(f"Item #{idx}: BottleSize {bs_id} not found")
            unit_price = float(bs.selling_price)
        else:
            try: unit_price = float(unit_price)
            except Exception: raise ValueError(f"Item #{idx}: unit_price must be numeric")

        total_price = unit_price * qty
        total_amount += total_price
        normalized.append({
            "bottle_size_id": int(bs_id),
            "quantity": qty,
            "unit_price": unit_price,
            "total_price": total_price
        })
    return total_amount, normalized

def _apply_stock_delta(old_items, new_items):
    from collections import defaultdict
    agg_old = defaultdict(int); agg_new = defaultdict(int)
    for it in old_items or []:
        agg_old[it.bottle_size_id] += int(it.quantity or 0)
    for it in new_items or []:
        agg_new[it["bottle_size_id"]] += int(it["quantity"] or 0)
    for size_id in set(agg_old.keys()) | set(agg_new.keys()):
        delta = agg_new[size_id] - agg_old[size_id]
        if delta != 0:
            _adjust_stock(size_id, -delta)

def generate_receipt_number(seq_width: int = SEQ_WIDTH) -> str:
    # Prefix on KE local date to reflect business day (UTC+03:00)
    date_part = datetime.now(TZ_KE).strftime("%Y%m%d")
    prefix = date_part
    latest = (
        RetailSale.query
        .with_entities(RetailSale.receipt_number)
        .filter(RetailSale.receipt_number.like(f"{prefix}%"))
        .order_by(RetailSale.receipt_number.desc())
        .first()
    )
    last_seq = 0
    if latest and latest[0]:
        suffix = latest[0][len(prefix):]
        if suffix.isdigit():
            last_seq = int(suffix)
    next_seq = last_seq + 1
    return f"{prefix}{str(next_seq).zfill(seq_width)}"

def _reserve_sale_header_with_unique_receipt(payload):
    for attempt in range(MAX_RECEIPT_RETRIES):
        try:
            rn = generate_receipt_number()
            sale = RetailSale(receipt_number=rn, **payload)
            db.session.add(sale)
            db.session.flush()
            return sale
        except IntegrityError:
            db.session.rollback()
            if attempt == MAX_RECEIPT_RETRIES - 1:
                raise

# ========================= Routes =========================
def _to_customer_dict(cust):
    return {
        "id": cust.id,
        "name": cust.name,
        "phone": cust.phone,
        "email": cust.email,
        "created_at": iso_ke(cust.created_at),
    }

# ---------------- customer creation ----------------
@retail_bp.route("/customers", methods=["POST"])
@jwt_required()
def create_customer():
    data = request.get_json(silent=True) or {}
    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 401
    try:
        customer = Customer(**data)
        db.session.add(customer)
        db.session.commit()
        return jsonify({"ok": True, "data": _to_customer_dict(customer)}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

@retail_bp.route("/customers", methods=["GET"])
@jwt_required()
def get_customers():
    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 401

    balances_subq = (
        db.session.query(
            RetailSale.customer_id.label("cid"),
            func.coalesce(func.sum(RetailSale.balance_due), 0.0).label("balance_due"),
        )
        .filter(RetailSale.is_deleted == False)
        .filter(RetailSale.customer_id.isnot(None))
        .group_by(RetailSale.customer_id)
        .subquery()
    )

    rows = (
        db.session.query(
            Customer,
            func.coalesce(balances_subq.c.balance_due, 0.0).label("balance_due"),
        )
        .outerjoin(balances_subq, Customer.id == balances_subq.c.cid)
        .order_by(Customer.name.asc())
        .all()
    )

    data = []
    for c, bal in rows:
        d = _to_customer_dict(c)
        d["total_balance_due"] = float(bal or 0.0)
        d["has_balance"] = (bal or 0.0) > 0
        data.append(d)
    return jsonify({"ok": True, "data": data}), 200

@retail_bp.route("/customers/<int:customer_id>", methods=["PUT", "PATCH"])
@jwt_required()
def update_customer(customer_id):
    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 401

    customer = db.session.get(Customer, customer_id)
    if not customer:
        return jsonify({"ok": False, "error": f"Customer {customer_id} not found"}), 404

    data = request.get_json(silent=True) or {}
    customer.name = (data.get("name") or customer.name).strip()
    customer.phone = (data.get("phone") or customer.phone).strip()
    customer.email = (data.get("email") or customer.email).strip()

    try:
        db.session.commit()
        return jsonify({"ok": True, "data": _to_customer_dict(customer)}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

@retail_bp.route("/customers/<int:customer_id>", methods=["DELETE"])
@jwt_required()
def delete_customer(customer_id):
    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 401

    customer = db.session.get(Customer, customer_id)
    if not customer:
        return jsonify({"ok": False, "error": f"Customer {customer_id} not found"}), 404

    try:
        db.session.delete(customer)
        db.session.commit()
        return jsonify({"ok": True, "message": f"Customer {customer_id} deleted"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

# ---------------- Sales ----------------
@retail_bp.route("/retail-sales", methods=["POST"])
@jwt_required()
def create_retail_sale():
    data = request.get_json(silent=True) or {}
    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 401

    sale_type = (data.get("sale_type") or "normal").strip().lower()
    if sale_type not in {"normal", "credit", "dispatch"}:
        return jsonify({"ok": False, "error": "sale_type must be one of: normal, credit, dispatch"}), 400

    cust_id = data.get("customer_id")
    cust_name = (data.get("customer_name") or "").strip() or None
    if cust_id is not None:
        customer = db.session.get(Customer, cust_id)
        if not customer:
            return jsonify({"ok": False, "error": f"Customer {cust_id} not found"}), 400
        if not cust_name:
            cust_name = customer.name

    try:
        total_amount, normalized_items = _calc_totals_with_default_price(data.get("items") or [])
        total_amount = float(total_amount or 0.0)
        paid_amount = 0.0
        balance_due = total_amount
    except ValueError as ve:
        return jsonify({"ok": False, "error": str(ve)}), 400

    try:
        sale = _reserve_sale_header_with_unique_receipt({
            "sale_type": sale_type,
            "date": _now_utc(),                 # store UTC
            "customer_name": cust_name,
            "customer_id": cust_id,
            "payment_method": None,
            "notes": (data.get("notes") or "").strip() or None,
            "total_amount": total_amount,
            "paid_amount": paid_amount,
            "balance_due": balance_due,
            "added_by": user.id,
            "is_deleted": False,
        })
        db.session.flush()
    except IntegrityError:
        db.session.rollback()
        return jsonify({"ok": False, "error": "Could not allocate receipt number, please retry"}), 409

    try:
        for it in normalized_items:
            _adjust_stock(it["bottle_size_id"], -it["quantity"])
            db.session.add(RetailSaleItem(
                sale_id=sale.id,
                bottle_size_id=it["bottle_size_id"],
                quantity=it["quantity"],
                unit_price=it["unit_price"],
                total_price=it["total_price"],
            ))
        db.session.commit()
        return jsonify({"ok": True, "message": "Sale created (unpaid)", "data": _to_sale_dict(sale)}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

# List sales (headers only)
@retail_bp.route("/retail-sales", methods=["GET"])
@jwt_required()
def list_retail_sales():
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, max(1, int(request.args.get("per_page", 50))))
    receipt = (request.args.get("receipt") or "").strip() or None
    customer = (request.args.get("customer") or "").strip() or None
    sale_type = (request.args.get("sale_type") or "").strip().lower() or None
    date_from = _parse_date(request.args.get("date_from"))
    date_to = _parse_date(request.args.get("date_to"))
    include_deleted = request.args.get("include_deleted", "false").lower() == "true"
    order = request.args.get("order", "desc")

    stmt = select(RetailSale)
    if not include_deleted:
        stmt = stmt.where(RetailSale.is_deleted == False)  # noqa: E712
    if receipt:
        stmt = stmt.where(RetailSale.receipt_number.ilike(f"%{receipt}%"))
    if customer:
        stmt = stmt.where(RetailSale.customer_name.ilike(f"%{customer}%"))
    if sale_type in {"normal", "credit", "dispatch"}:
        stmt = stmt.where(RetailSale.sale_type == sale_type)

    # Nairobi dates → UTC bounds
    start_utc, end_utc = _date_range_utc(date_from, date_to)
    if start_utc:
        stmt = stmt.where(RetailSale.date >= start_utc)
    if end_utc:
        stmt = stmt.where(RetailSale.date < end_utc)

    stmt = stmt.order_by(
        RetailSale.date.asc() if order == "asc" else RetailSale.date.desc(),
        RetailSale.id.desc()
    )
    paged = db.paginate(stmt, page=page, per_page=per_page, error_out=False)

    return jsonify({
        "ok": True,
        "data": [_to_sale_dict(s, include_items=False) for s in paged.items],
        "pagination": {
            "page": paged.page,
            "per_page": paged.per_page,
            "total": paged.total,
            "pages": paged.pages,
            "has_next": paged.has_next,
            "has_prev": paged.has_prev,
            "next_page": paged.next_num,
            "prev_page": paged.prev_num
        }
    }), 200

# Get sale (with items & payments)
@retail_bp.route("/retail-sales/<int:sale_id>", methods=["GET"])
@jwt_required()
def get_retail_sale(sale_id):
    include_deleted = request.args.get("include_deleted", "false").lower() == "true"
    sale = (
        db.session.query(RetailSale)
        .options(
            joinedload(RetailSale.items).joinedload(RetailSaleItem.bottle_size),
            joinedload(RetailSale.payments)
        )
        .filter(RetailSale.id == sale_id)
        .one_or_none()
    )
    if not sale or (not include_deleted and sale.is_deleted):
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404
    return jsonify({"ok": True, "data": _to_sale_dict(sale)}), 200

# Get sale by receipt number
@retail_bp.route("/retail-sales/by-receipt/<string:receipt_number>", methods=["GET"])
@jwt_required()
def get_retail_sale_by_receipt(receipt_number):
    include_deleted = request.args.get("include_deleted", "false").lower() == "true"
    sale = (
        db.session.query(RetailSale)
        .options(
            joinedload(RetailSale.items).joinedload(RetailSaleItem.bottle_size),
            joinedload(RetailSale.payments)
        )
        .filter(RetailSale.receipt_number == receipt_number)
        .one_or_none()
    )
    if not sale or (not include_deleted and sale.is_deleted):
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404
    return jsonify({"ok": True, "data": _to_sale_dict(sale)}), 200

# Yesterday (by Nairobi calendar day)
@retail_bp.route("/retail-sales/yesterday", methods=["GET"])
@jwt_required()
def get_yesterdays_sales():
    y_ke = _today_ke() - timedelta(days=1)
    start_utc, end_utc = _ke_bounds_utc(y_ke)
    sales = db.session.query(RetailSale).filter(
        RetailSale.date >= start_utc,
        RetailSale.date < end_utc,
        RetailSale.is_deleted == False
    ).order_by(RetailSale.date.asc(), RetailSale.id.asc()).all()
    return jsonify({
        "ok": True,
        "date": y_ke.isoformat(),
        "sales": [_to_sale_dict(s) for s in sales]
    }), 200

# Last 7 KE days (inclusive of today)
@retail_bp.route("/retail-sales/last-7-days", methods=["GET"])
@jwt_required()
def get_last_7_days_sales():
    # Nairobi-local window: [today-6 days 00:00, tomorrow 00:00)
    end_ke = _today_ke()
    start_ke = end_ke - timedelta(days=6)  # ✅ exactly 7 KE days: today + previous 6
    start_utc, _ = _ke_bounds_utc(start_ke)
    _, end_utc = _ke_bounds_utc(end_ke)    # exclusive upper bound (tomorrow 00:00 KE in UTC)

    sales = db.session.query(RetailSale).filter(
        RetailSale.date >= start_utc,
        RetailSale.date < end_utc,
        RetailSale.is_deleted == False
    ).order_by(RetailSale.date.asc(), RetailSale.id.asc()).all()

    return jsonify({
        "ok": True,
        "start_date": start_ke.isoformat(),
        "end_date": end_ke.isoformat(),
        "sales": [_to_sale_dict(s) for s in sales]
    }), 200

# Today (Nairobi exact)
@retail_bp.route("/retail-sales/today", methods=["GET"])
@jwt_required()
def list_today_sales():
    d = _today_ke()
    start_utc, end_utc = _ke_bounds_utc(d)
    stmt = (
        select(RetailSale)
        .where(RetailSale.is_deleted == False)  # noqa: E712
        .where(RetailSale.date >= start_utc)
        .where(RetailSale.date < end_utc)
        .order_by(RetailSale.date.desc(), RetailSale.id.desc())  # ✅ stable, newest first
    )
    sales = db.session.scalars(stmt).all()
    return jsonify({"ok": True, "data": [_to_sale_dict(s) for s in sales]}), 200

# Update sale (replace items if provided; stock-safe; no payment editing here)
@retail_bp.route("/retail-sales/<int:sale_id>", methods=["PUT", "PATCH"])
@jwt_required()
def update_retail_sale(sale_id):
    sale = db.session.get(RetailSale, sale_id)
    if not sale or sale.is_deleted:
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404

    data = request.get_json(silent=True) or {}
    try:
        if "date" in data:
            dd = _parse_date(data.get("date"))
            if dd is None:
                return jsonify({"ok": False, "error": "date must be YYYY-MM-DD"}), 400
            # set to midnight KE for that day, store in UTC
            start_utc, _ = _ke_bounds_utc(dd)
            sale.date = start_utc

        if "customer_name" in data:
            sale.customer_name = (data.get("customer_name") or "").strip() or None

        if "customer_id" in data:
            cid = data.get("customer_id")
            if cid is not None and not db.session.get(Customer, cid):
                return jsonify({"ok": False, "error": f"Customer {cid} not found"}), 400
            sale.customer_id = cid

        if "sale_type" in data:
            st = (data.get("sale_type") or "").strip().lower()
            if st not in {"normal", "credit", "dispatch", ""}:
                return jsonify({"ok": False, "error": "sale_type must be one of: normal, credit, dispatch"}), 400
            if st:
                sale.sale_type = st

        if "notes" in data:
            sale.notes = (data.get("notes") or "").strip() or None

        if "items" in data:
            total_amount, normalized_items = _calc_totals_with_default_price(data.get("items") or [])
            old_items = list(sale.items)
            _apply_stock_delta(old_items, normalized_items)
            for it in old_items: db.session.delete(it)
            for ni in normalized_items:
                db.session.add(RetailSaleItem(
                    sale_id=sale.id,
                    bottle_size_id=ni["bottle_size_id"],
                    quantity=ni["quantity"],
                    unit_price=ni["unit_price"],
                    total_price=ni["total_price"],
                ))
            sale.total_amount = total_amount
            sale.balance_due = max(0.0, sale.total_amount - (sale.paid_amount or 0))

        db.session.commit()
        return jsonify({"ok": True, "message": "Updated", "data": _to_sale_dict(sale)}), 200

    except ValueError as ve:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(ve)}), 400

# Soft delete sale (return stock)
@retail_bp.route("/retail-sales/<int:sale_id>", methods=["DELETE"])
@jwt_required()
def delete_retail_sale(sale_id):
    sale = db.session.get(RetailSale, sale_id)
    if not sale or sale.is_deleted:
        return jsonify({"ok": False, "error": "RetailSale not found or already deleted"}), 404
    try:
        for it in sale.items:
            _adjust_stock(it.bottle_size_id, +int(it.quantity or 0))
        sale.is_deleted = True
        db.session.commit()
        return jsonify({"ok": True, "message": "Deleted"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400
    


# Restore sale (consume stock again)
@retail_bp.route("/retail-sales/<int:sale_id>/restore", methods=["POST"])
@jwt_required()
def restore_retail_sale(sale_id):
    sale = db.session.get(RetailSale, sale_id)
    if not sale:
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404
    if not sale.is_deleted:
        return jsonify({"ok": True, "message": "Already active"}), 200
    try:
        for it in sale.items:
            _adjust_stock(it.bottle_size_id, -int(it.quantity or 0))
        sale.is_deleted = False
        db.session.commit()
        return jsonify({"ok": True, "message": "Restored", "data": _to_sale_dict(sale)}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

# List items for a sale
@retail_bp.route("/retail-sales/<int:sale_id>/items", methods=["GET"])
@jwt_required()
def list_items_for_sale(sale_id):
    sale = db.session.get(RetailSale, sale_id)
    if not sale:
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404
    return jsonify({"ok": True, "data": [_to_item_dict(it) for it in sale.items]}), 200

# Search retail sales with filters
@retail_bp.route("/retail-sales/search", methods=["GET"])
@jwt_required()
def search_retail_sales():
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, max(1, int(request.args.get("per_page", 20))))
    include_deleted = (request.args.get("include_deleted", "false").lower() == "true")
    include_items = (request.args.get("include_items", "false").lower() == "true")
    order = request.args.get("order", "desc")

    qtxt = (request.args.get("q") or "").strip() or None
    receipt = (request.args.get("receipt") or "").strip() or None
    customer = (request.args.get("customer") or "").strip() or None

    sale_type = (request.args.get("sale_type") or "").strip().lower() or None
    added_by = request.args.get("added_by")
    bottle_size_id = request.args.get("bottle_size_id")

    min_total = request.args.get("min_total")
    max_total = request.args.get("max_total")
    is_paid = (request.args.get("is_paid") or "").strip().lower() or None
    date_from = _parse_date(request.args.get("date_from"))
    date_to = _parse_date(request.args.get("date_to"))

    stmt = select(RetailSale)
    if not include_deleted:
        stmt = stmt.where(RetailSale.is_deleted == False)  # noqa: E712

    if qtxt:
        stmt = stmt.where(or_(
            RetailSale.receipt_number.ilike(f"%{qtxt}%"),
            RetailSale.customer_name.ilike(f"%{qtxt}%")
        ))
    if receipt:
        stmt = stmt.where(RetailSale.receipt_number.ilike(f"%{receipt}%"))
    if customer:
        stmt = stmt.where(RetailSale.customer_name.ilike(f"%{customer}%"))
    if sale_type in {"normal", "credit", "dispatch"}:
        stmt = stmt.where(RetailSale.sale_type == sale_type)

    # Nairobi dates → UTC bounds (range)
    start_utc, end_utc = _date_range_utc(date_from, date_to)
    if start_utc:
        stmt = stmt.where(RetailSale.date >= start_utc)
    if end_utc:
        stmt = stmt.where(RetailSale.date < end_utc)

    try:
        if min_total not in (None, ""):
            stmt = stmt.where(RetailSale.total_amount >= float(min_total))
        if max_total not in (None, ""):
            stmt = stmt.where(RetailSale.total_amount <= float(max_total))
    except ValueError:
        return jsonify({"ok": False, "error": "min_total/max_total must be numeric"}), 400

    if is_paid in {"paid", "unpaid", "partial"}:
        if is_paid == "paid":
            stmt = stmt.where((RetailSale.balance_due <= 1e-9))
        elif is_paid == "unpaid":
            stmt = stmt.where((RetailSale.paid_amount <= 1e-9) & (RetailSale.total_amount > 0))
        else:
            stmt = stmt.where((RetailSale.paid_amount > 1e-9) & (RetailSale.balance_due > 1e-9))

    if added_by not in (None, ""):
        try:
            stmt = stmt.where(RetailSale.added_by == int(added_by))
        except ValueError:
            return jsonify({"ok": False, "error": "added_by must be an integer"}), 400

    if bottle_size_id not in (None, ""):
        try:
            bsid = int(bottle_size_id)
        except ValueError:
            return jsonify({"ok": False, "error": "bottle_size_id must be an integer"}), 400
        subq = (
            select(func.count(RetailSaleItem.id))
            .where(
                RetailSaleItem.sale_id == RetailSale.id,
                RetailSaleItem.bottle_size_id == bsid
            )
            .scalar_subquery()
        )
        stmt = stmt.where(subq > 0)

    stmt = stmt.order_by(
        RetailSale.date.asc() if order == "asc" else RetailSale.date.desc(),
        RetailSale.id.desc()
    )
    paged = db.paginate(stmt, page=page, per_page=per_page, error_out=False)

    return jsonify({
        "ok": True,
        "data": [_to_sale_dict(s, include_items=include_items) for s in paged.items],
        "pagination": {
            "page": paged.page,
            "per_page": paged.per_page,
            "total": paged.total,
            "pages": paged.pages,
            "has_next": paged.has_next,
            "has_prev": paged.has_prev,
            "next_page": paged.next_num,
            "prev_page": paged.prev_num
        }
    }), 200

# ---------------- Export sales as CSV ----------------
@retail_bp.route("/retail-sales/export.csv", methods=["GET"])
@jwt_required()
def export_retail_sales_csv():
    date_from = _parse_date(request.args.get("date_from"))
    date_to = _parse_date(request.args.get("date_to"))
    include_items = request.args.get("include_items", "false").lower() == "true"

    stmt = select(RetailSale)
    start_utc, end_utc = _date_range_utc(date_from, date_to)
    if start_utc:
        stmt = stmt.where(RetailSale.date >= start_utc)
    if end_utc:
        stmt = stmt.where(RetailSale.date < end_utc)
    stmt = stmt.order_by(RetailSale.date.asc(), RetailSale.id.asc())
    sales = db.session.scalars(stmt).all()

    from io import StringIO
    si = StringIO()
    writer = csv.writer(si)

    if include_items:
        writer.writerow(["Receipt", "Date", "Customer", "Sale Type", "Bottle Size", "Quantity", "Unit Price", "Total Price"])
        for sale in sales:
            for item in sale.items:
                writer.writerow([
                    sale.receipt_number,
                    iso_ke(sale.date)[:10],  # YYYY-MM-DD in KE (+03:00 normalized)
                    sale.customer_name or "",
                    sale.sale_type,
                    item.bottle_size.label if item.bottle_size else "",
                    item.quantity,
                    item.unit_price,
                    item.total_price
                ])
    else:
        writer.writerow(["Receipt", "Date", "Customer", "Sale Type", "Total Amount", "Paid Amount", "Balance Due"])
        for sale in sales:
            writer.writerow([
                sale.receipt_number,
                iso_ke(sale.date)[:10],
                sale.customer_name or "",
                sale.sale_type,
                sale.total_amount,
                sale.paid_amount,
                sale.balance_due
            ])

    output = Response(si.getvalue(), mimetype="text/csv")
    output.headers["Content-Disposition"] = "attachment; filename=retail_sales.csv"
    return output

# ---------------- Payments ----------------
@retail_bp.route("/retail-sales/<int:sale_id>/payments", methods=["POST"])
@jwt_required()
def create_payment_for_sale(sale_id):
    sale = db.session.get(RetailSale, sale_id)
    if not sale or sale.is_deleted:
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404

    data = request.get_json(silent=True) or {}
    try:
        amt = float(data.get("amount"))
        if amt <= 0:
            return jsonify({"ok": False, "error": "amount must be > 0"}), 400
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "amount is required and must be a number"}), 400

    pmethod = (data.get("payment_method") or "").strip()
    if not pmethod:
        return jsonify({"ok": False, "error": "payment_method is required"}), 400

    remaining = max(0.0, float(sale.total_amount or 0) - float(sale.paid_amount or 0))
    if amt > remaining + 1e-9:
        return jsonify({"ok": False, "error": f"amount exceeds remaining balance ({remaining:.2f})"}), 400

    if data.get("date"):
        dd = _parse_date(data.get("date"))
        if dd is None:
            return jsonify({"ok": False, "error": "date must be YYYY-MM-DD"}), 400
        date_utc, _ = _ke_bounds_utc(dd)  # store midnight KE as UTC
    else:
        date_utc = _now_utc()

    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 401

    try:
        pay = CustomerPayment(
            retail_sale_id=sale.id,
            amount=amt,
            payment_method=pmethod,
            date=date_utc,
            added_by=user.id,
        )
        db.session.add(pay)

        sale.paid_amount = float(sale.paid_amount or 0.0) + amt
        sale.balance_due = max(0.0, float(sale.total_amount or 0.0) - float(sale.paid_amount or 0.0))

        db.session.commit()
        return jsonify({"ok": True, "message": "Payment recorded", "data": _to_payment_dict(pay)}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

def _send_credit_repayment_email(customer, sale, pay):
    try:
        from utils.email_alert import send_credit_repayment_email as _fn
        _fn(customer, sale, pay)
        return True
    except Exception as e1:
        try:
            from utils.email_alert import send_customer_payment_receipt as _fn2
            _fn2(
                customer_name=getattr(customer, "name", None),
                customer_email=getattr(customer, "email", None),
                amount=float(getattr(pay, "amount", 0) or 0),
                balance=float(getattr(sale, "balance_due", 0) or 0),
                sale_id=getattr(sale, "id", None),
            )
            return True
        except Exception as e2:
            current_app.logger.exception("Failed to send credit repayment email: %s / %s", e1, e2)
            return False

@retail_bp.route("/credit-sales/<int:sale_id>/payments", methods=["POST"])
@jwt_required()
def create_payment_for_credit_sale(sale_id):
    sale = db.session.get(RetailSale, sale_id)
    if not sale or sale.is_deleted:
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404
    if sale.sale_type != "credit":
        return jsonify({"ok": False, "error": "This is not a credit sale"}), 400

    data = request.get_json(silent=True) or {}
    try:
        amt = float(data.get("amount"))
        if amt <= 0:
            return jsonify({"ok": False, "error": "amount must be > 0"}), 400
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "amount is required and must be a number"}), 400

    pmethod = (data.get("payment_method") or "").strip()
    if not pmethod:
        return jsonify({"ok": False, "error": "payment_method is required"}), 400

    remaining = max(0.0, float(sale.total_amount or 0) - float(sale.paid_amount or 0))
    if amt > remaining + 1e-9:
        return jsonify({"ok": False, "error": f"amount exceeds remaining balance ({remaining:.2f})"}), 400

    if data.get("date"):
        dd = _parse_date(data.get("date"))
        if dd is None:
            return jsonify({"ok": False, "error": "date must be YYYY-MM-DD"}), 400
        dts = _ke_bounds_utc(dd)[0]
    else:
        dts = _now_utc()

    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 401

    try:
        pay = CustomerPayment(
            retail_sale_id=sale.id,
            amount=amt,
            payment_method=pmethod,
            date=dts,
            added_by=user.id,
        )
        db.session.add(pay)

        sale.paid_amount = float(sale.paid_amount or 0.0) + amt
        sale.balance_due = max(0.0, float(sale.total_amount or 0.0) - float(sale.paid_amount or 0.0))

        db.session.commit()
        db.session.refresh(sale)

        sent = False
        try:
            if sale.customer and sale.customer.email:
                sent = send_credit_repayment_email(sale.customer, sale, pay)
        except Exception as e:
            current_app.logger.warning("Credit repayment email failed: %s", e)

        return jsonify({
            "ok": True,
            "message": "Credit payment recorded",
            "email_sent": bool(sent),
            "data": _to_payment_dict(pay)
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

@retail_bp.route("/retail-sales/<int:sale_id>/payments", methods=["GET"])
@jwt_required()
def list_payments_for_sale(sale_id):
    sale = db.session.get(RetailSale, sale_id)
    if not sale:
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404
    rows = db.session.execute(
        select(CustomerPayment)
        .where(CustomerPayment.retail_sale_id == sale.id)
        .order_by(CustomerPayment.date.desc(), CustomerPayment.id.desc())  # ✅ time-first
    ).scalars().all()
    return jsonify({"ok": True, "data": [_to_payment_dict(p) for p in rows]}), 200

@retail_bp.route("/customer-payments/<int:payment_id>", methods=["GET"])
@jwt_required()
def get_payment(payment_id):
    pay = db.session.get(CustomerPayment, payment_id)
    if not pay:
        return jsonify({"ok": False, "error": "CustomerPayment not found"}), 404
    return jsonify({"ok": True, "data": _to_payment_dict(pay)}), 200

@retail_bp.route("/customer-payments/<int:payment_id>", methods=["PUT", "PATCH"])
@jwt_required()
def update_payment(payment_id):
    pay = db.session.get(CustomerPayment, payment_id)
    if not pay:
        return jsonify({"ok": False, "error": "CustomerPayment not found"}), 404

    sale = db.session.get(RetailSale, pay.retail_sale_id) if pay.retail_sale_id else None
    data = request.get_json(force=True, silent=True) or {}

    try:
        delta_amt = 0.0
        if "amount" in data:
            new_amt = float(data.get("amount"))
            if new_amt <= 0:
                return jsonify({"ok": False, "error": "amount must be > 0"}), 400
            if sale:
                new_total_paid = float(sale.paid_amount or 0.0) - float(pay.amount or 0.0) + new_amt
                if new_total_paid < -1e-9:
                    return jsonify({"ok": False, "error": "resulting paid amount would be negative"}), 400
                if new_total_paid > float(sale.total_amount or 0.0) + 1e-9:
                    return jsonify({"ok": False, "error": "resulting paid amount would exceed total"}), 400
            delta_amt = new_amt - float(pay.amount or 0.0)
            pay.amount = new_amt

        if "payment_method" in data:
            pmethod = (data.get("payment_method") or "").strip()
            pay.payment_method = pmethod or None

        if "date" in data:
            dd = _parse_date(data.get("date"))
            if dd is None:
                return jsonify({"ok": False, "error": "date must be YYYY-MM-DD"}), 400
            pay.date = _ke_bounds_utc(dd)[0]  # store midnight KE in UTC

        if "retail_sale_id" in data:
            return jsonify({"ok": False, "error": "Cannot reassign payment to another sale"}), 400

        if sale and abs(delta_amt) > 1e-12:
            sale.paid_amount = float(sale.paid_amount or 0.0) + delta_amt
            sale.balance_due = max(0.0, float(sale.total_amount or 0.0) - float(sale.paid_amount or 0.0))

        db.session.commit()
        return jsonify({"ok": True, "message": "Updated", "data": _to_payment_dict(pay)}), 200

    except (TypeError, ValueError) as ve:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(ve)}), 400
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

@retail_bp.route("/customer-payments", methods=["GET"])
@jwt_required()
def list_customer_payments():
    sale_id = request.args.get("sale_id")
    date_from = _parse_date(request.args.get("date_from"))
    date_to = _parse_date(request.args.get("date_to"))

    q = db.session.query(CustomerPayment)
    if sale_id:
        try:
            q = q.filter(CustomerPayment.retail_sale_id == int(sale_id))
        except ValueError:
            return jsonify({"ok": False, "error": "sale_id must be an integer"}), 400

    # Nairobi → UTC bounds
    start_utc, end_utc = _date_range_utc(date_from, date_to)
    if start_utc:
        q = q.filter(CustomerPayment.date >= start_utc)
    if end_utc:
        q = q.filter(CustomerPayment.date < end_utc)

    q = q.order_by(CustomerPayment.date.desc(), CustomerPayment.id.desc())
    rows = q.all()
    return jsonify({"ok": True, "data": [_to_payment_dict(p) for p in rows]}), 200

@retail_bp.route("/customer-payments/<int:payment_id>", methods=["DELETE"])
@jwt_required()
def delete_payment(payment_id):
    pay = db.session.get(CustomerPayment, payment_id)
    if not pay:
        return jsonify({"ok": False, "error": "CustomerPayment not found"}), 404
    sale = db.session.get(RetailSale, pay.retail_sale_id) if pay.retail_sale_id else None
    try:
        if sale:
            sale.paid_amount = max(0.0, float(sale.paid_amount or 0.0) - float(pay.amount or 0.0))
            sale.balance_due = max(0.0, float(sale.total_amount or 0.0) - float(sale.paid_amount or 0.0))
        db.session.delete(pay)
        db.session.commit()
        return jsonify({"ok": True, "message": "Deleted"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

# ---------------- Dispatch close ----------------
@retail_bp.route("/retail-sales/<int:sale_id>/close-dispatch", methods=["POST"])
@jwt_required()
def close_dispatch(sale_id):
    sale = db.session.get(RetailSale, sale_id)
    if not sale or sale.is_deleted:
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404
    if (sale.sale_type or "").lower() != "dispatch":
        return jsonify({"ok": False, "error": "Sale is not of type dispatch"}), 400

    data = request.get_json(silent=True) or {}
    returns = data.get("returns") or []
    amount_paid = _to_float_nonneg(data.get("amount_paid")) or 0.0
    pmethod = (data.get("payment_method") or "").strip() or None

    rmap = {}
    for r in returns:
        sid = r.get("bottle_size_id")
        qty = _to_int_nonneg(r.get("quantity_returned"))
        if sid is None or qty is None:
            return jsonify({"ok": False, "error": "Each return needs bottle_size_id and non-negative quantity_returned"}), 400
        rmap[int(sid)] = int(qty)

    try:
        new_total = 0.0
        for it in sale.items:
            sent = int(it.quantity or 0)
            ret = int(rmap.get(it.bottle_size_id, 0) or 0)
            if ret < 0 or ret > sent:
                return jsonify({"ok": False, "error": f"Invalid return qty for bottle_size_id={it.bottle_size_id}"}), 400
            if ret:
                _adjust_stock(it.bottle_size_id, +ret)
            sold = sent - ret
            it.quantity = sold
            it.total_price = float(it.unit_price or 0.0) * int(sold)
            new_total += it.total_price

        sale.total_amount = float(new_total)
        sale.balance_due = max(0.0, float(sale.total_amount) - float(sale.paid_amount or 0.0))

        if amount_paid and amount_paid > 0:
            remaining = max(0.0, float(sale.total_amount or 0) - float(sale.paid_amount or 0))
            if amount_paid > remaining + 1e-9:
                return jsonify({"ok": False, "error": f"amount_paid exceeds remaining balance ({remaining:.2f})"}), 400

            user = _current_user()
            if not user:
                return jsonify({"ok": False, "error": "User not found"}), 401

            pay = CustomerPayment(
                retail_sale_id=sale.id,
                amount=amount_paid,
                payment_method=pmethod,
                date=_now_utc(),
                added_by=user.id,
            )
            db.session.add(pay)
            sale.paid_amount = float(sale.paid_amount or 0.0) + amount_paid
            sale.balance_due = max(0.0, float(sale.total_amount or 0.0) - float(sale.paid_amount or 0.0))

        db.session.commit()

        sold_summary = [{
            "bottle_size_id": it.bottle_size_id,
            "bottle_size_label": (it.bottle_size.label if it.bottle_size else None),
            "cartons_sold": int(it.quantity or 0),
            "price_per_carton": float(it.unit_price or 0),
            "line_total": float(it.total_price or 0),
        } for it in sale.items]

        return jsonify({
            "ok": True,
            "message": "Dispatch closed",
            "data": {
                "sale": _to_sale_dict(sale),
                "sold_summary": sold_summary,
            }
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

# -------------- Cartons summary (by bottle size) --------------

def _cartons_summary(date_from: date | None, date_to: date | None, include_deleted: bool = False):
    """
    Returns:
      {
        "totals": { "cartons": int, "revenue": float, "bottles": int },
        "by_size": [
           { "bottle_size_id": int, "label": str, "cartons": int,
             "revenue": float, "units_per_carton": int | None, "bottles": int | None }
        ]
      }
    """
    # Base: join sale items -> sale header (for date/deleted filters) -> bottle size (label)
    q = (
        db.session.query(
            RetailSaleItem.bottle_size_id.label("bottle_size_id"),
            func.coalesce(BottleSize.label, "Unknown").label("label"),
            func.coalesce(func.sum(RetailSaleItem.quantity), 0).label("cartons"),
            func.coalesce(func.sum(RetailSaleItem.total_price), 0.0).label("revenue"),
        )
        .join(RetailSale, RetailSaleItem.sale_id == RetailSale.id)
        .outerjoin(BottleSize, BottleSize.id == RetailSaleItem.bottle_size_id)
    )

    if not include_deleted:
        q = q.filter(RetailSale.is_deleted == False)  # noqa: E712

    # Nairobi date range → UTC bounds, applied to the sale header timestamp
    start_utc, end_utc = _date_range_utc(date_from, date_to)
    if start_utc:
        q = q.filter(RetailSale.date >= start_utc)
    if end_utc:
        q = q.filter(RetailSale.date < end_utc)

    q = q.group_by(RetailSaleItem.bottle_size_id, BottleSize.label)
    # order largest first (cartons desc, then revenue desc)
    q = q.order_by(func.coalesce(func.sum(RetailSaleItem.quantity), 0).desc(),
                   func.coalesce(func.sum(RetailSaleItem.total_price), 0.0).desc())

    rows = q.all()

    by_size = []
    total_cartons = 0
    total_revenue = 0.0
    total_bottles = 0

    for r in rows:
        # Resolve units per carton from label using your PACK_SIZES map
        upc = _units_per_carton(r.label)
        cartons = int(r.cartons or 0)
        revenue = float(r.revenue or 0.0)
        bottles = cartons * int(upc) if upc else None

        by_size.append({
            "bottle_size_id": r.bottle_size_id,
            "label": r.label,
            "cartons": cartons,
            "revenue": revenue,
            "units_per_carton": int(upc) if upc else None,
            "bottles": int(bottles) if bottles is not None else None,
        })

        total_cartons += cartons
        total_revenue += revenue
        if bottles is not None:
            total_bottles += bottles

    return {
        "totals": {
            "cartons": int(total_cartons),
            "revenue": float(total_revenue),
            "bottles": int(total_bottles),
        },
        "by_size": by_size,
    }

@retail_bp.route("/retail-sales/summary/cartons", methods=["GET"])
@jwt_required()
def summary_cartons_by_size():
    """
    Query params:
      - date_from (YYYY-MM-DD) Nairobi local day, inclusive
      - date_to   (YYYY-MM-DD) Nairobi local day, inclusive
      - include_deleted: true|false (default false)
    Response:
      { ok: true, data: { totals: {...}, by_size: [...] } }
    """
    date_from = _parse_date(request.args.get("date_from"))
    date_to   = _parse_date(request.args.get("date_to"))
    include_deleted = _to_bool(request.args.get("include_deleted"), default=False)

    data = _cartons_summary(date_from, date_to, include_deleted)
    return jsonify({ "ok": True, "data": data }), 200

# ---- Convenience wrappers (Nairobi calendar) ----
@retail_bp.route("/retail-sales/summary/cartons/today", methods=["GET"])
@jwt_required()
def summary_cartons_today():
    d = _today_ke()
    data = _cartons_summary(d, d, include_deleted=False)
    return jsonify({ "ok": True, "date": d.isoformat(), "data": data }), 200

@retail_bp.route("/retail-sales/summary/cartons/yesterday", methods=["GET"])
@jwt_required()
def summary_cartons_yesterday():
    d = _today_ke() - timedelta(days=1)
    data = _cartons_summary(d, d, include_deleted=False)
    return jsonify({ "ok": True, "date": d.isoformat(), "data": data }), 200

@retail_bp.route("/retail-sales/summary/cartons/last-7-days", methods=["GET"])
@jwt_required()
def summary_cartons_last7():
    end_ke = _today_ke()
    start_ke = end_ke - timedelta(days=6)  # inclusive 7 days (t-6 .. t)
    data = _cartons_summary(start_ke, end_ke, include_deleted=False)
    return jsonify({
        "ok": True,
        "start_date": start_ke.isoformat(),
        "end_date": end_ke.isoformat(),
        "data": data
    }), 200

def _local_day_expr():
    """
    Return a SQLAlchemy expression that extracts the Africa/Nairobi calendar day
    from RetailSale.date across common dialects.
    """
    bind = db.session.get_bind()
    dialect = getattr(getattr(bind, "dialect", None), "name", "").lower()

    if dialect == "postgresql":
        # date(timezone('Africa/Nairobi', ts))
        return func.date(func.timezone("Africa/Nairobi", RetailSale.date))
    elif dialect in ("mysql", "mariadb"):
        # date(convert_tz(ts, '+00:00', '+03:00'))
        return func.date(func.convert_tz(RetailSale.date, "+00:00", "+03:00"))
    else:
        # SQLite / other: date(datetime(ts, '+3 hours'))
        return func.date(func.datetime(RetailSale.date, "+3 hours"))



# -------------- Summaries --------------
@retail_bp.route("/retail-sales/summary/by-date", methods=["GET"])
@jwt_required()
def summary_sales_by_date():
    """
    Daily sums grouped by Nairobi calendar day.
    - gross  = sum of RetailSaleItem.total_price (per sale)
    - paid   = sum of CustomerPayment.amount (per sale)
    - balance= sum over sales of max(gross - paid, 0)
    - count  = number of sales created that day
    """
    date_from = _parse_date(request.args.get("date_from"))
    date_to   = _parse_date(request.args.get("date_to"))
    include_deleted = (request.args.get("include_deleted", "false").lower() == "true")

    # Build UTC window from KE dates
    start_utc, end_utc = _date_range_utc(date_from, date_to)

    # Base sale headers within window
    q = db.session.query(RetailSale.id, RetailSale.date, RetailSale.is_deleted)
    if start_utc:
        q = q.filter(RetailSale.date >= start_utc)
    if end_utc:
        q = q.filter(RetailSale.date < end_utc)
    if not include_deleted:
        q = q.filter(RetailSale.is_deleted == False)  # noqa: E712

    sales = q.all()
    if not sales:
        return jsonify({"ok": True, "data": []}), 200

    sale_ids = [sid for sid, _, _ in sales]

    # Items → gross per sale
    items_totals = dict(
        db.session.query(
            RetailSaleItem.sale_id,
            func.coalesce(func.sum(RetailSaleItem.total_price), 0.0)
        )
        .filter(RetailSaleItem.sale_id.in_(sale_ids))
        .group_by(RetailSaleItem.sale_id)
        .all()
    )

    # Payments → paid per sale
    pay_totals = dict(
        db.session.query(
            CustomerPayment.retail_sale_id,
            func.coalesce(func.sum(CustomerPayment.amount), 0.0)
        )
        .filter(CustomerPayment.retail_sale_id.in_(sale_ids))
        .group_by(CustomerPayment.retail_sale_id)
        .all()
    )

    from collections import defaultdict
    agg = defaultdict(lambda: {"gross": 0.0, "paid": 0.0, "balance": 0.0, "count": 0})

    for sale_id, sale_dt, _is_del in sales:
        day = (iso_ke(sale_dt) or "")[:10]  # "YYYY-MM-DD" in KE
        g = float(items_totals.get(sale_id, 0.0))
        p = float(pay_totals.get(sale_id, 0.0))
        a = agg[day]
        a["gross"] += g
        a["paid"] += p
        a["balance"] += max(0.0, g - p)
        a["count"] += 1

    data = [
        {
            "date": day,
            "gross": round(v["gross"], 2),
            "paid": round(v["paid"], 2),
            "balance": round(v["balance"], 2),
            "count": v["count"],
        }
        for day, v in sorted(agg.items())
    ]
    return jsonify({"ok": True, "data": data}), 200



# ---------- COGS helper: sum purchases recorded as expenses ----------
def _sum_cogs_expenses(date_from: date | None, date_to: date | None, include_deleted: bool) -> float:
    """
    Sums Expense.amount where category='cogs' in the given (inclusive) DATE window.
    """
    q = db.session.query(func.coalesce(func.sum(Expense.amount), 0.0))\
                  .filter(func.lower(Expense.category) == "cogs")
    if not include_deleted:
        q = q.filter(Expense.is_deleted == False)  # noqa: E712
    if date_from:
        q = q.filter(Expense.date >= date_from)
    if date_to:
        q = q.filter(Expense.date <= date_to)
    return float(q.scalar() or 0.0)

# ---------- COGS Summary (totals + per bottle size) ----------
def _cogs_summary(date_from: date | None, date_to: date | None, include_deleted: bool = False):
    """
    Returns:
      {
        "totals": { "sales": float, "cogs": float, "gross": float, "gm": float,
                    "breakdown": {"cogs_sales": float, "purchases": float} },
        "by_size": [
           { "bottle_size_id": int|None, "label": str, "cartons": int,
             "sales": float, "cogs": float, "gross": float, "gm": float }
        ],
        "date_from": "YYYY-MM-DD" | None,
        "date_to":   "YYYY-MM-DD" | None
      }
    """
    # Sales-side COGS grouped by size
    q = (
        db.session.query(
            RetailSaleItem.bottle_size_id.label("bottle_size_id"),
            func.coalesce(BottleSize.label, "Unknown").label("label"),
            func.coalesce(func.sum(RetailSaleItem.quantity), 0).label("cartons"),
            func.coalesce(func.sum(RetailSaleItem.total_price), 0.0).label("sales"),
            func.coalesce(func.sum(RetailSaleItem.cogs_total), 0.0).label("cogs"),
        )
        .join(RetailSale, RetailSaleItem.sale_id == RetailSale.id)
        .outerjoin(BottleSize, BottleSize.id == RetailSaleItem.bottle_size_id)
    )

    if not include_deleted:
        q = q.filter(RetailSale.is_deleted == False)  # noqa: E712

    start_utc, end_utc = _date_range_utc(date_from, date_to)
    if start_utc:
        q = q.filter(RetailSale.date >= start_utc)
    if end_utc:
        q = q.filter(RetailSale.date < end_utc)

    q = q.group_by(RetailSaleItem.bottle_size_id, BottleSize.label)
    q = q.order_by(
        func.coalesce(func.sum(RetailSaleItem.quantity), 0).desc(),
        func.coalesce(func.sum(RetailSaleItem.total_price), 0.0).desc()
    )
    rows = q.all()

    by_size = []
    total_sales = 0.0
    total_cogs_sales = 0.0
    for r in rows:
        sales = float(r.sales or 0.0)
        cogs  = float(r.cogs or 0.0)
        gross = sales - cogs
        gm = (gross / sales * 100.0) if sales > 1e-9 else 0.0
        by_size.append({
            "bottle_size_id": r.bottle_size_id,
            "label": r.label,
            "cartons": int(r.cartons or 0),
            "sales": sales,
            "cogs":  cogs,              # per-size COGS = sales-side only
            "gross": gross,             # per-size gross = sales - per-size COGS
            "gm": gm,
        })
        total_sales += sales
        total_cogs_sales  += cogs

    # Add purchase-side COGS from Expense(category='cogs')
    purchases = _sum_cogs_expenses(date_from, date_to, include_deleted)

    cogs_total_combined = total_cogs_sales + purchases
    gross_total = total_sales - cogs_total_combined
    gm_total = (gross_total / total_sales * 100.0) if total_sales > 1e-9 else 0.0

    return {
        "totals": {
            "sales": float(total_sales),
            "cogs": float(cogs_total_combined),   # combined (sales + purchases)
            "gross": float(gross_total),
            "gm": float(gm_total),
            "breakdown": {
                "cogs_sales": float(total_cogs_sales),
                "purchases": float(purchases),
            }
        },
        "by_size": by_size,  # remains sales-side per size
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
    }

@retail_bp.route("/retail-sales/summary/cogs", methods=["GET"])
@jwt_required()
def summary_cogs():
    date_from = _parse_date(request.args.get("date_from"))
    date_to   = _parse_date(request.args.get("date_to"))
    include_deleted = _to_bool(request.args.get("include_deleted"), default=False)
    data = _cogs_summary(date_from, date_to, include_deleted)
    return jsonify({"ok": True, "data": data}), 200

@retail_bp.route("/retail-sales/summary/cogs/today", methods=["GET"])
@jwt_required()
def summary_cogs_today():
    d = _today_ke()
    data = _cogs_summary(d, d, include_deleted=False)
    return jsonify({"ok": True, "date": d.isoformat(), "data": data}), 200

@retail_bp.route("/retail-sales/summary/cogs/yesterday", methods=["GET"])
@jwt_required()
def summary_cogs_yesterday():
    d = _today_ke() - timedelta(days=1)
    data = _cogs_summary(d, d, include_deleted=False)
    return jsonify({"ok": True, "date": d.isoformat(), "data": data}), 200

@retail_bp.route("/retail-sales/summary/cogs/last-7-days", methods=["GET"])
@jwt_required()
def summary_cogs_last7():
    end_ke = _today_ke()
    start_ke = end_ke - timedelta(days=6)  # inclusive 7 KE days
    data = _cogs_summary(start_ke, end_ke, include_deleted=False)
    return jsonify({"ok": True, "start_date": start_ke.isoformat(), "end_date": end_ke.isoformat(), "data": data}), 200


# ---------------- Header block for PDF (place above the route) ----------------
def _header_table(styles, date_from, date_to):
    business_name_style = ParagraphStyle(
        name="BusinessName",
        parent=styles["Normal"],
        fontSize=16,
        textColor=colors.HexColor("#0B5394"),
        leading=20,
        spaceAfter=6,
        alignment=1,  # center
    )
    contact_info_style = ParagraphStyle(
        name="ContactInfo",
        parent=styles["Normal"],
        fontSize=10,
        leading=14,
        spaceAfter=2,
        alignment=1,
    )
    date_info_style = ParagraphStyle(
        name="DateInfo",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.grey,
        italic=True,
        alignment=1,
    )

    right_stack = [
        Paragraph(f"<b>{BUSINESS_NAME}</b>", business_name_style),
        Paragraph(BUSINESS_ADDR, contact_info_style),
        Paragraph(P_O_BOX, contact_info_style),
        Paragraph(f"Phone: {BUSINESS_PHONE}", contact_info_style),
        Paragraph(f"Email: {BUSINESS_EMAIL}", contact_info_style),
        Paragraph(f"Report Period: {date_from or '...'} to {date_to or '...'}", date_info_style),
        Paragraph(f"Generated: {datetime.now(TZ_KE).strftime('%Y-%m-%d %H:%M:%S')}", date_info_style),
    ]

    if os.path.exists(_logo_path()):
        logo_cell = RLImage(_logo_path(), width=90, height=70)
    else:
        logo_cell = Paragraph("<b>Logo missing</b>", styles["Normal"])

    tbl = Table([[logo_cell, right_stack]], colWidths=[100, 380], hAlign="LEFT")
    tbl.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("LEFTPADDING", (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("LINEBELOW", (0,0), (-1,-1), 1, colors.HexColor("#0B5394")),
    ]))
    return tbl


# ---------------- PDF export (replace your route with this) ----------------
@retail_bp.route("/retail-sales/export-items.pdf", methods=["GET"])
@jwt_required()
def export_sales_items_pdf():
    q = _sales_items_query()
    q, date_from, date_to = _apply_filters_items_q(q)
    rows = q.all()
    if not rows:
        return jsonify({"ok": False, "error": "No sales found for the given filters"}), 404

    total_sales = 0.0
    total_cogs_sales = 0.0  # sales-side COGS from items
    bottle_totals = defaultdict(lambda: {"cartons": 0, "value": 0.0})

    items_header = [
        "Sale ID","Receipt #","Date","Customer","Method",
        "Bottle Size","Cartons","Unit Price","Line Total","COGS Total"
    ]
    items_rows = []
    for r in rows:
        items_rows.append([
            r.sale_id, r.receipt_number,
            iso_ke(r.date)[:10] if r.date else "",
            r.customer_name or "", r.payment_method or "",
            r.bottle_size_label or "", int(r.quantity_cartons or 0),
            f"{float(r.unit_price_carton or 0):.2f}",
            f"{float(r.line_total or 0):.2f}",
            f"{float((r.cogs_total or 0)):.2f}",
        ])
        total_sales += float(r.line_total or 0)
        total_cogs_sales += float(r.cogs_total or 0)
        key = r.bottle_size_label or "Unknown"
        bottle_totals[key]["cartons"] += int(r.quantity_cartons or 0)
        bottle_totals[key]["value"] += float(r.line_total or 0)

    # Combine COGS: items + purchases (Expense.category='cogs') for the KE date window
    cogs_purchases = _sum_cogs_expenses(date_from, date_to, include_deleted=False)
    total_cogs_combined = float(total_cogs_sales) + float(cogs_purchases)

    gross_profit = total_sales - total_cogs_combined
    gross_profit_margin = (gross_profit / total_sales * 100) if total_sales else 0.0

    # Other (non-COGS) expenses only
    non_cogs_q = db.session.query(func.coalesce(func.sum(Expense.amount), 0.0))\
                           .filter(Expense.is_deleted == False)\
                           .filter(or_(Expense.category == None, func.lower(Expense.category) != "cogs"))
    if date_from:
        non_cogs_q = non_cogs_q.filter(Expense.date >= date_from)
    if date_to:
        non_cogs_q = non_cogs_q.filter(Expense.date <= date_to)
    total_other_expenses = float(non_cogs_q.scalar() or 0.0)

    net_profit = gross_profit - total_other_expenses
    profit_color = (
        colors.red if net_profit < 0
        else (colors.orange if (total_sales > 0 and (net_profit / total_sales) < 0.2) else colors.green)
    )

    bar_path, pie_path = _chart_paths_from_bottle_totals(bottle_totals)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4)
    styles = getSampleStyleSheet()
    elems = []

    # Header
    elems.append(_header_table(styles, date_from, date_to))
    elems.append(Spacer(1, 8))
    elems.append(Paragraph("<b>Sales Report</b>", styles["Title"]))
    elems.append(Spacer(1, 6))

    # Detail table
    table = Table([items_header] + items_rows, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#e6e6e6")),
        ("GRID",(0,0),(-1,-1), 0.25, colors.grey),
        ("ALIGN",(0,0),(-1,-1), "CENTER"),
        ("FONTNAME",(0,0),(-1,0), "Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1), 8),
        ("BOTTOMPADDING",(0,0),(-1,0), 6),
    ]))
    elems.append(table)
    elems.append(Spacer(1, 10))

    # Totals by size
    totals_header = ["Bottle Size","Cartons","Total Value"]
    totals_rows = [[k, v["cartons"], f"{v['value']:.2f}"]
                   for k,v in sorted(bottle_totals.items(), key=lambda kv: (-kv[1]["cartons"], -kv[1]["value"]))]

    totals_tbl = Table([totals_header] + totals_rows, repeatRows=1)
    totals_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#e6e6e6")),
        ("GRID",(0,0),(-1,-1), 0.25, colors.grey),
        ("ALIGN",(0,0),(-1,-1), "CENTER"),
        ("FONTNAME",(0,0),(-1,0), "Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1), 9),
    ]))
    elems.append(Paragraph("<b>Totals by Bottle Size</b>", styles["Heading3"]))
    elems.append(totals_tbl)
    elems.append(Spacer(1, 8))

    # Financial summary (with breakdown)
    summary_header = ["Metric", "Amount (KES)"]
    summary_rows = [
        ["Total Sales", f"{total_sales:,.2f}"],
        ["COGS (Sales)", f"{total_cogs_sales:,.2f}"],
        ["COGS (Purchases)", f"{cogs_purchases:,.2f}"],
        ["COGS (Total)", f"{total_cogs_combined:,.2f}"],
        ["Gross Profit", f"{gross_profit:,.2f} ({gross_profit_margin:.2f}%)"],
        ["Other Expenses (ex-COGS)", f"{total_other_expenses:,.2f}"],
        ["Net Profit", f"{net_profit:,.2f}"],
    ]
    net_row_idx = len(summary_rows)  # header=0, rows start at 1, net=last
    summary_tbl = Table([summary_header] + summary_rows, repeatRows=1)
    summary_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#e6e6e6")),
        ("GRID",(0,0),(-1,-1), 0.25, colors.grey),
        ("ALIGN",(0,0),(-1,-1), "CENTER"),
        ("FONTNAME",(0,0),(-1,0), "Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1), 9),
        ("TEXTCOLOR", (0,net_row_idx), (-1,net_row_idx), profit_color),
        ("FONTNAME", (0,net_row_idx), (-1,net_row_idx), "Helvetica-Bold"),
    ]))
    elems.append(Paragraph("<b>Financial Summary</b>", styles["Heading3"]))
    elems.append(summary_tbl)
    elems.append(Spacer(1, 8))

    # Grand total
    grand_tbl = Table([["Grand Total Sales", f"{total_sales:,.2f}"]])
    grand_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#f2f2f2")),
        ("GRID",(0,0),(-1,-1), 0.25, colors.grey),
        ("ALIGN",(0,0),(-1,-1), "CENTER"),
        ("FONTNAME",(0,0),(-1,0), "Helvetica-Bold"),
    ]))
    elems.append(grand_tbl)
    elems.append(Spacer(1, 8))

    # Charts
    if os.path.exists(bar_path):
        elems.append(Paragraph("<b>Cartons per Bottle Size</b>", styles["Heading3"]))
        elems.append(RLImage(bar_path, width=420, height=260))
    if os.path.exists(pie_path):
        elems.append(Spacer(1, 6))
        elems.append(Paragraph("<b>Revenue Share per Bottle Size</b>", styles["Heading3"]))
        elems.append(RLImage(pie_path, width=420, height=260))

    # Build PDF
    doc.build(elems)
    buf.seek(0)
    for p in (bar_path, pie_path):
        with suppress(Exception):
            os.unlink(p)

    return send_file(
        buf,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"sales-report-{datetime.now(TZ_KE).strftime('%Y%m%d-%H%M%S')}.pdf",
    )



# -------------- Receipt view --------------
@retail_bp.route("/retail-sales/<int:sale_id>/receipt", methods=["GET"])
@jwt_required()
def retail_receipt(sale_id):
    sale = db.session.get(RetailSale, sale_id)
    if not sale:
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404

    header = {
        "business_name": "Salam Purified Water",
        "address": "Industrial Area, Nairobi",
        "phone": "+254-700-000000",
        "footer_note": "Thank you for your purchase!",
    }

    items = []
    for it in sale.items:
        label = it.bottle_size.label if it.bottle_size else None
        upc = _units_per_carton(label)
        bottles = (upc or 0) * int(it.quantity or 0)
        items.append({
            "bottle_size_id": it.bottle_size_id,
            "bottle_size_label": label,
            "quantity_cartons": int(it.quantity or 0),
            "units_per_carton": upc,
            "bottles": bottles if upc else None,
            "unit_price_carton": float(it.unit_price or 0),
            "line_total": float(it.total_price or 0),
        })
    pays = [_to_payment_dict(p) for p in (sale.payments or [])]

    subtotal = float(sale.total_amount or 0)
    paid = float(sale.paid_amount or 0)
    balance = float(sale.balance_due or max(0.0, subtotal - paid))
    change = max(0.0, paid - subtotal)

    def pad(s, n): return (s or "")[:n].ljust(n)
    lines = [
        pad(header["business_name"], 32),
        pad(header["address"], 32),
        pad(header["phone"], 32),
        "-"*32,
        f"Receipt: {sale.receipt_number}",
        f"Date: {iso_ke(sale.date) or ''}",
        f"Customer: {sale.customer_name or '-'}",
        f"Type: {sale.sale_type}",
        "-"*32,
    ]
    for it in items:
        lines.append(f"{it['bottle_size_label'] or 'Size'} x{it['quantity_cartons']}")
        lines.append(f" @ {it['unit_price_carton']:.2f}  = {it['line_total']:.2f}")
    lines += [
        "-"*32,
        f"TOTAL: {subtotal:.2f}",
        f"PAID : {paid:.2f}",
        f"CHANGE: {change:.2f}",
        f"BALANCE: {balance:.2f}",
        "-"*32,
        pad(header["footer_note"], 32),
    ]
    print_text = "\n".join(lines)

    return jsonify({
        "ok": True,
        "data": {
            "header": header,
            "sale": {
                "id": sale.id,
                "receipt_number": sale.receipt_number,
                "date": iso_ke(sale.date),
                "customer_name": sale.customer_name,
                "sale_type": sale.sale_type,
                "is_deleted": sale.is_deleted,
            },
            "items": items,
            "payments": pays,
            "totals": {
                "subtotal": subtotal, "paid": paid, "change": change, "balance_due": balance
            },
            "print_text": print_text
        }
    }), 200

# ---------------- Send payment email ----------------
@retail_bp.route("/send-payment-email", methods=["POST"])
@jwt_required()
def send_payment_email():
    data = request.get_json() or {}
    retail_sale_id = data.get("retail_sale_id")
    amount = data.get("amount")
    balance = data.get("balance")

    if not retail_sale_id:
        return jsonify({"ok": False, "error": "Sale ID required"}), 400

    sale = db.session.get(RetailSale, retail_sale_id)
    if not sale or not sale.customer or not sale.customer.email:
        return jsonify({"ok": False, "error": "Customer email not found"}), 404

    ok = send_customer_payment_receipt(
        customer_name=sale.customer.name,
        customer_email=sale.customer.email,
        amount=amount,
        balance=balance,
        sale_id=sale.id
    )
    return jsonify({
        "ok": bool(ok),
        "message": "Email sent" if ok else None,
        "error": None if ok else "Failed to send email",
        "email_sent": bool(ok)
    }), 200

# ---------------- Expenses ----------------
def _to_expense_dict(expense):
    return {
        "id": expense.id,
        "amount": float(expense.amount or 0.0),
        "description": expense.description or "",
        "date": expense.date.isoformat() if expense.date else None,  # DATE only
        "payment_method": expense.payment_method or "Cash",
        "category": expense.category or "Other",
        "added_by": expense.added_by,
        "is_deleted": expense.is_deleted,
    }

@retail_bp.route("/expenses", methods=["POST"])
@jwt_required()
def create_expense():
    data = request.get_json(silent=True) or {}
    amount = _to_float_nonneg(data.get("amount"))
    if amount is None:
        return jsonify({"ok": False, "error": "amount is required and must be a positive number"}), 400
    description = (data.get("description") or "").strip()
    if not description:
        return jsonify({"ok": False, "error": "description is required"}), 400

    date_str = data.get("date")
    if date_str:
        dd = _parse_date(date_str)
        if dd is None:
            return jsonify({"ok": False, "error": "date must be YYYY-MM-DD"}), 400
        date_value = dd
    else:
        date_value = _today_ke()  # Nairobi day

    payment_method = (data.get("payment_method") or "Cash").strip()
    ALLOWED_PAY_METHODS = {"Cash", "M-Pesa", "Bank", "Other"}
    if payment_method not in ALLOWED_PAY_METHODS:
        return jsonify({"ok": False, "error": f"payment_method must be one of: {', '.join(sorted(ALLOWED_PAY_METHODS))}"}), 400

    category = (data.get("category") or "").strip() or None
    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 401

    try:
        expense = Expense(
            amount=amount,
            description=description,
            date=date_value,
            payment_method=payment_method,
            category=category,
            added_by=user.id,
        )
        db.session.add(expense)
        db.session.commit()
        return jsonify({"ok": True, "message": "Expense recorded", "data": _to_expense_dict(expense)}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

@retail_bp.route("/expenses", methods=["GET"])
@jwt_required()
def list_expenses():
    date_from = _parse_date(request.args.get("date_from"))
    date_to = _parse_date(request.args.get("date_to"))
    include_deleted = (request.args.get("include_deleted", "false").lower() == "true")

    q = db.session.query(Expense)
    if not include_deleted:
        q = q.filter(Expense.is_deleted == False)  # noqa: E712
    if date_from:
        q = q.filter(Expense.date >= date_from)
    if date_to:
        q = q.filter(Expense.date <= date_to)
    q = q.order_by(Expense.date.desc(), Expense.id.desc())
    rows = q.all()

    return jsonify({"ok": True, "data": [_to_expense_dict(e) for e in rows]}), 200

@retail_bp.route("/expenses/today", methods=["GET"])
@jwt_required()
def get_expenses_today():
    today = _today_ke()
    rows = db.session.query(Expense).filter(
        Expense.is_deleted == False,
        Expense.date == today
    ).order_by(Expense.id.desc()).all()
    return jsonify({"ok": True, "data": [_to_expense_dict(e) for e in rows]}), 200

# GET YESTERDAY expenses
@retail_bp.route("/expenses/yesterday", methods=["GET"])
@jwt_required()
def get_expenses_yesterday():
    yesterday = _today_ke() - timedelta(days=1)
    rows = db.session.query(Expense).filter(
        Expense.is_deleted == False,
        Expense.date == yesterday
    ).order_by(Expense.id.desc()).all()
    return jsonify({"ok": True, "data": [_to_expense_dict(e) for e in rows]}), 200

# GET expense last 7 days
@retail_bp.route("/expenses/last-7-days", methods=["GET"])
@jwt_required()
def get_expenses_last_7_days():
    today = _today_ke()
    start = today - timedelta(days=6)  # inclusive 7-day window
    rows = db.session.query(Expense).filter(
        Expense.is_deleted == False,
        Expense.date >= start,
        Expense.date <= today
    ).order_by(Expense.date.desc(), Expense.id.desc()).all()
    return jsonify({"ok": True, "data": [_to_expense_dict(e) for e in rows]}), 200

@retail_bp.route("/expenses/<int:expense_id>", methods=["GET"])
@jwt_required()
def get_expense(expense_id):
    expense = db.session.get(Expense, expense_id)
    if not expense:
        return jsonify({"ok": False, "error": "Expense not found"}), 404
    return jsonify({"ok": True, "data": _to_expense_dict(expense)}), 200

@retail_bp.route("/expenses/<int:expense_id>", methods=["PUT", "PATCH"])
@jwt_required()
def update_expense(expense_id):
    expense = db.session.get(Expense, expense_id)
    if not expense:
        return jsonify({"ok": False, "error": "Expense not found"}), 404

    data = request.get_json(force=True, silent=True) or {}
    ALLOWED_PAY_METHODS = {"Cash", "M-Pesa", "Bank", "Other"}

    try:
        if "amount" in data:
            new_amt = _to_float_nonneg(data.get("amount"))
            if new_amt is None:
                return jsonify({"ok": False, "error": "amount must be a positive number"}), 400
            expense.amount = new_amt

        if "description" in data:
            desc = (data.get("description") or "").strip()
            if not desc:
                return jsonify({"ok": False, "error": "description is required"}), 400
            expense.description = desc

        if "payment_method" in data:
            pm = (data.get("payment_method") or "Cash").strip()
            if pm not in ALLOWED_PAY_METHODS:
                return jsonify({"ok": False, "error": f"payment_method must be one of: {', '.join(sorted(ALLOWED_PAY_METHODS))}"}), 400
            expense.payment_method = pm

        if "category" in data:
            expense.category = (data.get("category") or "").strip() or None

        if "date" in data:
            dd = _parse_date(data.get("date"))
            if dd is None:
                return jsonify({"ok": False, "error": "date must be YYYY-MM-DD"}), 400
            expense.date = dd

        db.session.commit()
        return jsonify({"ok": True, "message": "Expense updated", "data": _to_expense_dict(expense)}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400

@retail_bp.route("/expenses/<int:expense_id>", methods=["DELETE"])
@jwt_required()
def delete_expense(expense_id):
    expense = db.session.get(Expense, expense_id)
    if not expense:
        return jsonify({"ok": False, "error": "Expense not found"}), 404
    try:
        db.session.delete(expense)
        db.session.commit()
        return jsonify({"ok": True, "message": "Expense deleted"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400
    finally:
        db.session.close()

# add near _current_user()
def _user_display_name(u):
    if not u:
        return "—"
    for attr in ("full_name", "name", "username", "email"):
        v = getattr(u, attr, None)
        if v:
            return str(v)
    return f"User #{getattr(u, 'id', '—')}"

@retail_bp.route("/retail-sales/<int:sale_id>/print", methods=["POST"])
@jwt_required()
def print_sale_receipt(sale_id):
    sale = db.session.get(RetailSale, sale_id)
    if not sale or getattr(sale, "is_deleted", False):
        return jsonify({"ok": False, "error": "RetailSale not found"}), 404

    try:
        uid = get_jwt_identity()
        user = db.session.get(User, uid) if uid else None
    except Exception:
        user = None

    if user:
        sale.served_by_name = _user_display_name(user)
        with suppress(Exception):
            sale.added_by_user = user

    body = request.get_json(silent=True) or {}
    copies = max(1, int(body.get("copies") or 1))

    if body.get("copy_label"):
        sale._copy_label = str(body["copy_label"]).strip()
    if body.get("is_reprint") is not None:
        sale._is_reprint = bool(body.get("is_reprint"))

    if body.get("payment_method"):
        sale.payment_method = str(body["payment_method"]).strip()
    if body.get("payment_ref"):
        sale.payment_ref = str(body["payment_ref"]).strip()

    if not getattr(sale, "payment_method", None):
        latest = None
        try:
            latest = max(
                (getattr(sale, "payments", None) or []),
                key=lambda p: ((getattr(p, "date", None) or datetime.min.replace(tzinfo=UTC)), getattr(p, "id", 0))
            )
        except ValueError:
            latest = None
        if latest and getattr(latest, "payment_method", None):
            sale.payment_method = latest.payment_method

    try:
        print_sale_80mm(sale, copies=copies)
        return jsonify({"ok": True, "message": f"Receipt sent (x{copies})"}), 200
    except Exception as e:
        current_app.logger.error("Print failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 502


@retail_bp.post("/cogs")
@jwt_required()
def create_cogs():
    """
    Body:
      { "amount": number, "description": "Water purchase", "date": "YYYY-MM-DD", "payment_method": "Cash" }
    Optional:
      { "bottle_size_id": 2, "unit_cost_carton": 320 }  # if you want to update the default COGS for that size
    """
    data = request.get_json(silent=True) or {}

    amount = _to_float_nonneg(data.get("amount"))
    if amount is None:
        return jsonify({"ok": False, "error": "amount is required and must be a positive number"}), 400

    description = (data.get("description") or "COGS purchase").strip()
    if not description:
        return jsonify({"ok": False, "error": "description is required"}), 400

    # Date defaults to today's Nairobi calendar day
    dd = _parse_date(data.get("date")) or _today_ke()

    payment_method = (data.get("payment_method") or "Cash").strip()
    ALLOWED_PAY_METHODS = {"Cash", "M-Pesa", "Bank", "Other"}
    if payment_method not in ALLOWED_PAY_METHODS:
        return jsonify({"ok": False, "error": f"payment_method must be one of: {', '.join(sorted(ALLOWED_PAY_METHODS))}"}), 400

    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 401

    try:
        # Save as an Expense but locked to category='cogs'
        exp = Expense(
            amount=amount,
            description=description,
            date=dd,
            payment_method=payment_method,
            category="cogs",
            added_by=user.id,
        )
        db.session.add(exp)

        # (Optional) If they provided bottle_size_id + unit_cost_carton, update default COGS for that size
        bsid = data.get("bottle_size_id")
        unit_cost_carton = data.get("unit_cost_carton")
        if bsid is not None and unit_cost_carton is not None:
            try:
                from models import BottleSize
                bs = db.session.get(BottleSize, int(bsid))
                if not bs:
                    return jsonify({"ok": False, "error": f"BottleSize {bsid} not found"}), 400
                bs.cogs_cost_carton = float(unit_cost_carton)
            except (ValueError, TypeError):
                return jsonify({"ok": False, "error": "bottle_size_id must be int and unit_cost_carton numeric"}), 400

        db.session.commit()
        return jsonify({"ok": True, "message": "COGS recorded", "data": _to_expense_dict(exp)}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400


