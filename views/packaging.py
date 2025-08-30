# routes/packaging.py
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime, date, timezone
from sqlalchemy import func, select
from models import db, PackagingEntry, BottleSize, StockBalance, User
from zoneinfo import ZoneInfo

packaging_bp = Blueprint("packaging_bp", __name__)

# ---- Time / TZ (store UTC, display Africa/Nairobi +03:00) --------------------
TZ_KE = ZoneInfo("Africa/Nairobi")
UTC = timezone.utc

def _now_utc() -> datetime:
    """Aware UTC now for DB DateTime fields."""
    return datetime.now(UTC)

def _today_ke() -> date:
    """Nairobi-local calendar date for DATE fields."""
    return datetime.now(TZ_KE).date()

def iso_ke(dt: datetime | None) -> str | None:
    """Serialize DB DateTime to ISO string in Africa/Nairobi (+03:00).
       If naive, assume stored UTC."""
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(TZ_KE).isoformat()

# --- Carton pack sizes (for derived bottles only) ---
PACK_SIZES = {
    "500ml": 24,
    "1.5L": 12,
    "5L": 4,
    "1.5l": 12,
    "5l": 4,
}

# ---------------- Helpers ----------------
def parse_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None

def current_user():
    uid = get_jwt_identity()
    return db.session.get(User, uid)

def entry_or_404(entry_id, include_deleted=False):
    entry = db.session.get(PackagingEntry, entry_id)
    if not entry or (not include_deleted and entry.is_deleted):
        return None, jsonify({"ok": False, "error": "PackagingEntry not found"}), 404
    return entry, None, None

def bottle_size_or_404(size_id):
    bs = db.session.get(BottleSize, size_id)
    if not bs:
        return None, jsonify({"ok": False, "error": "BottleSize not found"}), 404
    return bs, None, None

def pack_size_for_label(label):
    if not label:
        return None
    return PACK_SIZES.get(label) or PACK_SIZES.get(str(label).lower())

def _to_int_nonneg(val):
    try:
        iv = int(val)
    except (TypeError, ValueError):
        return None
    return iv if iv >= 0 else None

def _to_float_nonneg(val):
    try:
        fv = float(val)
    except (TypeError, ValueError):
        return None
    return fv if fv >= 0 else None

def to_bs_dict(bs: BottleSize):
    per = pack_size_for_label(bs.label)
    return {
        "id": bs.id,
        "label": bs.label,
        "selling_price": float(bs.selling_price) if bs.selling_price is not None else None,  # per carton
        "cost_price_carton": float(bs.cost_price_carton) if bs.cost_price_carton is not None else None,  # per carton
        "units_per_carton": per,
        "carton_label": f"{bs.label} x {per or '?'}",
    }

def to_entry_dict(e: PackagingEntry):
    label = getattr(e.bottle_size, "label", None)
    per_carton = pack_size_for_label(label) or 0
    cartons = int(e.quantity or 0)  # quantity == cartons
    bottles = cartons * per_carton
    return {
        "id": e.id,
        "date": e.date.isoformat() if e.date else None,
        "bottle_size_id": e.bottle_size_id,
        "bottle_size_label": label,
        "cartons": cartons,
        "bottles": bottles,
        "units_per_carton": per_carton,
        "added_by": e.added_by,
        "added_by_name": getattr(e.added_by_user, "name", None),
        "is_deleted": e.is_deleted,
    }

def get_or_create_stock_balance(size_id: int) -> StockBalance:
    sb = db.session.execute(
        select(StockBalance).where(StockBalance.bottle_size_id == size_id)
    ).scalar_one_or_none()
    if not sb:
        sb = StockBalance(
            bottle_size_id=size_id,
            quantity_available=0,
            updated_at=_now_utc(),
        )
        db.session.add(sb)
        db.session.flush()
    return sb

def adjust_stock(size_id: int, delta_cartons: int):
    """Increment (or decrement) stock balance in CARTONS for a bottle size."""
    sb = get_or_create_stock_balance(size_id)
    new_qty = int(sb.quantity_available or 0) + int(delta_cartons)
    if new_qty < 0:
        raise ValueError("Stock would go negative; operation aborted.")
    sb.quantity_available = new_qty
    sb.updated_at = _now_utc()

def bottle_size_in_use(size_id: int) -> bool:
    ref = db.session.query(PackagingEntry.id).filter_by(bottle_size_id=size_id, is_deleted=False).first()
    return ref is not None

# ================= Misc/Health ==============
@packaging_bp.route("/packaging/health", methods=["GET"])
def packaging_health():
    return jsonify({"ok": True, "message": "packaging routes live"}), 200

# =====================================================
# ================ BottleSize (no pagination) =========
# =====================================================

@packaging_bp.route("/bottle-sizes", methods=["POST"])
@jwt_required()
def create_bottle_size():
    data = request.get_json(silent=True) or {}
    label = (data.get("label") or "").strip()
    price = _to_float_nonneg(data.get("selling_price"))
    cost_price = _to_float_nonneg(data.get("cost_price_carton"))  # NEW: cost per carton

    if not label:
        return jsonify({"ok": False, "error": "label is required"}), 400
    if price is None:
        return jsonify({"ok": False, "error": "selling_price must be a number >= 0 (per carton)"}), 400
    if cost_price is None:
        cost_price = 0.0  # default if not provided

    exists = db.session.query(BottleSize.id).filter(BottleSize.label == label).first()
    if exists:
        return jsonify({"ok": False, "error": "A bottle size with this label already exists."}), 409

    bs = BottleSize(label=label, selling_price=price, cost_price_carton=cost_price)
    db.session.add(bs)
    db.session.commit()
    return jsonify({"ok": True, "message": "Created", "data": to_bs_dict(bs)}), 201

@packaging_bp.route("/bottle-sizes", methods=["GET"])
@jwt_required()
def list_bottle_sizes():
    stmt = select(BottleSize).order_by(BottleSize.label.asc())
    rows = db.session.execute(stmt).scalars().all()
    return jsonify({"ok": True, "data": [to_bs_dict(r) for r in rows]}), 200

@packaging_bp.route("/bottle-sizes/<int:size_id>", methods=["GET"])
@jwt_required()
def get_bottle_size(size_id):
    bs = db.session.get(BottleSize, size_id)
    if not bs:
        return jsonify({"ok": False, "error": "BottleSize not found"}), 404
    return jsonify({"ok": True, "data": to_bs_dict(bs)}), 200

@packaging_bp.route("/bottle-sizes/<int:size_id>", methods=["PUT", "PATCH"])
@jwt_required()
def update_bottle_size(size_id):
    """
    Partial update supported:
      { "selling_price": 320 } or { "label": "500ml" } or both.
      Also supports { "cost_price_carton": 220 }.
    """
    bs = db.session.get(BottleSize, size_id)
    if not bs:
        return jsonify({"ok": False, "error": "BottleSize not found"}), 404

    data = request.get_json(silent=True) or {}

    # --- Update label ---
    if "label" in data:
        new_label = (data.get("label") or "").strip()
        if not new_label:
            return jsonify({"ok": False, "error": "label cannot be empty"}), 400
        exists = db.session.query(BottleSize.id).filter(
            BottleSize.label == new_label, BottleSize.id != size_id
        ).first()
        if exists:
            return jsonify({"ok": False, "error": "A bottle size with this label already exists."}), 409
        bs.label = new_label

    # --- Update selling price ---
    if "selling_price" in data:
        price = _to_float_nonneg(data.get("selling_price"))
        if price is None:
            return jsonify({"ok": False, "error": "selling_price must be a number >= 0 (per carton)"}), 400
        bs.selling_price = price

    # --- Update cost price carton ---
    if "cost_price_carton" in data:
        cost_price = _to_float_nonneg(data.get("cost_price_carton"))
        if cost_price is None:
            return jsonify({"ok": False, "error": "cost_price_carton must be a number >= 0"}), 400
        bs.cost_price_carton = cost_price

    db.session.commit()
    return jsonify({"ok": True, "message": "Updated", "data": to_bs_dict(bs)}), 200

@packaging_bp.route("/bottle-sizes/<int:size_id>", methods=["DELETE"])
@jwt_required()
def delete_bottle_size(size_id):
    bs = db.session.get(BottleSize, size_id)
    if not bs:
        return jsonify({"ok": False, "error": "BottleSize not found"}), 404

    if bottle_size_in_use(size_id):
        return jsonify({"ok": False, "error": "Cannot delete: size is used by packaging entries."}), 409

    db.session.delete(bs)
    db.session.commit()
    return jsonify({"ok": True, "message": "Deleted"}), 200

# HARD DELETE (permanently)
@packaging_bp.route("/bottle-sizes/<int:size_id>/hard-delete", methods=["DELETE"])
@jwt_required()
def hard_delete_bottle_size(size_id):
    bs = db.session.get(BottleSize, size_id)
    if not bs:
        return jsonify({"ok": False, "error": "BottleSize not found"}), 404

    db.session.delete(bs)
    db.session.commit()
    return jsonify({"ok": True, "message": "Permanently deleted"}), 200

@packaging_bp.route("/bottle-sizes/options", methods=["GET"])
@jwt_required()
def bottle_size_options():
    stmt = select(BottleSize.id, BottleSize.label).order_by(BottleSize.label.asc())
    rows = db.session.execute(stmt).all()
    data = [{"id": rid, "label": lbl} for (rid, lbl) in rows]
    return jsonify({"ok": True, "data": data}), 200

# =====================================================
# ================ PackagingEntry (CARTONS) ===========
# =====================================================

@packaging_bp.route("/packaging", methods=["POST"])
@jwt_required()
def create_packaging():
    """
    Body:
      {
        "bottle_size_id": 1,
        "cartons": 10,
        "date": "YYYY-MM-DD"   # optional (defaults to today in Nairobi)
      }
    Effects:
      - Inserts PackagingEntry (quantity = cartons)
      - Increments StockBalance.quantity_available by +cartons for that size
    """
    data = request.get_json(silent=True) or {}
    bottle_size_id = data.get("bottle_size_id")
    cartons = _to_int_nonneg(data.get("cartons"))
    date_str = data.get("date")

    if bottle_size_id is None:
        return jsonify({"ok": False, "error": "bottle_size_id is required"}), 400
    bs, err, code = bottle_size_or_404(bottle_size_id)
    if err:
        return err, code

    if cartons is None:
        return jsonify({"ok": False, "error": "cartons is required and must be a non-negative integer"}), 400

    d = parse_date(date_str) if date_str else _today_ke()
    if date_str and d is None:
        return jsonify({"ok": False, "error": "date must be in YYYY-MM-DD format"}), 400

    user = current_user()
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 401

    try:
        entry = PackagingEntry(
            date=d,
            bottle_size_id=bs.id,
            quantity=cartons,  # quantity = cartons
            added_by=user.id,
            is_deleted=False,
        )
        db.session.add(entry)

        adjust_stock(bs.id, +cartons)
        db.session.commit()
    except ValueError as ve:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(ve)}), 400

    return jsonify({"ok": True, "message": "Created", "data": to_entry_dict(entry)}), 201

@packaging_bp.route("/packaging", methods=["GET"])
@jwt_required()
def list_packaging():
    """
    Query:
      page, per_page, bottle_size_id, date_from, date_to,
      include_deleted ('true'|'false'), order ('asc'|'desc' by date)
    """
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, max(1, int(request.args.get("per_page", 20))))
    bottle_size_id = request.args.get("bottle_size_id", type=int)
    date_from = parse_date(request.args.get("date_from"))
    date_to = parse_date(request.args.get("date_to"))
    include_deleted = request.args.get("include_deleted", "false").lower() == "true"
    order = request.args.get("order", "desc")

    stmt = select(PackagingEntry)
    if not include_deleted:
        stmt = stmt.where(PackagingEntry.is_deleted == False)  # noqa: E712
    if bottle_size_id:
        stmt = stmt.where(PackagingEntry.bottle_size_id == bottle_size_id)
    if date_from:
        stmt = stmt.where(PackagingEntry.date >= date_from)
    if date_to:
        stmt = stmt.where(PackagingEntry.date <= date_to)
    if order == "asc":
        stmt = stmt.order_by(PackagingEntry.date.asc(), PackagingEntry.id.asc())
    else:
        stmt = stmt.order_by(PackagingEntry.date.desc(), PackagingEntry.id.desc())

    paginated = db.paginate(stmt, page=page, per_page=per_page, error_out=False)
    items = [to_entry_dict(e) for e in paginated.items]
    return jsonify({
        "ok": True,
        "data": items,
        "pagination": {
            "page": paginated.page,
            "per_page": paginated.per_page,
            "total": paginated.total,
            "pages": paginated.pages,
            "has_next": paginated.has_next,
            "has_prev": paginated.has_prev,
            "next_page": paginated.next_num,
            "prev_page": paginated.prev_num,
        }
    }), 200

@packaging_bp.route("/packaging/<int:entry_id>", methods=["GET"])
@jwt_required()
def get_packaging(entry_id):
    include_deleted = request.args.get("include_deleted", "false").lower() == "true"
    entry, err, code = entry_or_404(entry_id, include_deleted)
    if err:
        return err, code

    bs = db.session.get(BottleSize, entry.bottle_size_id)
    per_carton = pack_size_for_label(bs.label) if bs else None

    return jsonify({
        "ok": True,
        "data": {
            "id": entry.id,
            "date": entry.date.isoformat() if entry.date else None,
            "bottle_size_id": entry.bottle_size_id,
            "bottle_size_label": bs.label if bs else None,
            "selling_price_per_carton": float(bs.selling_price) if bs and bs.selling_price is not None else None,
            "units_per_carton": per_carton,
            "cartons": int(entry.quantity or 0),
            "bottles": (int(entry.quantity or 0) * per_carton) if per_carton else None,
            "added_by": entry.added_by,
            "added_by_name": getattr(entry.added_by_user, "name", None),
            "is_deleted": entry.is_deleted
        }
    }), 200

@packaging_bp.route("/packaging/<int:entry_id>", methods=["PUT", "PATCH"])
@jwt_required()
def update_packaging(entry_id):
    """
    Allows changing:
      - bottle_size_id
      - cartons
      - date
    Adjusts StockBalance by the delta in cartons (and handles size changes).
    """
    entry, err, code = entry_or_404(entry_id)
    if err:
        return err, code

    data = request.get_json(silent=True) or {}
    old_size_id = entry.bottle_size_id
    old_cartons = int(entry.quantity or 0)

    # Track new values
    new_size_id = old_size_id
    new_cartons = old_cartons

    if "bottle_size_id" in data:
        bs_id = data.get("bottle_size_id")
        if bs_id is None:
            return jsonify({"ok": False, "error": "bottle_size_id cannot be null"}), 400
        bs, err, code = bottle_size_or_404(bs_id)
        if err:
            return err, code
        new_size_id = bs.id

    if "cartons" in data:
        cartons = _to_int_nonneg(data.get("cartons"))
        if cartons is None:
            return jsonify({"ok": False, "error": "cartons must be a non-negative integer"}), 400
        new_cartons = cartons

    if "date" in data:
        d = parse_date(data.get("date"))
        if d is None:
            return jsonify({"ok": False, "error": "date must be in YYYY-MM-DD format"}), 400
        entry.date = d

    try:
        if new_size_id != old_size_id:
            adjust_stock(old_size_id, -old_cartons)   # reverse old
            adjust_stock(new_size_id, +new_cartons)   # apply new
            entry.bottle_size_id = new_size_id
            entry.quantity = new_cartons
        else:
            delta = new_cartons - old_cartons
            if delta != 0:
                adjust_stock(new_size_id, delta)
            entry.quantity = new_cartons

        db.session.commit()
    except ValueError as ve:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(ve)}), 400

    return jsonify({"ok": True, "message": "Updated", "data": to_entry_dict(entry)}), 200

@packaging_bp.route("/packaging/<int:entry_id>", methods=["DELETE"])
@jwt_required()
def soft_delete_packaging(entry_id):
    """
    Soft delete: marks entry deleted and DECREMENTS stock by its cartons.
    """
    entry, err, code = entry_or_404(entry_id)
    if err:
        return err, code
    if entry.is_deleted:
        return jsonify({"ok": True, "message": "Already deleted"}), 200

    try:
        adjust_stock(entry.bottle_size_id, -int(entry.quantity or 0))
        entry.is_deleted = True
        db.session.commit()
    except ValueError as ve:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(ve)}), 400

    return jsonify({"ok": True, "message": "Deleted"}), 200



@packaging_bp.route("/packaging/<int:entry_id>/restore", methods=["POST"])
@jwt_required()
def restore_packaging(entry_id):
    """
    Restore: unmarks deleted and INCREMENTS stock by its cartons.
    """
    entry, err, code = entry_or_404(entry_id, include_deleted=True)
    if err:
        return err, code
    if not entry.is_deleted:
        return jsonify({"ok": True, "message": "Already active"}), 200

    try:
        adjust_stock(entry.bottle_size_id, +int(entry.quantity or 0))
        entry.is_deleted = False
        db.session.commit()
    except ValueError as ve:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(ve)}), 400

    return jsonify({"ok": True, "message": "Restored", "data": to_entry_dict(entry)}), 200

# --------- Live Stock View (cartons + derived bottles) ---------
@packaging_bp.route("/stock-balances", methods=["GET"])
@jwt_required()
def stock_balances():
    sizes = {s.id: s for s in db.session.execute(select(BottleSize)).scalars().all()}
    balances = db.session.execute(select(StockBalance)).scalars().all()

    data = []
    for sb in balances:
        bs = sizes.get(sb.bottle_size_id)
        label = bs.label if bs else None
        per = pack_size_for_label(label) or 0
        cartons = int(sb.quantity_available or 0)
        data.append({
            "bottle_size_id": sb.bottle_size_id,
            "label": label,
            "cartons_on_hand": cartons,
            "bottles_on_hand": cartons * per,
            "units_per_carton": per,
            "carton_price": float(bs.selling_price) if bs and bs.selling_price is not None else None,
            "updated_at": iso_ke(sb.updated_at) if sb.updated_at else None,
        })

    # Include sizes without a StockBalance row yet
    for sid, bs in sizes.items():
        if not any(row["bottle_size_id"] == sid for row in data):
            per = pack_size_for_label(bs.label) or 0
            data.append({
                "bottle_size_id": sid,
                "label": bs.label,
                "cartons_on_hand": 0,
                "bottles_on_hand": 0,
                "units_per_carton": per,
                "carton_price": float(bs.selling_price) if bs.selling_price is not None else None,
                "updated_at": None,
            })

    data.sort(key=lambda x: x["label"] or "")
    return jsonify({"ok": True, "data": data}), 200
