# views/auth.py  (or routes/auth.py)
from __future__ import annotations
from flask import Blueprint, request, jsonify
from werkzeug.security import check_password_hash, generate_password_hash
from flask_jwt_extended import (
    create_access_token, jwt_required, get_jwt, get_jwt_identity
)
from flask_jwt_extended.exceptions import NoAuthorizationError, JWTExtendedException
from models import db, User, TokenBlockList, DeviceApprovalRequest
from app import limiter

from datetime import datetime, timedelta
from random import randint
import os
import re

auth_bp = Blueprint("auth_bp", __name__)

# ----------------------------- helpers ----------------------------------------
def _now_utc() -> datetime:
    # keep tz-naive UTC for consistency with your existing columns
    return datetime.utcnow()

def _is_overall_admin(u: User | None) -> bool:
    return bool(u and u.role == "admin" and (u.admin_level or "").lower() == "overall")

def _real_client_ip() -> str:
    """
    If you run behind Nginx/Cloudflare and use ProxyFix correctly, request.remote_addr
    should be the real client IP. If you *must* trust XFF, set TRUST_XFF=true in env.
    """
    if os.getenv("TRUST_XFF", "false").lower() == "true":
        xff = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        if xff:
            return xff
    ip = request.remote_addr or ""
    if not ip:
        route = request.access_route
        if route:
            ip = route[0]
    return ip

# ---- User-Agent normalization (stable fingerprint) ---------------------------
_UA_TOKEN_KEEP = re.compile(r"(windows|linux|android|iphone|ipad|macintosh|mac os x|x11|arm|x86_64|wow64|intel|chrome|edg|edge|safari|firefox|crios|fxios)")

def _normalize_ua(ua_raw: str | None) -> str:
    """
    Convert noisy UA into a stable, version-agnostic fingerprint.
    Example:
      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML...) Chrome/131.0.0.0 Safari/537.36'
      -> 'linux x86_64 chrome safari'
    """
    if not ua_raw:
        return "unknown"
    ua = ua_raw.lower()
    # replace delimiters with spaces
    ua = re.sub(r"[()/_;:,]+", " ", ua)
    # drop version numbers
    ua = re.sub(r"\b\d+(\.\d+)*\b", " ", ua)
    # keep only known platform/engine tokens
    tokens = [t for t in ua.split() if _UA_TOKEN_KEEP.fullmatch(t)]
    # dedupe in order
    seen = set()
    kept = []
    for t in tokens:
        if t not in seen:
            kept.append(t)
            seen.add(t)
    return " ".join(kept) or "unknown"

def _strict_ip_required() -> bool:
    return os.getenv("STRICT_DEVICE_IP", "false").lower() == "true"


# ------------------------------ auth routes -----------------------------------

# ✅ Login with rate-limit + no user enumeration + *stable* device approval flow
@auth_bp.route("/login", methods=["POST"])
@limiter.limit("5 per minute")
def login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    try:
        user = User.query.filter_by(email=email).first()

        # generic on purpose: avoid user enumeration
        if not user or not user.is_active:
            return jsonify({"error": "Invalid email or password"}), 401

        if not user.password_hash or not check_password_hash(user.password_hash, password):
            return jsonify({"error": "Invalid email or password"}), 401

        # Collect client signals
        user_ip_raw = _real_client_ip()
        ua_raw = request.headers.get("User-Agent") or ""
        ua_norm = _normalize_ua(ua_raw)

        # restricted roles must use device approval
        is_restricted = (
            user.role in ["cashier", "server"] or
            (user.role == "admin" and (user.admin_level or "normal").lower() != "overall")
        )

        if is_restricted:
            # MATCH RULE:
            # - UA must match normalized value we stored at approval time
            # - IP match is optional (default OFF); enable via STRICT_DEVICE_IP=true
            ua_ok = bool(user.device_approved and (user.allowed_user_agent or "") == ua_norm)
            ip_ok = True if not _strict_ip_required() else (user.allowed_ip or "") == user_ip_raw
            device_matches = ua_ok and ip_ok

            if not device_matches:
                # Close any previous unresolved requests for this user (avoid duplicates)
                DeviceApprovalRequest.query.filter_by(
                    user_id=user.id,
                    is_resolved=False
                ).update({DeviceApprovalRequest.is_resolved: True})
                db.session.commit()

                # Create a fresh approval request (store normalized UA for stability)
                code = str(randint(100000, 999999))
                req = DeviceApprovalRequest(
                    user_id=user.id,
                    ip_address=user_ip_raw,
                    user_agent=ua_norm,          # <-- store normalized UA
                    secret_code=code,
                    expires_at=_now_utc() + timedelta(minutes=15)  # short TTL
                )
                db.session.add(req)
                db.session.commit()

                return jsonify({
                    "error": "DEVICE_PENDING",
                    "message": "New device detected. Awaiting admin approval.",
                    "ip": user_ip_raw,
                    "user_agent": ua_norm,
                    "request_id": req.id,
                    "mode": "manual"
                }), 403

        # token lifetime: currently 8 hours
        access_token = create_access_token(identity=str(user.id), expires_delta=timedelta(hours=8))

        return jsonify({
            "message": "Login successful",
            "token": access_token,
            "user": {
                "id": user.id,
                "name": user.name,
                "role": user.role,
                "admin_level": user.admin_level
            }
        }), 200

    except Exception as e:
        print("🔥 LOGIN ERROR:", str(e))
        db.session.rollback()
        return jsonify({"error": "Internal server error"}), 500


@auth_bp.route("/logout", methods=["POST"])
@limiter.limit("30 per minute")
@jwt_required()
def logout():
    try:
        jti = get_jwt().get("jti")
        if jti:
            db.session.add(TokenBlockList(jti=jti, created_at=_now_utc()))
            db.session.commit()
        return jsonify({"message": "Successfully logged out"}), 200
    except Exception:
        db.session.rollback()
        return jsonify({"error": "Logout failed"}), 500


# ✅ Secure password reset: self (with current pw) or overall admin
@auth_bp.route("/reset-password/<int:user_id>", methods=["PUT"])
@limiter.limit("5 per minute")
@jwt_required()
def reset_password(user_id: int):
    actor_id_raw = get_jwt_identity()
    try:
        actor_id = int(actor_id_raw)
    except Exception:
        return jsonify({"error": "Unauthorized"}), 401

    actor = db.session.get(User, actor_id)
    if not actor:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    new_password = data.get("new_password")
    current_password = data.get("current_password")

    if not new_password:
        return jsonify({"error": "New password is required"}), 400

    user = db.session.get(User, user_id)
    if not user:
        return jsonify({"error": "User not found"}), 404

    is_overall = _is_overall_admin(actor)
    is_self = (actor_id == user_id)

    if not (is_self or is_overall):
        return jsonify({"error": "Forbidden"}), 403

    # if self-reset, require current password
    if is_self:
        if not current_password or not check_password_hash(user.password_hash or "", current_password):
            return jsonify({"error": "Current password is incorrect"}), 400

    try:
        user.password_hash = generate_password_hash(new_password)
        db.session.commit()
        return jsonify({"message": f"Password for {user.name} reset successfully"}), 200
    except Exception:
        db.session.rollback()
        return jsonify({"error": "Failed to reset password"}), 500


@auth_bp.route("/current-user", methods=["GET"])
@jwt_required()
def get_current_user():
    try:
        user_id_raw = get_jwt_identity()
        user = db.session.get(User, int(user_id_raw))
        if not user:
            return jsonify({"error": "User not found"}), 404

        return jsonify({
            "id": user.id,
            "name": user.name,
            "email": user.email,
            "role": user.role,
            "admin_level": user.admin_level,
            "phone": user.phone,
            "image": user.image
        }), 200
    except Exception:
        return jsonify({"error": "Failed to fetch user details"}), 500


# ✅ Manual approval — overall admin only
@auth_bp.route("/approve-device", methods=["POST"])
@limiter.limit("10 per minute")
@jwt_required()
def approve_device():
    try:
        admin_id_raw = get_jwt_identity()
        admin = db.session.get(User, int(admin_id_raw)) if admin_id_raw is not None else None

        if not _is_overall_admin(admin):
            return jsonify({"error": "Only overall admins can approve devices"}), 403

        data = request.get_json(silent=True) or {}
        req_id = data.get("request_id")
        if not req_id:
            return jsonify({"error": "request_id is required"}), 400

        req = db.session.get(DeviceApprovalRequest, int(req_id))
        if not req or req.is_resolved:
            return jsonify({"error": "Request not found or already handled"}), 404

        # enforce expiry even for manual approvals
        if req.expires_at and _now_utc() > req.expires_at:
            return jsonify({"error": "This device request has expired"}), 400

        user = req.user
        # store normalized UA from the request; IP optional depending on STRICT_DEVICE_IP
        user.allowed_user_agent = req.user_agent            # normalized value
        user.allowed_ip = req.ip_address if _strict_ip_required() else None
        user.device_approved = True
        req.is_resolved = True

        db.session.commit()
        return jsonify({"message": f"{user.name}'s device approved manually"}), 200

    except JWTExtendedException as e:
        print("❌ JWT ERROR:", str(e))
        return jsonify({"error": "JWT failed", "details": str(e)}), 401
    except Exception as e:
        print("🔥 Unknown error:", str(e))
        db.session.rollback()
        return jsonify({"error": "Internal error", "details": str(e)}), 500


# (Optional) Code-based approval still available if you ever need it
@auth_bp.route("/approve-by-code", methods=["POST"])
@limiter.limit("10 per minute")
@jwt_required()
def approve_by_code():
    try:
        admin_id_raw = get_jwt_identity()
        admin = db.session.get(User, int(admin_id_raw)) if admin_id_raw is not None else None

        if not _is_overall_admin(admin):
            return jsonify({"error": "Only overall admins can approve devices"}), 403

        data = request.get_json(silent=True) or {}
        code = str(data.get("code") or "").strip()
        req_id = data.get("request_id")

        if not code or not req_id:
            return jsonify({"error": "request_id and code are required"}), 400

        req = db.session.get(DeviceApprovalRequest, int(req_id))
        if not req or req.is_resolved or req.secret_code != code or _now_utc() > (req.expires_at or _now_utc()):
            return jsonify({"error": "Invalid or expired code"}), 404

        user = req.user
        user.allowed_user_agent = req.user_agent            # normalized value
        user.allowed_ip = req.ip_address if _strict_ip_required() else None
        user.device_approved = True
        req.is_resolved = True
        db.session.commit()

        return jsonify({"message": f"{user.name}'s device approved"}), 200

    except JWTExtendedException as e:
        print("❌ JWT ERROR:", str(e))
        return jsonify({"error": "JWT failed", "details": str(e)}), 401
    except Exception as e:
        print("🔥 Unknown error:", str(e))
        db.session.rollback()
        return jsonify({"error": "Internal error", "details": str(e)}), 500


# ✅ List pending device requests — overall admin only
@auth_bp.route("/device-requests", methods=["GET"])
@limiter.limit("10 per minute")
@jwt_required()
def get_device_requests():
    current_user = db.session.get(User, int(get_jwt_identity()))
    if not _is_overall_admin(current_user):
        return jsonify({"error": "Unauthorized"}), 403

    requests = DeviceApprovalRequest.query.filter_by(is_resolved=False)\
        .order_by(DeviceApprovalRequest.created_at.desc()).all()
    return jsonify([
        {
            "id": r.id,
            "user": r.user.name,
            "user_id": r.user.id,
            "ip": r.ip_address,
            "user_agent": r.user_agent,   # already normalized
            "created_at": (r.created_at or _now_utc()).isoformat()
        } for r in requests
    ]), 200


# ✅ Reject a specific device request — overall admin only
@auth_bp.route("/device-requests/<int:req_id>", methods=["DELETE"])
@limiter.limit("10 per minute")
@jwt_required()
def reject_device(req_id: int):
    admin = db.session.get(User, int(get_jwt_identity()))
    if not _is_overall_admin(admin):
        return jsonify({"error": "Only overall admins can reject"}), 403

    req = db.session.get(DeviceApprovalRequest, req_id)
    if not req:
        return jsonify({"ok": False, "error": "Request not found"}), 404

    try:
        req.is_resolved = True  # mark as handled/rejected
        db.session.commit()
        return jsonify({"ok": True, "message": "Device request rejected"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400


# ✅ Summary of resolved devices — overall admin only
@auth_bp.route("/device-summary", methods=["GET"])
@limiter.limit("20 per minute")
@jwt_required()
def device_summary():
    current_user = db.session.get(User, int(get_jwt_identity()))
    if not _is_overall_admin(current_user):
        return jsonify({"error": "Unauthorized"}), 403

    approved_devices = db.session.query(
        DeviceApprovalRequest.user_id,
        DeviceApprovalRequest.ip_address,
        DeviceApprovalRequest.user_agent
    ).filter(DeviceApprovalRequest.is_resolved.is_(True)).all()

    summary: dict[str, list[str]] = {}
    for user_id, ip, agent in approved_devices:
        key = str(user_id)
        label = f"{ip or '—'} | {agent}"
        summary.setdefault(key, []).append(label)

    return jsonify(summary), 200


# ✅ Bulk delete from device summary — overall admin only
@auth_bp.route("/device-summary", methods=["DELETE"])
@limiter.limit("5 per minute")
@jwt_required()
def delete_device_summary():
    admin = db.session.get(User, int(get_jwt_identity()))
    if not _is_overall_admin(admin):
        return jsonify({"error": "Unauthorized"}), 403

    body = request.get_json(silent=True) or {}

    user_ids        = body.get("user_ids")
    ids             = body.get("ids")
    ip              = body.get("ip")
    user_agent      = body.get("user_agent")
    older_than_days = body.get("older_than_days")
    only_resolved   = body.get("only_resolved", True)
    delete_all      = body.get("all", False)

    q = db.session.query(DeviceApprovalRequest)
    if only_resolved:
        q = q.filter(DeviceApprovalRequest.is_resolved.is_(True))

    if user_ids:
        q = q.filter(DeviceApprovalRequest.user_id.in_(list({int(u) for u in user_ids})))
    if ids:
        q = q.filter(DeviceApprovalRequest.id.in_(list({int(i) for i in ids})))
    if ip:
        q = q.filter(DeviceApprovalRequest.ip_address == str(ip))
    if user_agent:
        q = q.filter(DeviceApprovalRequest.user_agent == str(user_agent))
    if older_than_days is not None:
        try:
            cutoff = _now_utc() - timedelta(days=int(older_than_days))
            q = q.filter(DeviceApprovalRequest.created_at < cutoff)
        except Exception:
            return jsonify({"error": "older_than_days must be an integer"}), 400

    if not any([user_ids, ids, ip, user_agent, older_than_days, delete_all]):
        return jsonify({
            "error": "Refusing to delete without filters. "
                     "Pass user_ids / ids / ip / user_agent / older_than_days "
                     "or set 'all': true (use with care)."
        }), 400

    if delete_all and not any([user_ids, ids, ip, user_agent, older_than_days]):
        q = db.session.query(DeviceApprovalRequest)
        if only_resolved:
            q = q.filter(DeviceApprovalRequest.is_resolved.is_(True))

    deleted = q.delete(synchronize_session=False)
    db.session.commit()
    return jsonify({"ok": True, "deleted": int(deleted)}), 200


# --------------------------- error handlers -----------------------------------

@auth_bp.app_errorhandler(422)
def handle_unprocessable_entity(e):
    return jsonify({"error": "Missing or invalid JSON payload"}), 422

@auth_bp.app_errorhandler(NoAuthorizationError)
def handle_no_auth_error(e):
    return jsonify({"error": "Unauthorized request"}), 401



