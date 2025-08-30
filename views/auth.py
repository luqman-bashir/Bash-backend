from flask import Blueprint, request, jsonify
from werkzeug.security import check_password_hash, generate_password_hash
from flask_jwt_extended import (
    create_access_token, jwt_required, get_jwt, get_jwt_identity
)
from models import db, User, TokenBlockList, DeviceApprovalRequest
from datetime import datetime, timedelta
from random import randint
from utils.email_alert import send_admin_approval_code
from app import limiter
from sqlalchemy import func, distinct
from flask_jwt_extended.exceptions import NoAuthorizationError, JWTExtendedException



auth_bp = Blueprint("auth_bp", __name__)



# ✅ Login with added rate-limiting, always send new code for unapproved device
@auth_bp.route("/login", methods=["POST"])
@limiter.limit("5 per minute")  # limit login attempts
def login():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Invalid JSON payload'}), 400

    email = (data.get('email') or "").strip().lower()
    password = data.get('password') or ""

    if not email or not password:
        return jsonify({'error': 'Email and password are required'}), 400

    try:
        user = User.query.filter_by(email=email).first()
        if not user:
            return jsonify({'error': 'User not found'}), 404

        # ✅ Use is_active instead of can_login
        if not user.is_active:
            return jsonify({'error': 'User not authorized to login'}), 403

        # ✅ Handle missing password_hash safely
        if not user.password_hash or not check_password_hash(user.password_hash, password):
            return jsonify({'error': 'Invalid password'}), 401

        # Try to get real client IP if you're behind a reverse proxy
        forwarded_for = request.headers.get("X-Forwarded-For")
        user_ip = (forwarded_for.split(",")[0].strip() if forwarded_for else request.remote_addr) or ""
        user_agent = request.headers.get("User-Agent") or ""

        # ✅ Same restriction logic you had
        is_restricted = (
            user.role in ["cashier", "server"] or
            (user.role == "admin" and (user.admin_level or "normal") != "overall")
        )

        if is_restricted:
            device_matches = (
                user.device_approved and
                (user.allowed_ip == user_ip) and
                (user.allowed_user_agent == user_agent)
            )
            if not device_matches:
                # Expire old pending requests for this device
                DeviceApprovalRequest.query.filter_by(
                    user_id=user.id,
                    ip_address=user_ip,
                    user_agent=user_agent,
                    is_resolved=False
                ).update({DeviceApprovalRequest.is_resolved: True})
                db.session.commit()  # commit the update

                # Create & email a fresh approval code
                code = str(randint(100000, 999999))
                new_request = DeviceApprovalRequest(
                    user_id=user.id,
                    ip_address=user_ip,
                    user_agent=user_agent,
                    secret_code=code,
                    expires_at=datetime.utcnow() + timedelta(minutes=15)
                )
                db.session.add(new_request)
                db.session.commit()
                send_admin_approval_code(user, user_ip, user_agent, code)

                return jsonify({
                    'error': 'New device detected. Awaiting admin approval.',
                    'ip': user_ip,
                    'user_agent': user_agent
                }), 403

        access_token = create_access_token(identity=str(user.id), expires_delta=timedelta(hours=8))
        return jsonify({
            'message': 'Login successful',
            'token': access_token,
            'user': {
                'id': user.id,
                'name': user.name,
                'role': user.role,
                'admin_level': user.admin_level
            }
        }), 200

    except Exception as e:
        print("🔥 LOGIN ERROR:", str(e))
        db.session.rollback()
        return jsonify({'error': 'Internal server error'}), 500



@auth_bp.route("/logout", methods=["POST"])
@jwt_required()
def logout():
    try:
        jti = get_jwt()["jti"]
        db.session.add(TokenBlockList(jti=jti, created_at=datetime.utcnow()))
        db.session.commit()
        return jsonify({"message": "Successfully logged out"}), 200
    except Exception:
        db.session.rollback()
        return jsonify({'error': 'Logout failed'}), 500


@auth_bp.route('/reset-password/<int:user_id>', methods=['PUT'])
def reset_password(user_id):
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Invalid JSON payload'}), 400

    new_password = data.get('password')
    if not new_password:
        return jsonify({'error': 'Password is required'}), 400

    user = User.query.get_or_404(user_id)
    try:
        user.password_hash = generate_password_hash(new_password)
        db.session.commit()
        return jsonify({'message': f'Password for {user.name} reset successfully'}), 200
    except Exception:
        db.session.rollback()
        return jsonify({'error': 'Failed to reset password'}), 500


@auth_bp.route("/current-user", methods=["GET"])
@jwt_required()
def get_current_user():
    try:
        user_id = get_jwt_identity()
        user = User.query.get_or_404(user_id)

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
        return jsonify({'error': 'Failed to fetch user details'}), 500


@auth_bp.route("/approve-by-code", methods=["POST"])
@jwt_required()
def approve_by_code():
    try:
        # who is calling?
        admin_id = get_jwt_identity()
        admin = User.query.get(int(admin_id)) if admin_id is not None else None

        # only overall admin can approve
        if not admin or admin.role != "admin" or (admin.admin_level or "").lower() != "overall":
            return jsonify({"error": "Only overall admins can approve devices"}), 403

        data = request.get_json(silent=True) or {}
        code = str(data.get("code") or "").strip()
        if not code:
            return jsonify({"error": "Code required"}), 400

        req = DeviceApprovalRequest.query.filter_by(secret_code=code, is_resolved=False).first()
        if not req or datetime.utcnow() > req.expires_at:
            return jsonify({"error": "Invalid or expired code"}), 404

        user = req.user
        user.allowed_ip = req.ip_address
        user.allowed_user_agent = req.user_agent
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



@auth_bp.route("/device-requests", methods=["GET"])
@jwt_required()
def get_device_requests():
    current_user = User.query.get(get_jwt_identity())
    if current_user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    requests = DeviceApprovalRequest.query.filter_by(is_resolved=False).all()
    return jsonify([
        {
            "id": r.id,
            "user": r.user.name,
            "user_id": r.user.id,
            "ip": r.ip_address,
            "user_agent": r.user_agent,
            "created_at": r.created_at.isoformat()
        } for r in requests
    ]), 200
# routes/device_requests.py
@auth_bp.route("/device-requests/<int:req_id>", methods=["DELETE"])
@jwt_required()
def reject_device(req_id):
    req = db.session.get(DeviceApprovalRequest, req_id)
    if not req:
        return jsonify({"ok": False, "error": "Request not found"}), 404

    # also fetch user
    user = db.session.get(User, req.user_id)

    try:
        db.session.delete(req)
        if user and user.role != "overall":  # protect overall admin
            db.session.delete(user)  # remove user as well
        db.session.commit()
        return jsonify({"ok": True, "message": "Device request and user deleted"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400



@auth_bp.route("/device-summary", methods=["GET"])
@jwt_required()
def device_summary():
    identity = get_jwt_identity()
    current_user = User.query.get(identity)

    if not current_user:
        return jsonify({"error": "User not found"}), 404

    if current_user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    approved_devices = db.session.query(
        DeviceApprovalRequest.user_id,
        DeviceApprovalRequest.ip_address,
        DeviceApprovalRequest.user_agent
    ).filter(DeviceApprovalRequest.is_resolved == True).all()

    summary = {}
    for user_id, ip, agent in approved_devices:
        key = str(user_id)
        label = f"{ip} | {agent}"
        summary.setdefault(key, []).append(label)

    return jsonify(summary)



# DELETE DEVICE SUMMARY
@auth_bp.route("/device-summary", methods=["DELETE"])
@jwt_required()
def delete_device_summary():
    # ── auth ─────────────────────────────────────────────────────────────────
    uid = get_jwt_identity()
    admin = db.session.get(User, uid)
    if not admin:
        return jsonify({"error": "User not found"}), 404
    if getattr(admin, "role", None) != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    # ── filters ──────────────────────────────────────────────────────────────
    body = request.get_json(silent=True) or {}

    user_ids         = body.get("user_ids")        # e.g. [2,3]
    ids              = body.get("ids")             # specific DeviceApprovalRequest ids
    ip               = body.get("ip")              # e.g. "172.23.0.3"
    user_agent       = body.get("user_agent")      # full UA string match
    older_than_days  = body.get("older_than_days") # e.g. 30
    only_resolved    = body.get("only_resolved", True)
    delete_all       = body.get("all", False)      # force delete everything (dangerous)

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
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(older_than_days))
            q = q.filter(DeviceApprovalRequest.created_at < cutoff)
        except Exception:
            return jsonify({"error": "older_than_days must be an integer"}), 400

    # Require at least one filter unless 'all' is explicitly set
    if not any([user_ids, ids, ip, user_agent, older_than_days, delete_all]):
        return jsonify({
            "error": "Refusing to delete without filters. "
                     "Pass user_ids / ids / ip / user_agent / older_than_days "
                     "or set 'all': true (use with care)."
        }), 400

    # If 'all' is set, drop all rows that match only the 'only_resolved' flag
    # (i.e., ignore other filters if none were given)
    if delete_all and not any([user_ids, ids, ip, user_agent, older_than_days]):
        q = db.session.query(DeviceApprovalRequest)
        if only_resolved:
            q = q.filter(DeviceApprovalRequest.is_resolved.is_(True))

    deleted = q.delete(synchronize_session=False)
    db.session.commit()

    return jsonify({"ok": True, "deleted": int(deleted)}), 200



@auth_bp.app_errorhandler(422)
def handle_unprocessable_entity(e):
    return jsonify({"error": "Missing or invalid JSON payload"}), 422


@auth_bp.app_errorhandler(NoAuthorizationError)
def handle_no_auth_error(e):
    return jsonify({"error": "Unauthorized request"}), 401
