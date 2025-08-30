from flask import Blueprint, request, jsonify
from werkzeug.security import generate_password_hash
from models import db, User
from datetime import datetime

user_bp = Blueprint("user_bp", __name__)

# ---------------------------
# Create a new user
# ---------------------------
@user_bp.route("/users", methods=["POST"])
def create_user():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON payload"}), 400

    name = data.get("name")
    email = data.get("email")
    password = data.get("password")
    role = data.get("role", "cashier")
    admin_level = data.get("admin_level", "normal")
    phone = data.get("phone")
    image = data.get("image")

    if not name or not role:
        return jsonify({"error": "Name and role are required"}), 400

    if email and User.query.filter_by(email=email).first():
        return jsonify({"error": "Email already exists"}), 409

    user = User(
        name=name,
        email=email,
        password_hash=generate_password_hash(password) if password else None,
        role=role,
        admin_level=admin_level,
        phone=phone,
        image=image
    )

    db.session.add(user)
    db.session.commit()

    return jsonify({"message": "User created successfully", "id": user.id}), 201


# ---------------------------
# Get all users
# ---------------------------
@user_bp.route("/users", methods=["GET"])
def get_users():
    show_all = request.args.get("all", "false").lower() == "true"
    query = User.query
    if not show_all:
        query = query.filter_by(is_active=True)
    users = query.all()

    return jsonify([
        {
            "id": u.id,
            "name": u.name,
            "email": u.email,
            "role": u.role,
            "admin_level": u.admin_level,
            "phone": u.phone,
            "image": u.image,
            "device_approved": u.device_approved,
            "is_active": u.is_active,
            "created_at": u.created_at.isoformat()
        }
        for u in users
    ]), 200


# ---------------------------
# Get single user
# ---------------------------
@user_bp.route("/users/<int:user_id>", methods=["GET"])
def get_user(user_id):
    user = User.query.get_or_404(user_id)
    return jsonify({
        "id": user.id,
        "name": user.name,
        "email": user.email,
        "role": user.role,
        "admin_level": user.admin_level,
        "phone": user.phone,
        "image": user.image,
        "allowed_ip": user.allowed_ip,
        "allowed_user_agent": user.allowed_user_agent,
        "device_approved": user.device_approved,
        "is_active": user.is_active,
        "created_at": user.created_at.isoformat()
    }), 200


# ---------------------------
# Update a user
# ---------------------------
@user_bp.route("/users/<int:user_id>", methods=["PUT"])
def update_user(user_id):
    user = User.query.get_or_404(user_id)
    data = request.get_json(silent=True)

    user.name = data.get("name", user.name)
    user.email = data.get("email", user.email)
    if data.get("password"):
        user.password_hash = generate_password_hash(data["password"])
    user.role = data.get("role", user.role)
    user.admin_level = data.get("admin_level", user.admin_level)
    user.phone = data.get("phone", user.phone)
    user.image = data.get("image", user.image)
    user.allowed_ip = data.get("allowed_ip", user.allowed_ip)
    user.allowed_user_agent = data.get("allowed_user_agent", user.allowed_user_agent)
    if "device_approved" in data:
        user.device_approved = bool(data["device_approved"])
    if "is_active" in data:
        user.is_active = bool(data["is_active"])

    db.session.commit()
    return jsonify({"message": "User updated successfully"}), 200


# ---------------------------
# Soft delete (set inactive)
# ---------------------------
@user_bp.route("/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    user = User.query.get_or_404(user_id)
    user.is_active = False
    db.session.commit()
    return jsonify({"message": "User deactivated (soft delete)"}), 200

# DELETE USER
# @user_bp.route("/users/<int:user_id>", methods=["DELETE"])
# def hard_delete_user(user_id):
#     user = User.query.get_or_404(user_id)
#     db.session.delete(user)
#     db.session.commit()
#     return jsonify({"message": "User deleted permanently"}), 200


# ---------------------------
# Reactivate user
# ---------------------------
@user_bp.route("/users/<int:user_id>/reactivate", methods=["POST"])
def reactivate_user(user_id):
    user = User.query.get_or_404(user_id)
    user.is_active = True
    db.session.commit()
    return jsonify({"message": "User reactivated successfully"}), 200
