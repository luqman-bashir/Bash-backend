# app.py
from flask import Flask, jsonify
from flask_migrate import Migrate
from flask_jwt_extended import JWTManager
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix
from sqlalchemy import text
from datetime import timedelta, datetime, timezone
import os

from models import db, User, TokenBlockList

app = Flask(__name__)

# --- Core config ---
DATABASE_URI = os.getenv("DATABASE_URI")
if not DATABASE_URI:
    raise RuntimeError("DATABASE_URI is missing")

app.config.update(
    SQLALCHEMY_DATABASE_URI=DATABASE_URI,
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    SECRET_KEY=os.getenv("SECRET_KEY", "change-me"),            # set in prod
    JWT_SECRET_KEY=os.getenv("JWT_SECRET_KEY", "change-me-too"),# set in prod
    JWT_ACCESS_TOKEN_EXPIRES=timedelta(hours=8),                # 8h session
    PROPAGATE_EXCEPTIONS=True,
)

# Trust upstream proxy for client IP / scheme / host
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

# --- Extensions ---
db.init_app(app)
migrate = Migrate(app, db)

jwt = JWTManager(app)

# Limiter (use Redis in prod: e.g. redis://redis:6379/0)
limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=os.getenv("RATELIMIT_STORAGE_URI", "memory://"),
)
limiter.init_app(app)

# --- JWT blocklist & error handlers ------------------------------------------
@jwt.token_in_blocklist_loader
def is_token_revoked(jwt_header, jwt_payload):
    """
    Return True to reject the token.
    - If token JTI is in TokenBlockList (explicit logout)
    - If the user does not exist or is deactivated (is_active = False)
    - (Optional) If you later add user.token_invalid_after to hard-kill older tokens
    """
    try:
        # 1) Explicit logout via TokenBlockList
        jti = jwt_payload.get("jti")
        if jti and db.session.query(TokenBlockList.id).filter_by(jti=jti).first():
            return True

        # 2) Deactivated / missing user → reject immediately
        sub = jwt_payload.get("sub")
        user_id = int(sub) if sub is not None else None
        user = db.session.get(User, user_id) if user_id is not None else None
        if not user or not user.is_active:
            return True

        # 3) Optional "token_invalid_after" support
        # If you add a DateTime column on User and set it when you deactivate/reactivate,
        # this will invalidate any token issued before that timestamp.
        if getattr(user, "token_invalid_after", None):
            iat = jwt_payload.get("iat")
            if iat:
                issued_at = datetime.fromtimestamp(iat, tz=timezone.utc)
                if issued_at < user.token_invalid_after.replace(tzinfo=timezone.utc):
                    return True

        return False
    except Exception:
        # Fail closed on unexpected errors
        return True

@jwt.revoked_token_loader
def revoked_token_callback(jwt_header, jwt_payload):
    return jsonify({"error": "Token has been revoked"}), 401

@jwt.expired_token_loader
def expired_token_callback(jwt_header, jwt_payload):
    return jsonify({"error": "Token has expired"}), 401

@jwt.invalid_token_loader
def invalid_token_callback(reason):
    return jsonify({"error": "Invalid token", "details": reason}), 401

@jwt.unauthorized_loader
def missing_token_callback(reason):
    return jsonify({"error": "Missing or invalid authorization", "details": reason}), 401

# --- Blueprints (import after limiter/JWT are set) ----------------------------
from views.auth import auth_bp
from views.users import user_bp
from views.packaging import packaging_bp
from views.sale import retail_bp

app.register_blueprint(auth_bp)
app.register_blueprint(user_bp)
app.register_blueprint(packaging_bp)
app.register_blueprint(retail_bp)

# --- Misc routes --------------------------------------------------------------
@app.get("/")
def home():
    return "App is running"

@app.get("/health")
def health():
    db.session.execute(text("SELECT 1"))
    return {"status": "ok", "db": "up"}, 200

# --- Entrypoint ---------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=os.getenv("FLASK_DEBUG", "false").lower() == "true")
