from flask import Flask
from flask_migrate import Migrate
from flask_jwt_extended import JWTManager
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from sqlalchemy import text
from models import db
import os
from flask_mail import Message  # ✅ add this
from email.utils import parseaddr  # ✅ add this
from extensions import mail  # only mail here


app = Flask(__name__)

# --- DB ---
DATABASE_URI = os.getenv("DATABASE_URI")
if not DATABASE_URI:
    raise RuntimeError("DATABASE_URI is missing")
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URI
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# --- Secrets ---
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-me")
app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY", "change-me-too")

app.config["MAIL_SERVER"] = os.getenv("MAIL_SERVER", "smtp.gmail.com")
app.config["MAIL_PORT"] = int(os.getenv("MAIL_PORT", "465"))
app.config["MAIL_USE_TLS"] = os.getenv("MAIL_USE_TLS", "False").lower() == "true"
app.config["MAIL_USE_SSL"] = os.getenv("MAIL_USE_SSL", "True").lower() == "true"
app.config["MAIL_USERNAME"] = os.getenv("MAIL_USERNAME", "")
app.config["MAIL_PASSWORD"] = os.getenv("MAIL_PASSWORD", "")
app.config["MAIL_DEFAULT_SENDER"] = os.getenv("MAIL_DEFAULT_SENDER", app.config["MAIL_USERNAME"])


if app.config.get("MAIL_USE_SSL") or app.config.get("MAIL_PORT") == 465:
    app.config["MAIL_USE_TLS"] = False

# --- Mail/JWT ---
mail.init_app(app)
db.init_app(app)
migrate = Migrate(app, db)
jwt = JWTManager(app)

# --- Rate limiter (choose this style; don't pass key_func twice) ---
limiter = Limiter(key_func=get_remote_address)
limiter.init_app(app)

# --- Import blueprints AFTER limiter exists to avoid circulars ---
from views.auth import auth_bp
from views.users import user_bp
from views.packaging import packaging_bp
from views.sale import retail_bp


app.register_blueprint(auth_bp)
app.register_blueprint(user_bp)
app.register_blueprint(packaging_bp)
app.register_blueprint(retail_bp)

@app.get("/")
def home():
    return "App is running"

@app.get("/health")
def health():
    db.session.execute(text("SELECT 1"))
    return {"status": "ok", "db": "up"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
