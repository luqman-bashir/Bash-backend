# utils/email_alert.py
from flask import current_app
from flask_mail import Message
from extensions import mail
from models import User, db
from flask_jwt_extended import get_jwt_identity
import os

# ---------- helpers ----------
def _current_user():
    """Get the currently logged-in user from JWT (returns None if unavailable)."""
    try:
        uid = get_jwt_identity()
        if not uid:
            return None
        return db.session.get(User, uid)
    except Exception:
        return None

def _send_email(subject: str, recipients: list[str], body: str, html: str | None = None,
                reply_to: str | None = None, sender: str | None = None) -> bool:
    """
    Centralized email sender. Returns True on success, False on any failure.
    Uses MAIL_DEFAULT_SENDER unless 'sender' override is provided.
    """
    if not recipients:
        return False

    try:
        msg = Message(subject=subject, recipients=recipients, body=body, sender=sender)
        if html:
            msg.html = html
        if reply_to:
            msg.reply_to = reply_to
        mail.send(msg)
        return True
    except Exception as e:
        print(f"❌ Email send failed: {e}")
        return False

# ---------- emails ----------
def send_admin_approval_code(user, ip, agent, code, request_id=None):
    """Send approval code email to all overall admins (layout preserved)."""
    print("📧 EMAIL FUNCTION TRIGGERED from send_admin_approval_code()")

    overall_admins = User.query.filter_by(
        role="admin", admin_level="overall", is_active=True
    ).all()
    to_list = [a.email for a in overall_admins if a.email]
    if not to_list:
        return False

    subject = "Approval Code: New Device Login"

    rid_line = f"🆔 Request ID: {request_id}\n" if request_id else ""

    body = f"""
Hello,

A login attempt from an unapproved device requires your authorization.

👤 User: {user.name} ({user.role})
📍 IP Address: {ip}
🖥️ User-Agent: {agent}

{rid_line}Approval Code: {code}

Enter this code in the admin dashboard to approve the login.

Thank you,
Overall Admin Team
""".strip()

    ok = True
    for to in to_list:
        if not _send_email(subject, [to], body):
            ok = False
            print(f"❌ Failed to send email to {to}")
        else:
            print(f"✅ Email sent to {to}")
    return ok


def send_credit_repayment_email(customer, sale, payment):
    """
    Email the customer when they make a payment on a CREDIT sale.
    Includes the current user's name/role in the signature.
    """
    if not customer or not customer.email:
        print("⚠️ Customer or customer email missing; skipping credit repayment email.")
        return False

    sender = _current_user()
    sender_name = sender.name if sender else "System"
    sender_role = (sender.role.capitalize() if sender and sender.role else "Staff")

    business = os.getenv("BUSINESS_NAME", "Your Company")

    subject = f"Payment Received — Receipt {sale.receipt_number or sale.id}"
    body = f"""
Hello {customer.name or 'Customer'},

We have received your payment for credit sale #{sale.id}.

💵 Amount Paid: {float(payment.amount or 0):.2f}
💳 Payment Method: {payment.payment_method or '-'}
📅 Date: {payment.date.strftime('%Y-%m-%d')}
💰 Remaining Balance: {float(sale.balance_due or 0):.2f}

Thank you for your payment.

Best regards,
{sender_name} ({sender_role})
{business}
""".strip()

    ok = _send_email(subject, [customer.email], body)
    print(f"{'✅' if ok else '❌'} Credit repayment email to {customer.email}")
    return ok


def send_customer_payment_receipt(customer_name: str, customer_email: str, amount, balance, sale_id: int):
    """
    Generic payment receipt (used by /send-payment-email route).
    """
    if not customer_email:
        print("⚠️ No customer_email provided; skipping.")
        return False

    business = os.getenv("BUSINESS_NAME", "Your Company")

    subject = f"Payment Receipt — Sale #{sale_id}"
    body = f"""
Hello {customer_name or 'Customer'},

We have received your payment for sale #{sale_id}.

💵 Amount Paid: {float(amount or 0):.2f}
💰 Remaining Balance: {float(balance or 0):.2f}

Thank you for your business.

Regards,
{business}
""".strip()

    ok = _send_email(subject, [customer_email], body)
    print(f"{'✅' if ok else '❌'} Payment receipt email to {customer_email}")
    return ok
