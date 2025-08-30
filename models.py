from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, date
import uuid
from sqlalchemy import event , select

db = SQLAlchemy()

# -----------------------------
# 1. User (with roles)
# -----------------------------
class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(120), unique=True, index=True, nullable=True)
    phone = db.Column(db.String(20), nullable=True)
    password_hash = db.Column(db.String(255), nullable=True)
    role = db.Column(db.String(20), nullable=False, default='cashier')
    admin_level = db.Column(db.String(20), nullable=True, default='normal')
    image = db.Column(db.String(255), nullable=True)
    allowed_ip = db.Column(db.String(100), nullable=True)
    allowed_user_agent = db.Column(db.String(255), nullable=True)
    device_approved = db.Column(db.Boolean, default=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    device_requests = db.relationship(
        "DeviceApprovalRequest",
        back_populates="user",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<User {self.id} {self.name} ({self.role})>"

    @property
    def can_login(self) -> bool:
        return bool(self.is_active)



# -----------------------------
# 2. Bottle Size
# -----------------------------
class BottleSize(db.Model):
    __tablename__ = 'bottle_size'
    id = db.Column(db.Integer, primary_key=True)
    label = db.Column(db.String(20), nullable=False)   # '500ml', '1.5L', '5L'
    selling_price = db.Column(db.Float, nullable=False)

    # ✅ NEW: your manual all-in cost per carton (bottles, caps, labels, KRA, labor, etc.)
    cost_price_carton = db.Column(db.Float, default=0.0)

    stock_balance = db.relationship(
        "StockBalance",
        back_populates="bottle_size",
        uselist=False,
        cascade="all, delete"
    )


    def __repr__(self):
        return f"<BottleSize {self.label} @ {self.selling_price}>"


# -----------------------------
# 3. Packaging Entry
# -----------------------------
class PackagingEntry(db.Model):
    __tablename__ = 'packaging_entry'

    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, default=date.today)
    bottle_size_id = db.Column(db.Integer, db.ForeignKey('bottle_size.id'))
    quantity = db.Column(db.Integer)
    added_by = db.Column(db.Integer, db.ForeignKey('users.id'))
    is_deleted = db.Column(db.Boolean, default=False)

    bottle_size = db.relationship("BottleSize")
    added_by_user = db.relationship("User")

    def __repr__(self):
        return f"<PackagingEntry size={self.bottle_size_id} qty={self.quantity} date={self.date}>"

class Customer(db.Model):
    __tablename__ = 'customers'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20), nullable=True)
    email = db.Column(db.String(120), nullable=True)  # For sending payment notifications
    notes = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    sales = db.relationship('RetailSale', back_populates='customer')

    def __repr__(self):
        return f"<Customer {self.name}>"


# -----------------------------
# 2. Retail Sale
# -----------------------------
class RetailSale(db.Model):
    __tablename__ = 'retail_sale'

    id = db.Column(db.Integer, primary_key=True)
    sale_type = db.Column(db.String(20), nullable=False, default="normal")  # normal, credit, dispatch
    receipt_number = db.Column(db.String(50), unique=True, default=lambda: f"R-{uuid.uuid4().hex[:8].upper()}")
    date = db.Column(db.DateTime, default=datetime.utcnow)

    # Optional link to customer (required for credit sales)
    customer_id = db.Column(db.Integer, db.ForeignKey('customers.id'), nullable=True)
    customer_name = db.Column(db.String(100), nullable=True)  # For walk-in customers

    total_amount = db.Column(db.Float, default=0.0)
    paid_amount = db.Column(db.Float, default=0.0)
    balance_due = db.Column(db.Float, default=0.0)
    payment_method = db.Column(db.String(50), nullable=True)  # Cash, M-PESA, Bank, etc.
    notes = db.Column(db.String(255), nullable=True)
    is_deleted = db.Column(db.Boolean, default=False)

    added_by = db.Column(db.Integer, db.ForeignKey('users.id'))

    # Relationships
    customer = db.relationship('Customer', back_populates='sales')
    items = db.relationship('RetailSaleItem', back_populates='sale', cascade='all, delete-orphan')
    payments = db.relationship('CustomerPayment', back_populates='retail_sale', cascade='all, delete-orphan')

    def __repr__(self):
        return f"<RetailSale {self.receipt_number} KES{self.total_amount}>"


# -----------------------------
# 3. Retail Sale Item
# -----------------------------
class RetailSaleItem(db.Model):
    __tablename__ = 'retail_sale_item'

    id = db.Column(db.Integer, primary_key=True)
    sale_id = db.Column(db.Integer, db.ForeignKey('retail_sale.id'), nullable=False)
    bottle_size_id = db.Column(db.Integer, db.ForeignKey('bottle_size.id'), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)   # cartons
    unit_price = db.Column(db.Float, nullable=False)
    total_price = db.Column(db.Float, nullable=False)

    # ✅ NEW: COGS snapshot at time of sale (per carton & line total)
    cogs_unit_price = db.Column(db.Float)              # pulled from BottleSize.cost_price_carton
    cogs_total = db.Column(db.Float)                   # cogs_unit_price * quantity

    sale = db.relationship('RetailSale', back_populates='items')
    bottle_size = db.relationship('BottleSize')


    def __repr__(self):
        return f"<RetailSaleItem sale={self.sale_id} size={self.bottle_size_id} qty={self.quantity}>"
    
# models.py (bottom of file, after all model classes)

# --- COGS auto-fill events (runs on ORM insert/update) ---
@event.listens_for(RetailSaleItem, "before_insert")
def _fill_cogs_on_insert(mapper, connection, target: RetailSaleItem):
    # If not provided by the route, take current BottleSize cost
    if target.cogs_unit_price is None:
        bs_cost = connection.execute(
            select(BottleSize.cost_price_carton).where(BottleSize.id == target.bottle_size_id)
        ).scalar()
        target.cogs_unit_price = float(bs_cost or 0.0)

    # Ensure cogs_total = quantity * cogs_unit_price
    if target.cogs_total is None:
        qty = int(target.quantity or 0)
        target.cogs_total = float(target.cogs_unit_price or 0.0) * qty

@event.listens_for(RetailSaleItem, "before_update")
def _recalc_cogs_on_update(mapper, connection, target: RetailSaleItem):
    qty = int(target.quantity or 0)
    cup = float(target.cogs_unit_price or 0.0)
    target.cogs_total = cup * qty



# -----------------------------
# 4. Customer Payment
# -----------------------------
class CustomerPayment(db.Model):
    __tablename__ = 'customer_payment'

    id = db.Column(db.Integer, primary_key=True)
    retail_sale_id = db.Column(db.Integer, db.ForeignKey('retail_sale.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    payment_method = db.Column(db.String(50), nullable=True)  # Cash, M-PESA, Bank
    date = db.Column(db.DateTime, default=datetime.utcnow)
    added_by = db.Column(db.Integer, db.ForeignKey('users.id'))

    retail_sale = db.relationship('RetailSale', back_populates='payments')
    added_by_user = db.relationship('User')

    def __repr__(self):
        return f"<CustomerPayment amt={self.amount} method={self.payment_method}>"




# -----------------------------
# 9. Expense
# -----------------------------

class Expense(db.Model):
    __tablename__ = 'expense'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, default=date.today, index=True)  # ✅ index for fast range filters
    description = db.Column(db.String(255))
    category = db.Column(db.String(40))        # ✅ optional: 'Fuel', 'Power', 'Salaries', ...
    amount = db.Column(db.Float)
    payment_method = db.Column(db.String(30))  # optional
    added_by = db.Column(db.Integer, db.ForeignKey('users.id'))
    is_deleted = db.Column(db.Boolean, default=False)

    added_by_user = db.relationship('User')


    def __repr__(self):
        return f"<Expense {self.description} KES{self.amount} on {self.date}>"


# -----------------------------
# 10. Device Approval Request
# -----------------------------
class DeviceApprovalRequest(db.Model):
    __tablename__ = 'device_approval_requests'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id', ondelete="CASCADE"), nullable=False)
    ip_address = db.Column(db.String(100), nullable=False)
    user_agent = db.Column(db.String(255), nullable=False)
    secret_code = db.Column(db.String(6), nullable=False)
    is_resolved = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    expires_at = db.Column(db.DateTime, nullable=False)

    user = db.relationship('User', back_populates='device_requests')

    def __repr__(self):
        return f"<DeviceApprovalRequest user={self.user_id} resolved={self.is_resolved}>"


# 


# -----------------------------
# 13. Stock Balance
# -----------------------------
class StockBalance(db.Model):
    __tablename__ = 'stock_balance'

    id = db.Column(db.Integer, primary_key=True)
    bottle_size_id = db.Column(db.Integer, db.ForeignKey('bottle_size.id'), unique=True)
    quantity_available = db.Column(db.Integer, default=0)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)

    bottle_size = db.relationship('BottleSize', back_populates='stock_balance')

    def __repr__(self):
        return f"<StockBalance size={self.bottle_size_id} qty={self.quantity_available}>"




class TokenBlockList(db.Model):
    __tablename__ = 'token_blocklist'
    id = db.Column(db.Integer, primary_key=True)
    jti = db.Column(db.String(36), nullable=False, unique=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<TokenBlocklist id={self.id}, jti={self.jti}, created_at={self.created_at}>"


# bottom of models.py
db.Index('ix_retail_sale_date', RetailSale.date)
db.Index('ix_rsi_sale_id', RetailSaleItem.sale_id)
db.Index('ix_rsi_bottle_size_id', RetailSaleItem.bottle_size_id)




    

    # FUTURE MODELS


# -----------------------------
# 6. Dispatch Record
# -----------------------------
# class DispatchRecord(db.Model):
#     __tablename__ = 'dispatch_record'

#     id = db.Column(db.Integer, primary_key=True)
#     date = db.Column(db.Date, default=date.today)
#     destination = db.Column(db.String(100))
#     driver_name = db.Column(db.String(100))
#     vehicle_number = db.Column(db.String(50))
#     total_value = db.Column(db.Float, default=0.0)
#     amount_paid = db.Column(db.Float, default=0.0)
#     payment_method = db.Column(db.String(50))
#     status = db.Column(db.String(20), default='pending')  # pending, finalized
#     added_by = db.Column(db.Integer, db.ForeignKey('users.id'))
#     is_deleted = db.Column(db.Boolean, default=False)

#     items = db.relationship(
#         'DispatchItem',
#         back_populates='dispatch',
#         cascade='all, delete-orphan'
#     )

#     def __repr__(self):
#         return f"<DispatchRecord {self.id} {self.destination} {self.date}>"


# # -----------------------------
# # 7. Dispatch Items
# # -----------------------------
# class DispatchItem(db.Model):
#     __tablename__ = 'dispatch_item'

#     id = db.Column(db.Integer, primary_key=True)
#     dispatch_id = db.Column(db.Integer, db.ForeignKey('dispatch_record.id'))
#     bottle_size_id = db.Column(db.Integer, db.ForeignKey('bottle_size.id'))
#     quantity = db.Column(db.Integer)
#     is_deleted = db.Column(db.Boolean, default=False)

#     dispatch = db.relationship('DispatchRecord', back_populates='items')
#     bottle_size = db.relationship('BottleSize')

#     def __repr__(self):
#         return f"<DispatchItem dispatch={self.dispatch_id} size={self.bottle_size_id} qty={self.quantity}>"


# # -----------------------------
# # 8. Dispatch Return
# # -----------------------------
# class DispatchReturn(db.Model):
#     __tablename__ = 'dispatch_return'

#     id = db.Column(db.Integer, primary_key=True)
#     dispatch_id = db.Column(db.Integer, db.ForeignKey('dispatch_record.id'))
#     bottle_size_id = db.Column(db.Integer, db.ForeignKey('bottle_size.id'))
#     quantity_sent = db.Column(db.Integer)
#     quantity_sold = db.Column(db.Integer)
#     quantity_returned = db.Column(db.Integer)
#     remarks = db.Column(db.String(255))
#     recorded_by = db.Column(db.Integer, db.ForeignKey('users.id'))
#     timestamp = db.Column(db.DateTime, default=datetime.utcnow)
#     is_deleted = db.Column(db.Boolean, default=False)

#     dispatch = db.relationship('DispatchRecord')
#     bottle_size = db.relationship('BottleSize')
#     recorder = db.relationship('User')

#     def __repr__(self):
#         return f"<DispatchReturn dispatch={self.dispatch_id} size={self.bottle_size_id}>"


# # -----------------------------
# # # 11. Customer Order
# # # -----------------------------
# # class CustomerOrder(db.Model):
# #     __tablename__ = 'customer_order'

# #     id = db.Column(db.Integer, primary_key=True)
# #     customer_id = db.Column(db.Integer, db.ForeignKey('users.id'))
# #     date = db.Column(db.DateTime, default=datetime.utcnow)
# #     status = db.Column(db.String(20), default='pending')  # pending, approved, dispatched
# #     total_value = db.Column(db.Float, default=0.0)
# #     is_deleted = db.Column(db.Boolean, default=False)

# #     items = db.relationship(
# #         'OrderItem',
# #         back_populates='order',
# #         cascade='all, delete-orphan'
# #     )
# #     payments = db.relationship(
# #         'CustomerPayment',
# #         back_populates='customer_order'
# #     )

# #     customer = db.relationship('User')

# #     def __repr__(self):
# #         return f"<CustomerOrder {self.id} user={self.customer_id} {self.status}>"


# # -----------------------------
# # 12. Customer Order Items
# # -----------------------------
# class OrderItem(db.Model):
#     __tablename__ = 'order_item'

#     id = db.Column(db.Integer, primary_key=True)
#     order_id = db.Column(db.Integer, db.ForeignKey('customer_order.id'))
#     bottle_size_id = db.Column(db.Integer, db.ForeignKey('bottle_size.id'))
#     quantity = db.Column(db.Integer)

#     order = db.relationship('CustomerOrder', back_populates='items')
#     bottle_size = db.relationship('BottleSize')

#     def __repr__(self):
#         return f"<OrderItem order={self.order_id} size={self.bottle_size_id} qty={self.quantity}>"