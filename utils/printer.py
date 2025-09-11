# utils/printer.py
import os
import socket
import ipaddress
from contextlib import suppress, closing
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from flask import current_app
from escpos.printer import Network
# --- quiet down python-escpos destructor/close noise --------------------------
from contextlib import suppress
try:
    import escpos.printer as _epr
    from escpos.escpos import Escpos as _Escpos

    # Wrap Network.close so shutdown/close errors don't bubble to stderr
    _orig_close = _epr.Network.close
    def _quiet_network_close(self, *a, **kw):
        try:
            return _orig_close(self, *a, **kw)
        except OSError:
            # already closed / bad fd — ignore
            return None
        except Exception:
            return None
    _epr.Network.close = _quiet_network_close

    # Wrap Escpos.__del__ so any late close errors are swallowed
    if hasattr(_Escpos, "__del__"):
        _orig_del = _Escpos.__del__
        def _quiet_del(self):
            with suppress(Exception):
                _orig_del(self)
        _Escpos.__del__ = _quiet_del
except Exception:
    # If library layout differs, just skip the patch
    pass
# ------------------------------------------------------------------------------

# ── Printer/env config ──────────────────────────────────────────────────────────
PRINTER_IP: str = os.getenv("PRINTER_IP", "192.168.150.165").strip()
PRINTER_PORT: int = int(os.getenv("PRINTER_PORT", "9100"))

# NEW: smart-resolver settings (all optional)
PRINTER_HOST: str = os.getenv("PRINTER_HOST", "").strip()                  # e.g. 'receipt-printer'
PRINTER_SUBNET: str = os.getenv("PRINTER_SUBNET", "192.168.150.0/24").strip()
PRINTER_CACHE_FILE: str = os.getenv("PRINTER_CACHE_FILE", "/tmp/receipt_printer_ip").strip()
PRINTER_DISCOVERY_TIMEOUT_MS: int = int(os.getenv("PRINTER_DISCOVERY_TIMEOUT_MS", "250"))
PRINTER_DISCOVERY_WORKERS: int = int(os.getenv("PRINTER_DISCOVERY_WORKERS", "80"))        # "192.168.0.101,192.168.0.110-120,192.168.0.0/28"

LINE_WIDTH = 48  # ≈48 chars per line on 80mm
TZ = ZoneInfo(os.getenv("RECEIPT_TZ", "Africa/Nairobi"))

# Company header (stays at the top)
COMPANY_NAME = os.getenv("RECEIPT_COMPANY_NAME", "Blue Bash Investment Ltd")
COMPANY_CONTACT_LINES = [
    os.getenv("RECEIPT_ADDRESS", "P.O.Box 101-70100, Garissa"),
    os.getenv("RECEIPT_TEL",     "Tel: 02 02 447 447 / 07 42 252 535"),
    os.getenv("RECEIPT_EMAIL",   "Email: bluebashdrinkingwater@gmail.com"),
]

# ── Logo settings (outline + small) ─────────────────────────────────────────────
LOGO_ENABLED = (os.getenv("RECEIPT_LOGO_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off"))
LOGO_PATH_ENV = (os.getenv("RECEIPT_LOGO_PATH", "") or "").strip()
LOGO_MAX_WIDTH = max(64, int(os.getenv("RECEIPT_LOGO_MAX_WIDTH", "240")))  # was 180
LOGO_EDGE_THR = max(1, min(254, int(os.getenv("RECEIPT_LOGO_EDGE_THR", "50"))))  # was 40
LOGO_EDGE_MARGIN = max(0, int(os.getenv("RECEIPT_LOGO_EDGE_MARGIN", "3")))
LOGO_STROKE_DILATE = (os.getenv("RECEIPT_LOGO_STROKE_DILATE", "0").strip().lower() not in ("0","false","no","off"))

# ── ESC/POS compat helpers (use these instead of p.set) ────────────────────────
def _align(p, where: str = "left"):
    m = {"left": 0, "center": 1, "right": 2}
    p._raw(b"\x1b\x61" + bytes([m.get(where, 0)]))  # ESC a n

def _bold(p, on: bool):
    p._raw(b"\x1bE" + (b"\x01" if on else b"\x00"))  # ESC E n

def _underline(p, n: int = 0):
    p._raw(b"\x1b-" + bytes([0 if n not in (1, 2) else n]))  # ESC - n

def _size(p, width: int = 1, height: int = 1):
    # GS ! n  (bit 0-3: height, 4-7: width)
    w = 0x10 if (width or 1) >= 2 else 0x00
    h = 0x01 if (height or 1) >= 2 else 0x00
    p._raw(b"\x1d!" + bytes([w | h]))

def pset(p, *, align=None, bold=None, width=None, height=None, underline=None):
    if align is not None: _align(p, align)
    if width is not None or height is not None: _size(p, width or 1, height or 1)
    if bold is not None: _bold(p, bool(bold))
    if underline is not None: _underline(p, int(underline))

# ── Smart IP resolution helpers ────────────────────────────────────────────────
def _cache_get() -> str | None:
    with suppress(Exception):
        with open(PRINTER_CACHE_FILE, "r") as f:
            v = (f.read() or "").strip()
            return v or None
    return None

def _cache_set(ip: str) -> None:
    with suppress(Exception):
        with open(PRINTER_CACHE_FILE, "w") as f:
            f.write(ip)

def _port_open(ip: str, port: int, timeout_ms: int) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(max(0.05, timeout_ms / 1000.0))
        return s.connect_ex((ip, port)) == 0

def _is_ip(s: str) -> bool:
    try:
        socket.inet_aton(s)
        return True
    except OSError:
        return False

def _expand_token_to_ips(token: str) -> list[str]:
    """
    Accepts:
      - single IP: 192.168.0.101
      - last-octet range: 192.168.0.110-120
      - full range: 192.168.0.100-192.168.0.150
      - CIDR: 192.168.0.0/28
    """
    token = token.strip()
    if not token:
        return []
    # CIDR
    if "/" in token:
        net = ipaddress.ip_network(token, strict=False)
        return [str(h) for h in net.hosts()]
    # Range with dash
    if "-" in token:
        left, right = [x.strip() for x in token.split("-", 1)]
        if _is_ip(left) and _is_ip(right):
            a = list(map(int, left.split(".")))
            b = list(map(int, right.split(".")))
            if a[:3] != b[:3]:
                return []
            start, end = a[3], b[3]
            base = ".".join(map(str, a[:3]))
            return [f"{base}.{i}" for i in range(min(start, end), max(start, end) + 1)]
        elif _is_ip(left) and right.isdigit():
            a = list(map(int, left.split(".")))
            start = a[3]; end = int(right)
            base = ".".join(map(str, a[:3]))
            return [f"{base}.{i}" for i in range(min(start, end), max(start, end) + 1)]
        else:
            return []
    # Single IP
    if _is_ip(token):
        return [token]
    return []

def _targets_from_scan_list() -> list[str]:
    if not PRINTER_SCAN_LIST:
        return []
    seen, out = set(), []
    for tok in PRINTER_SCAN_LIST.split(","):
        for ip in _expand_token_to_ips(tok):
            if ip not in seen:
                seen.add(ip); out.append(ip)
    return out

def _discover_given_targets(ips: list[str]) -> list[str]:
    if not ips:
        return []
    hits: list[str] = []
    with ThreadPoolExecutor(max_workers=max(10, PRINTER_DISCOVERY_WORKERS)) as ex:
        futs = {ex.submit(_port_open, ip, PRINTER_PORT, PRINTER_DISCOVERY_TIMEOUT_MS): ip for ip in ips}
        for fut in as_completed(futs):
            ip = futs[fut]
            with suppress(Exception):
                if fut.result():
                    hits.append(ip)
    def _key(s: str): return tuple(int(x) for x in s.split("."))
    return sorted(hits, key=_key)

def _discover_printers_on_subnet() -> list[str]:
    """Scan PRINTER_SUBNET for devices with TCP/9100 open."""
    try:
        net = ipaddress.ip_network(PRINTER_SUBNET, strict=False)
    except Exception:
        # fallback: try same /24 as PRINTER_IP
        with suppress(Exception):
            parts = [int(x) for x in PRINTER_IP.split(".")]
            if len(parts) == 4:
                net = ipaddress.ip_network(".".join(map(str, parts[:3])) + ".0/24", strict=False)
            else:
                net = ipaddress.ip_network("192.168.0.0/24", strict=False)
    hosts = [str(h) for h in net.hosts()]
    return _discover_given_targets(hosts)

def resolve_printer_ip() -> str:
    """
    Order:
      1) PRINTER_HOST (DNS) if reachable
      2) PRINTER_IP from .env if reachable
      3) Cached last-good IP
      4) PRINTER_SCAN_LIST targets (fast)
      5) Full PRINTER_SUBNET scan (slowest)
    """
    # 1) Hostname
    if PRINTER_HOST:
        with suppress(Exception):
            ip = socket.gethostbyname(PRINTER_HOST)
            if _port_open(ip, PRINTER_PORT, PRINTER_DISCOVERY_TIMEOUT_MS):
                _cache_set(ip); return ip

    # 2) .env IP
    if PRINTER_IP:
        with suppress(Exception):
            if _port_open(PRINTER_IP, PRINTER_PORT, PRINTER_DISCOVERY_TIMEOUT_MS):
                _cache_set(PRINTER_IP); return PRINTER_IP

    # 3) Cached
    cached = _cache_get()
    if cached:
        with suppress(Exception):
            if _port_open(cached, PRINTER_PORT, PRINTER_DISCOVERY_TIMEOUT_MS):
                return cached

    # 4) Preferred targets
    targets = _targets_from_scan_list()
    if targets:
        candidates = _discover_given_targets(targets)
        if candidates:
            for pref in (PRINTER_IP, cached):
                if pref and pref in candidates:
                    _cache_set(pref); return pref
            _cache_set(candidates[0]); return candidates[0]

    # 5) Subnet scan
    candidates = _discover_printers_on_subnet()
    if not candidates:
        raise RuntimeError(
            f"Could not find any device listening on TCP/{PRINTER_PORT} in {PRINTER_SUBNET}. "
            "Check LAN reachability and that the printer is powered on."
        )
    for pref in (PRINTER_IP, cached):
        if pref and pref in candidates:
            _cache_set(pref); return pref
    _cache_set(candidates[0]); return candidates[0]

# ── Low-level helpers ───────────────────────────────────────────────────────────
def _reset(p: Network):
    """ESC @ — clear any previous binary mode/state."""
    p._raw(b"\x1b@")

def _connect(timeout: float = 5.0) -> Network:
    """
    CHANGED: Always use PRINTER_IP directly, skip any discovery/scan.
    """
    if not PRINTER_IP:
        raise RuntimeError("PRINTER_IP is not set. Set it in your .env (e.g., 192.168.0.100).")
    return Network(PRINTER_IP, PRINTER_PORT, timeout=timeout)

# ── Formatting helpers ──────────────────────────────────────────────────────────
def _money(x) -> str:
    try:
        return f"{float(x or 0):.2f}"
    except Exception:
        return "0.00"

def _wrap(text: str, width: int) -> list[str]:
    words = (text or "").split()
    lines, cur = [], ""
    for w in words:
        if len(cur) + (1 if cur else 0) + len(w) <= width:
            cur = (cur + " " + w).strip()
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines

def _label_value_lines(label: str, value: str) -> list[str]:
    """
    Render 'Label : value' on one line; wrap subsequent lines under the value with indentation.
    Example (width=48):
    Customer : Very long long name that wraps
               continues here...
    """
    label = (label or "").strip()
    prefix = f"{label} : "
    inner_w = max(1, LINE_WIDTH - len(prefix))
    parts = _wrap(value or "-", inner_w) or ["-"]
    out = [prefix + parts[0]]
    for extra in parts[1:]:
        out.append(" " * len(prefix) + extra)
    return out

def _box_lines_left(lines: list[str]) -> list[str]:
    """Solid rectangular box with padding (kept for compatibility if you need it elsewhere)."""
    inner_w = LINE_WIDTH - 4  # 2 borders + 2 padding
    top = "+" + "-" * (LINE_WIDTH - 2) + "+"
    bottom = top
    body = ["| " + line.ljust(inner_w) + " |" for line in lines]
    return [top] + body + [bottom]

# ── Table (ITEM | QTY | EACH | TOTAL) ─────────────────────────────────────────
ITEM_W, QTY_W, EACH_W, TOTAL_W = 26, 5, 8, 9  # sums to 48

def _row_item(item: str, qty, each, total) -> str:
    return (
        (item or "")[:ITEM_W].ljust(ITEM_W) +
        str(int(qty or 0)).rjust(QTY_W) +
        _money(each).rjust(EACH_W) +
        _money(total).rjust(TOTAL_W)
    )

def _row_wrap_item_only(line: str) -> str:
    """Continuation line when item name wraps; leave numeric cols blank."""
    return line.ljust(ITEM_W) + " " * (QTY_W + EACH_W + TOTAL_W)

def _row_right(label: str, value) -> str:
    """
    Show a label on the left (spanning ITEM+QTY+EACH) and a right-aligned value in the TOTAL column.
    Useful for counts that are NOT currency.
    """
    left_w = ITEM_W + QTY_W + EACH_W
    return label[:left_w].ljust(left_w) + str(value).rjust(TOTAL_W)

def _format_dt(dt_val) -> str:
    """
    Return a Nairobi-time string (Africa/Nairobi, UTC+03:00).
    """
    if isinstance(dt_val, datetime):
        if dt_val.tzinfo is None:
            # assume DB-stored UTC if naive
            dt_val = dt_val.replace(tzinfo=timezone.utc)
        dt_val = dt_val.astimezone(TZ)
    else:
        dt_val = datetime.now(TZ)
    return dt_val.strftime("%Y-%m-%d %H:%M:%S")

def _served_by_from_sale(sale) -> str:
    """
    Try multiple fields to get the current user who served the sale.
    Priority: explicit attributes/relationships on sale, then env fallback.
    """
    candidates = []

    with suppress(Exception):
        u = getattr(sale, "added_by_user", None)
        if u:
            for field in ("full_name", "name", "username", "email"):
                v = getattr(u, field, None)
                if v:
                    candidates.append(str(v))

    for field in ("served_by_name", "served_by", "added_by_name", "cashier_name"):
        with suppress(Exception):
            v = getattr(sale, field, None)
            if v:
                candidates.append(str(v))

    with suppress(Exception):
        u2 = getattr(sale, "user", None)
        if u2:
            for field in ("full_name", "name", "username", "email"):
                v = getattr(u2, field, None)
                if v:
                    candidates.append(str(v))

    env_name = os.getenv("RECEIPT_SERVED_BY", "").strip()
    if env_name:
        candidates.append(env_name)

    for c in candidates:
        if c and not c.isdigit():
            return c.strip()

    with suppress(Exception):
        aid = getattr(sale, "added_by", None)
        if aid:
            return f"User #{aid}"

    return "—"

# ── Logo helpers ───────────────────────────────────────────────────────────────
def _logo_path() -> str | None:
    """Find the logo file path; prefer env, then common app/static paths, then cwd."""
    candidates = []
    if LOGO_PATH_ENV:
        candidates.append(LOGO_PATH_ENV)

    with suppress(Exception):
        root = current_app.root_path  # flask app root
        candidates += [
            os.path.join(root, "static", "images", "Logo.png"),
            os.path.join(root, "static", "images", "logo.png"),
            os.path.join(root, "Logo.png"),
            os.path.join(root, "logo.png"),
        ]

    candidates += [
        os.path.abspath("Logo.png"),
        os.path.abspath("logo.png"),
    ]

    for pth in candidates:
        if pth and os.path.exists(pth):
            return pth
    return None

def _print_logo_outline(p: Network) -> None:
    """Load Logo.png, outline-only (thicker), auto-cropped; small; centered; no extra spacing."""
    if not LOGO_ENABLED:
        return
    path = _logo_path()
    if not path:
        return
    try:
        from PIL import Image, ImageOps, ImageFilter

        img = Image.open(path)

        # Flatten alpha -> white, grayscale
        if img.mode in ("RGBA", "LA"):
            bg = Image.new("RGBA", img.size, (255, 255, 255, 0))
            bg.paste(img, mask=img.split()[-1])
            img = bg.convert("L")
        else:
            img = img.convert("L")

        # Resize (slightly larger default)
        w, h = img.size
        if w > LOGO_MAX_WIDTH:
            new_w = LOGO_MAX_WIDTH
            new_h = max(1, int(h * (new_w / float(w))))
            img = img.resize((new_w, new_h), Image.LANCZOS)
            w, h = img.size

        # Edges-only → thicker: lower threshold + optional dilation
        edges = img.filter(ImageFilter.FIND_EDGES)
        edges = ImageOps.autocontrast(edges)

        # Threshold to black edges on white
        mask = edges.point(lambda x: 0 if x >= LOGO_EDGE_THR else 255, mode="L")

        # Optional 1px dilation to reduce “diffuse” look
        if LOGO_STROKE_DILATE:
            inv = ImageOps.invert(mask)          # lines -> 255
            inv = inv.filter(ImageFilter.MaxFilter(3))
            mask = ImageOps.invert(inv)          # back to lines -> 0 on white 255

        # Remove any outer frame near borders
        m = LOGO_EDGE_MARGIN
        if m > 0 and w > 2*m and h > 2*m:
            px = mask.load()
            for y in list(range(0, m)) + list(range(h - m, h)):
                for x in range(w):
                    px[x, y] = 255
            for y in range(m, h - m):
                for x in list(range(0, m)) + list(range(w - m, w)):
                    px[x, y] = 255

        # Auto-crop empty margins so header sits right below
        inv = ImageOps.invert(mask)  # non-white content becomes nonzero
        box = inv.getbbox()
        if box:
            mask = mask.crop(box)

        bw = mask.convert("1")  # 1-bit for thermal printers

        pset(p, align="center")
        p.image(bw)
        # No extra newline here → header follows immediately
    except Exception:
        pass

# ── Public API (no QR) ─────────────────────────────────────────────────────────
def print_sale_80mm(sale, copies: int = 1) -> None:
    """Print formatted 80mm receipt (with small outline logo)."""
    copies = max(1, int(copies or 1))
    for _ in range(copies):
        _print_one_copy(sale)

# Backward-compat shim: ignore logo_path.
def print_sale_80mm_with_logo(sale, logo_path=None, copies: int = 1) -> None:
    """Deprecated: kept to avoid breaking old callers. Prints with outline logo."""
    print_sale_80mm(sale, copies=copies)

def _print_one_copy(sale) -> None:
    p = _connect()
    try:
        _reset(p)

        # ── Top: small centered outline logo ─────────────────────────────────
        _print_logo_outline(p)

        # ── Company header ────────────────────────────────────────────────────
        pset(p, align="center", bold=True, width=1, height=1)
        p.text(COMPANY_NAME + "\n")
        pset(p, align="center", bold=False, width=1, height=1)
        for line in COMPANY_CONTACT_LINES:
            if line:
                p.text(str(line) + "\n")
        p.text("\n")

        # ── Meta (top): Receipt / Date / Type / Customer (one-line style) ────
        pset(p, align="left", bold=True, width=1, height=1)
        p.text(f"Receipt : {getattr(sale, 'receipt_number', '')}\n")
        date_val = getattr(sale, "date", None)
        p.text(f"Date    : {_format_dt(date_val)}\n")
        sale_type = getattr(sale, "sale_type", "")
        if sale_type:
            p.text(f"Type    : {sale_type}\n")

        # Customer on ONE line (wraps neatly with indent if long)
        customer_name = (getattr(sale, "customer_name", "") or "-").strip()
        for ln in _label_value_lines("Customer", customer_name):
            p.text(ln + "\n")
        p.text("\n")

        # ── Items table header ────────────────────────────────────────────────
        pset(p, align="left", bold=True)
        p.text(
            "ITEM".ljust(ITEM_W) +
            "QTY".rjust(QTY_W) +
            "EACH".rjust(EACH_W) +
            "TOTAL".rjust(TOTAL_W) + "\n"
        )
        p.text("-" * LINE_WIDTH + "\n")

        # ── Items ────────────────────────────────────────────────────────────
        pset(p, bold=False)
        items = (getattr(sale, "items", None) or [])
        total_qty = 0

        for it in items:
            label_obj = getattr(it, "bottle_size", None)
            label = (label_obj.label if label_obj else getattr(it, "label", None)) or "Item"
            qty   = int(getattr(it, "quantity", 0) or 0)
            unit  = float(getattr(it, "unit_price", 0) or 0)
            line  = float(getattr(it, "total_price", 0) or 0)

            total_qty += qty

            lines = _wrap(str(label), ITEM_W) or [""]
            p.text(_row_item(lines[0], qty, unit, line) + "\n")
            for cont in lines[1:]:
                p.text(_row_wrap_item_only(cont) + "\n")

        p.text("-" * LINE_WIDTH + "\n")

        # ── Totals ───────────────────────────────────────────────────────────
        subtotal = float(getattr(sale, "total_amount", 0) or 0)
        paid     = float(getattr(sale, "paid_amount", 0) or 0)
        balance  = max(0.0, subtotal - paid)

        pset(p, bold=True)
        p.text(_row_right("TOTAL",   _money(subtotal)) + "\n")
        p.text(_row_right("PAID",    _money(paid))     + "\n")
        p.text(_row_right("BALANCE", _money(balance))  + "\n")
        pset(p, bold=False)

        # ── Payment method / reference (if any) ──────────────────────────────
        mpesa_ref  = getattr(sale, "payment_ref", None) or getattr(sale, "mpesa_ref", None)
        pay_method = getattr(sale, "payment_method", None)
        if pay_method or mpesa_ref:
            p.text("-" * LINE_WIDTH + "\n")
            if pay_method:
                for ln in _label_value_lines("Method", str(pay_method)):
                    p.text(ln + "\n")
            if mpesa_ref:
                for ln in _label_value_lines("Ref", str(mpesa_ref)):
                    p.text(ln + "\n")

        # ── Summary just below totals ────────────────────────────────────────
        p.text("-" * LINE_WIDTH + "\n")
        p.text(_row_right("TOTAL ITEMS (QTY)", total_qty) + "\n")

        # ── Served by (moved LOWER after sale details) ───────────────────────
        p.text("-" * LINE_WIDTH + "\n")
        for ln in _label_value_lines("Served by", _served_by_from_sale(sale)):
            p.text(ln + "\n")

        # ── Footer ───────────────────────────────────────────────────────────
        p.text("-" * LINE_WIDTH + "\n")
        pset(p, align="center")
        p.text("Thank you for your purchase!\n\n")
        p.cut()
    finally:
        # CHANGED: close quietly to avoid noisy OSError: [Errno 9] on GC.
        try:
            p.close()
        except Exception:
            pass
