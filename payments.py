"""
payments.py — Razorpay integration helpers
"""

import hashlib
import hmac
import io
import logging
import urllib.parse

import cv2
import httpx
import numpy as np
from PIL import Image
import qrcode
import razorpay
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

# ── Persistent HTTP clients (reuse TCP+TLS connections for fast Razorpay calls) ──
_RZP_CLIENT: razorpay.Client | None = None
_RZP_AUTH: tuple[str, str] | None = None

_HTTP_CLIENT = httpx.Client(
    follow_redirects=True,
    timeout=httpx.Timeout(12.0, connect=5.0),
    limits=httpx.Limits(max_connections=30, max_keepalive_connections=15),
)


def get_razorpay_client(key_id: str, key_secret: str) -> razorpay.Client:
    """Return a persistent Razorpay client with pooled keep-alive connections."""
    global _RZP_CLIENT, _RZP_AUTH
    if _RZP_CLIENT is None or _RZP_AUTH != (key_id, key_secret):
        client = razorpay.Client(auth=(key_id, key_secret))
        try:
            adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30)
            client.session.mount("https://", adapter)
            client.session.mount("http://", adapter)
        except Exception:
            pass
        _RZP_CLIENT = client
        _RZP_AUTH = (key_id, key_secret)
    return _RZP_CLIENT


def create_order(client: razorpay.Client, amount_inr: float,
                 receipt: str, notes: dict | None = None) -> dict:
    """
    Create a Razorpay order.
    amount_inr is in Rupees; Razorpay uses paise (×100).
    """
    amount_paise = int(round(amount_inr * 100))
    data = {
        "amount": amount_paise,
        "currency": "INR",
        "receipt": receipt,
        "notes": notes or {},
    }
    order = client.order.create(data=data)
    logger.info("Razorpay order created: %s", order["id"])
    return order


def create_upi_qr(client: razorpay.Client, amount_inr: float,
                  name: str, notes: dict | None = None) -> dict:
    """
    Create a single-use dynamic UPI QR code for exact amount.
    """
    amount_paise = int(round(amount_inr * 100))
    data = {
        "type": "upi_qr",
        "name": (name or "Channel Access")[:40],
        "usage": "single_use",
        "fixed_amount": True,
        "payment_amount": amount_paise,
        "description": "Channel Access",
        "notes": notes or {},
    }
    qr = client.qrcode.create(data=data)
    logger.info("Razorpay UPI QR created: %s, amount=₹%.2f, notes=%s", qr["id"], amount_inr, notes)
    return qr


def fetch_qr_payments(client: razorpay.Client, qr_id: str) -> list:
    """Fetch payments made to a dynamic QR code."""
    res = client.qrcode.fetch_all_payments(qr_id)
    return res.get("items", []) if isinstance(res, dict) else res


def download_and_crop_razorpay_qr(image_url: str, qr_id: str | None = None) -> bytes:
    """
    Download the Razorpay QR image and return a clean, perfectly-cropped QR code.

    Strategy (in order of preference):
    1. Decode UPI payload with cv2 -> regenerate a fresh clean QR (best quality).
    2. Upscale 2x and retry decode (helps with low-res posters).
    3. Use cv2.detect() bounding box (no full decode needed) to crop precisely.
    4. NumPy transition-frequency scan to find the dense QR grid region.
    5. Hardcoded crop targeting upper-centre of Razorpay poster (last resort).
    """
    raw_bytes = None
    # Fast path: bypass rzp.io shortlink redirect by requesting the direct QR endpoint first
    if qr_id:
        direct_url = f"https://api.razorpay.com/v1/l/qrcode/{qr_id}"
        try:
            resp = _HTTP_CLIENT.get(direct_url)
            if resp.status_code == 200 and len(resp.content) > 500:
                raw_bytes = resp.content
        except Exception:
            pass

    if raw_bytes is None:
        if isinstance(image_url, str) and (image_url.startswith("http://") or image_url.startswith("https://")):
            resp = _HTTP_CLIENT.get(image_url)
            resp.raise_for_status()
            raw_bytes = resp.content
        elif isinstance(image_url, bytes):
            raw_bytes = image_url
        else:
            with open(image_url, "rb") as f:
                raw_bytes = f.read()

    # ----- Strategy 1 & 2: cv2 decode -> regenerate clean QR ------------------
    pts = None
    try:
        np_arr = np.frombuffer(raw_bytes, np.uint8)
        cv_img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        if cv_img is not None:
            detector = cv2.QRCodeDetector()

            # Strategy 1: decode at original size
            val, pts, _ = detector.detectAndDecode(cv_img)
            if val and ("upi://" in val or "pa=" in val):
                logger.info("Strategy 1: Decoded UPI payload from QR poster: %s...", val[:40])
                return generate_clean_qr_image(val)

            # Strategy 2: upscale 2x and retry (helps low-res posters)
            if not val:
                big = cv2.resize(cv_img, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
                val2, pts2, _ = detector.detectAndDecode(big)
                if val2 and ("upi://" in val2 or "pa=" in val2):
                    logger.info("Strategy 2: Decoded UPI payload from 2x upscaled poster: %s...", val2[:40])
                    return generate_clean_qr_image(val2)
                # Downscale pts2 back to original coordinates
                if pts2 is not None and len(pts2) > 0:
                    pts = pts2 / 2.0

            # Strategy 3: use detect() bounding points (no full decode needed)
            if pts is None or len(pts) == 0:
                _, pts = detector.detect(cv_img)

    except Exception as exc:
        logger.warning("cv2 QR decode/detect failed: %s", exc)
        pts = None

    # ----- Strategy 3: crop via cv2 bounding box ------------------------------
    img = Image.open(io.BytesIO(raw_bytes)).convert("RGB")
    w, h = img.size
    crop_box = None

    try:
        if pts is not None and len(pts) > 0:
            p = pts[0]
            x0 = int(min(pt[0] for pt in p))
            y0 = int(min(pt[1] for pt in p))
            x1 = int(max(pt[0] for pt in p))
            y1 = int(max(pt[1] for pt in p))
            if (x1 - x0) > 40 and (y1 - y0) > 40:
                crop_box = (x0, y0, x1, y1)
                logger.info("Strategy 3: Located QR bbox via OpenCV pts: %s", crop_box)
    except Exception as exc:
        logger.warning("Strategy 3 bbox extraction failed: %s", exc)

    # ----- Strategy 4: NumPy transition-frequency scan -----------------------
    if not crop_box:
        try:
            gray = np.array(img.convert("L"))
            binary = (gray < 128).astype(np.int8)
            center_bin = binary[:, int(w * 0.1):int(w * 0.9)]
            h_trans = np.sum(np.abs(np.diff(center_bin, axis=1)), axis=1)
            qr_rows = np.where(h_trans >= 10)[0]
            if len(qr_rows) > 20:
                gaps = np.where(np.diff(qr_rows) > 5)[0]
                segs = np.split(qr_rows, gaps + 1)
                longest_row = max(segs, key=len)
                y0_n, y1_n = int(longest_row[0]), int(longest_row[-1])
                qr_band = binary[y0_n:y1_n, :]
                v_trans = np.sum(np.abs(np.diff(qr_band, axis=0)), axis=0)
                qr_cols = np.where(v_trans >= 10)[0]
                if len(qr_cols) > 20:
                    gaps_c = np.where(np.diff(qr_cols) > 5)[0]
                    segs_c = np.split(qr_cols, gaps_c + 1)
                    longest_col = max(segs_c, key=len)
                    x0_n, x1_n = int(longest_col[0]), int(longest_col[-1])
                    crop_box = (x0_n, y0_n, x1_n, y1_n)
                    logger.info("Strategy 4: Located QR bbox via transition analysis: %s", crop_box)
        except Exception as exc:
            logger.warning("Strategy 4 transition scan failed: %s", exc)

    # Apply detected crop box with generous quiet-zone padding
    if crop_box:
        x0, y0, x1, y1 = crop_box
        qr_w, qr_h = x1 - x0, y1 - y0
        # Use 8% padding each side so the quiet zone is never cut
        pad_x = max(20, int(qr_w * 0.08))
        pad_y = max(20, int(qr_h * 0.08))
        final_box = (
            max(0, x0 - pad_x),
            max(0, y0 - pad_y),
            min(w, x1 + pad_x),
            min(h, y1 + pad_y),
        )
        logger.info("Final QR crop box: %s (original %dx%d)", final_box, w, h)
        cropped = img.crop(final_box)
        buf = io.BytesIO()
        cropped.save(buf, format="PNG")
        buf.seek(0)
        return buf.getvalue()

    # ----- Strategy 5: hardcoded fallback targeting Razorpay poster layout ----
    # Razorpay posters typically have the QR centred in the upper portion.
    logger.warning("All QR detection strategies failed; using hardcoded fallback crop")
    fallback_box = (
        int(w * 0.08),   # 8% from left
        int(h * 0.05),   # 5% from top
        int(w * 0.92),   # 92% from left
        int(h * 0.70),   # 70% from top
    )
    cropped = img.crop(fallback_box)
    buf = io.BytesIO()
    cropped.save(buf, format="PNG")
    buf.seek(0)
    return buf.getvalue()


def generate_clean_qr_image(upi_str: str) -> bytes:
    """
    Generate clean cropped QR code image without merchant name or branding.
    """
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=12,  # Larger modules = bigger/clearer QR
        border=4,     # Standard quiet zone = 4 modules per QR spec
    )
    qr.add_data(upi_str)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf.getvalue()


def extract_upi_vpa(upi_str: str) -> str:
    """Extract payee VPA from upi://pay string."""
    try:
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(upi_str).query)
        return qs.get("pa", [""])[0]
    except Exception:
        return ""


def verify_payment_signature(key_secret: str, order_id: str,
                              payment_id: str, signature: str) -> bool:
    """
    Verify Razorpay payment signature using HMAC-SHA256.
    """
    message = f"{order_id}|{payment_id}"
    expected = hmac.new(
        key_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def fetch_payment(client: razorpay.Client, payment_id: str) -> dict:
    """Fetch payment details from Razorpay."""
    return client.payment.fetch(payment_id)


def verify_and_capture(client: razorpay.Client, key_secret: str,
                        order_id: str, payment_id: str,
                        signature: str, amount_paise: int) -> bool:
    """
    Verify signature and capture the payment if authorized.
    Returns True on success.
    """
    if not verify_payment_signature(key_secret, order_id, payment_id, signature):
        logger.warning("Signature mismatch for order %s", order_id)
        return False
    try:
        payment = fetch_payment(client, payment_id)
        if payment.get("status") in ("captured", "authorized"):
            if payment["status"] == "authorized":
                client.payment.capture(payment_id, {"amount": amount_paise})
            return True
    except Exception as exc:
        logger.error("Capture failed: %s", exc)
    return False


def fetch_order(client: razorpay.Client, order_id: str) -> dict:
    """Fetch order details from Razorpay."""
    return client.order.fetch(order_id)


def fetch_order_payments(client: razorpay.Client, order_id: str) -> list:
    """Fetch payments associated with an order."""
    res = client.order.payments(order_id)
    return res.get("items", []) if isinstance(res, dict) else res


def verify_webhook_signature(body: bytes, signature: str, secret: str) -> bool:
    """
    Verify Razorpay webhook signature using HMAC-SHA256.
    Returns True if secret is empty or signature is valid.
    """
    if not secret:
        return True
    expected = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)
