"""
payments.py — Razorpay integration helpers
"""

import hashlib
import hmac
import io
import logging
import urllib.parse

import qrcode
import razorpay

logger = logging.getLogger(__name__)


def get_razorpay_client(key_id: str, key_secret: str) -> razorpay.Client:
    return razorpay.Client(auth=(key_id, key_secret))


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


def download_and_crop_razorpay_qr(image_url: str) -> bytes:
    """
    Download the Razorpay QR image and crop to JUST the QR code — boundary to boundary.
    No BHIM/UPI logos, no text, no payment app icons, no background.

    1. First attempts to decode the UPI URI payload with cv2.QRCodeDetector.
       If decoded, regenerates a clean, crisp, high-resolution QR image with zero raster artifacts.
    2. If not decoded, locates the exact QR matrix bounding box using OpenCV detector points
       or NumPy black/white transition-frequency analysis (finding the square QR block).
    3. Crops the image cleanly to the QR code boundaries with a clean quiet-zone margin.
    """
    import httpx
    from PIL import Image
    import numpy as np

    if isinstance(image_url, str) and (image_url.startswith("http://") or image_url.startswith("https://")):
        resp = httpx.get(image_url, follow_redirects=True, timeout=15.0)
        resp.raise_for_status()
        raw_bytes = resp.content
    elif isinstance(image_url, bytes):
        raw_bytes = image_url
    else:
        with open(image_url, "rb") as f:
            raw_bytes = f.read()

    # Step 1: Attempt to decode the UPI payment payload directly with OpenCV
    pts = None
    try:
        import cv2
        np_arr = np.frombuffer(raw_bytes, np.uint8)
        cv_img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        if cv_img is not None:
            detector = cv2.QRCodeDetector()
            val, pts, _ = detector.detectAndDecode(cv_img)
            if val and ("upi://" in val or "pa=" in val):
                logger.info("Successfully decoded UPI payload from QR poster: %s...", val[:40])
                return generate_clean_qr_image(val)
    except Exception as exc:
        logger.warning("cv2 QR decode failed or cv2 not available: %s", exc)
        pts = None

    # Step 2: Crop to exact QR bounding box
    img = Image.open(io.BytesIO(raw_bytes)).convert("RGB")
    w, h = img.size

    crop_box = None
    try:
        # Check if cv2 detector found the corner points
        if pts is not None and len(pts) > 0:
            p = pts[0]
            x0 = int(min(pt[0] for pt in p))
            y0 = int(min(pt[1] for pt in p))
            x1 = int(max(pt[0] for pt in p))
            y1 = int(max(pt[1] for pt in p))
            if (x1 - x0) > 40 and (y1 - y0) > 40:
                crop_box = (x0, y0, x1, y1)
                logger.info("Located QR bbox via OpenCV pts: %s", crop_box)

        # If not, use NumPy transition-frequency scan
        if not crop_box:
            gray = np.array(img.convert("L"))
            binary = (gray < 128).astype(np.int8)
            center_bin = binary[:, int(w * 0.15):int(w * 0.85)]
            h_trans = np.sum(np.abs(np.diff(center_bin, axis=1)), axis=1)
            qr_rows = np.where(h_trans >= 12)[0]
            if len(qr_rows) > 30:
                gaps = np.where(np.diff(qr_rows) > 5)[0]
                segs = np.split(qr_rows, gaps + 1)
                longest_row = max(segs, key=len)
                y0, y1 = int(longest_row[0]), int(longest_row[-1])

                qr_band = binary[y0:y1, :]
                v_trans = np.sum(np.abs(np.diff(qr_band, axis=0)), axis=0)
                qr_cols = np.where(v_trans >= 12)[0]
                if len(qr_cols) > 30:
                    gaps_c = np.where(np.diff(qr_cols) > 5)[0]
                    segs_c = np.split(qr_cols, gaps_c + 1)
                    longest_col = max(segs_c, key=len)
                    x0, x1 = int(longest_col[0]), int(longest_col[-1])
                    crop_box = (x0, y0, x1, y1)
                    logger.info("Located QR bbox via transition analysis: %s", crop_box)

        if crop_box:
            x0, y0, x1, y1 = crop_box
            qr_w, qr_h = x1 - x0, y1 - y0
            # Clean quiet-zone padding (~4% of QR dimension, min 10px)
            pad_x = max(10, int(qr_w * 0.04))
            pad_y = max(10, int(qr_h * 0.04))
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

    except Exception as exc:
        logger.warning("QR crop auto-detect failed: %s", exc)

    # Fallback only if all detection fails
    fallback_box = (
        int(w * 0.15),
        int(h * 0.35),
        int(w * 0.85),
        int(h * 0.65),
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
        box_size=10,
        border=2,
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


