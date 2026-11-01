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

    Uses pixel-density scanning to isolate the QR module grid within the white card.
    """
    import httpx
    from PIL import Image
    import numpy as np

    resp = httpx.get(image_url, follow_redirects=True, timeout=15.0)
    resp.raise_for_status()

    img = Image.open(io.BytesIO(resp.content)).convert("RGB")
    w, h = img.size

    try:
        arr = np.array(img)
        # Grayscale (fast weighted sum)
        gray = (0.299 * arr[:, :, 0] + 0.587 * arr[:, :, 1] + 0.114 * arr[:, :, 2]).astype(np.float32)

        # ── Step 1: Find the white card boundaries ────────────────────────────
        # The white card has a solid bright interior. Scan horizontal lines
        # across the full width to find where rows are uniformly bright.
        # Use the center 50% of width to avoid colored side panels.
        mid_left  = int(w * 0.25)
        mid_right = int(w * 0.75)
        mid_strip = gray[:, mid_left:mid_right]   # (h, mid_w)
        mid_w = mid_right - mid_left

        # A "card row" is one where >80% of mid-strip pixels are bright white (>230)
        bright_count = np.sum(mid_strip > 230, axis=1)
        is_card_row  = bright_count > (mid_w * 0.80)

        # Find runs of card rows; take the longest run (= the main white card)
        card_indices = np.where(is_card_row)[0]

        if len(card_indices) > 30:
            # Find the longest consecutive run of card rows
            gaps = np.where(np.diff(card_indices) > 5)[0]
            segments = np.split(card_indices, gaps + 1)
            longest  = max(segments, key=len)
            card_top    = int(longest[0])
            card_bottom = int(longest[-1])
        else:
            card_top    = int(h * 0.22)
            card_bottom = int(h * 0.85)

        # ── Step 2: Within the card, find the QR zone ────────────────────────
        # The QR code has rows with many dark pixels (black modules on white).
        # "SCAN & PAY" text and BHIM/UPI logos also have some dark pixels but
        # far fewer per row. We look for rows with high dark-pixel density.

        card_strip   = gray[card_top:card_bottom, mid_left:mid_right]  # card interior
        dark_per_row = np.sum(card_strip < 60, axis=1)   # strict black only

        # QR rows have significantly more dark pixels than text rows.
        # Threshold: >8% of mid-strip width is dark
        qr_row_mask = dark_per_row > (mid_w * 0.08)
        qr_rel_rows = np.where(qr_row_mask)[0]

        if len(qr_rel_rows) > 30:
            # Take the largest consecutive block (= QR code body)
            gaps2    = np.where(np.diff(qr_rel_rows) > 10)[0]
            segs2    = np.split(qr_rel_rows, gaps2 + 1)
            qr_block = max(segs2, key=len)
            qr_top    = card_top + int(qr_block[0])
            qr_bottom = card_top + int(qr_block[-1])
        else:
            # Fallback: empirical — QR spans ~40%–64% of image height
            qr_top    = int(h * 0.40)
            qr_bottom = int(h * 0.64)

        # ── Step 3: Find horizontal QR boundaries ────────────────────────────
        qr_band      = gray[qr_top:qr_bottom, :]
        dark_per_col = np.sum(qr_band < 60, axis=0)
        qr_h_span    = qr_bottom - qr_top
        qr_col_mask  = dark_per_col > (qr_h_span * 0.04)
        qr_cols      = np.where(qr_col_mask)[0]

        if len(qr_cols) > 20:
            qr_left  = int(qr_cols[0])
            qr_right = int(qr_cols[-1])
        else:
            qr_left  = int(w * 0.13)
            qr_right = int(w * 0.87)

        # ── Tiny padding (1%) so finder pattern corners aren't cut ───────────
        pad_h = max(3, int((qr_bottom - qr_top) * 0.01))
        pad_w = max(3, int((qr_right  - qr_left) * 0.01))

        crop_box = (
            max(0, qr_left  - pad_w),
            max(0, qr_top   - pad_h),
            min(w, qr_right + pad_w),
            min(h, qr_bottom + pad_h),
        )
        logger.info(
            "QR crop: %dx%d → card [%d–%d] → qr rows [%d–%d] cols [%d–%d] box=%s",
            w, h, card_top, card_bottom, qr_top, qr_bottom, qr_left, qr_right, crop_box,
        )

    except Exception as exc:
        logger.warning("QR crop auto-detect failed (%s), using hardcoded ratios", exc)
        # Empirical: Razorpay 674×1644 image — QR at ~40–64% H, ~13–87% W
        crop_box = (
            int(w * 0.13),
            int(h * 0.40),
            int(w * 0.87),
            int(h * 0.64),
        )

    cropped = img.crop(crop_box)
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
        border=3,
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


