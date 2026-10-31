# 📡 Channel Link Provider Bot

A high-performance, security-focused Telegram bot that allows channel administrators to sell access to private Telegram channels with **Anti-Leak Join Protection** and automated **Razorpay Payments**.

The bot enables admins to publish broadcast announcements directly into public/main channels (showing the channel's native branding) with an inline **"💳 Buy Access"** deep-link button. The private channel link is **never exposed publicly**; users are redirected to the bot, complete payment, and receive a cryptographically locked, non-transferable access link with join-request verification and content protection.

---

## ✨ Key Features

| Feature | Details |
|---|---|
| 📢 **Channel Broadcast Wizard (`/post`)** | Post image, text, price & buy button to your Main Channel with live preview before sending |
| 🛡️ **Anti-Leak Join Protection** | Links use `creates_join_request=True`. The bot auto-approves paying users and **declines all unauthorized users** if a link is shared publicly |
| 🔒 **Telegram Content Protection** | Messages are sent with `protect_content=True` to block forwarding, copying text, and taking screenshots |
| 🔗 **Deep-Link Redirection** | Channel posts contain deep-links (`t.me/Bot?start=buy_{post_id}`) that seamlessly open the bot with the exact offer pre-loaded |
| 💳 **Automated Razorpay Payments** | Secure INR payments with automatic order creation and instant verification by Payment ID |
| 🗄️ **Multi-Channel Management** | Add unlimited channels with individual pricing, descriptions, and custom chat IDs |
| 📊 **Admin Dashboard (`/stats`)** | Real-time stats on users, sales, revenue per channel, and transaction counts |
| 🔍 **Chat ID Discovery (`/getid`)** | Forward any post to the bot to quickly discover channel chat IDs |
| ⚡ **Rate-Limit Safe Broadcaster** | Rate-limiting safe message broadcasting to all registered users with exponential back-off |

---

## 🔒 How Anti-Leak Link Protection Works

When access to a private channel is sold, standard static links can easily be leaked, forwarded, or shared in public groups. This bot solves that problem through a 3-layer security system:

```
[Main Channel Post]
       │  (Contains Image + Description + Price)
       │  (Private channel link is NEVER exposed here)
       ▼
User taps [💳 Buy Access — ₹499]
       │
       ▼
Redirected to Bot (/start buy_{post_id})
       │
       ▼
User Pays via Razorpay → Verifies Payment ID
       │
       ▼
Bot generates Join-Request Link (creates_join_request=True)
and delivers it with protect_content=True (Copying & Forwarding blocked)
       │
       ▼
User taps [🚀 Join Channel Now] → Submits Join Request to Telegram
       │
       ▼
Bot intercepts ChatJoinRequest update:
  ├─ User ID in paid database? ──► ✅ AUTO-APPROVED INSTANTLY!
  └─ User ID NOT in database? ──► 🚫 DECLINED & BLOCKED! (Leaked link is useless)
```

1. **Hidden Links**: Private links are never posted in your public/main channel.
2. **Content Protection (`protect_content=True`)**: Users cannot forward the bot message, select text, copy the link, or screenshot it on Android.
3. **Join Request Firewall**: Even if someone manually types out or shares the link, Telegram routes all join attempts through the bot's `handle_chat_join_request`. Only the verified buyer's Telegram User ID is admitted.

---

## 🗂️ File Structure

```
Channel link provider/
├── bot.py           ← Main bot entry point (handlers, deep-linking, join firewall)
├── database.py      ← SQLite layer (users, channels, payments, posts, settings)
├── payments.py      ← Razorpay integration (order creation & verification)
├── broadcast.py     ← Rate-limit-safe broadcaster for bot users
├── requirements.txt
├── .env.example     ← Environment variables template
└── README.md
```

---

## ⚙️ Setup & Installation

### 1. Clone / Download the Project

### 2. Create and Activate Virtual Environment
```bash
python -m venv venv

# Windows (PowerShell):
.\venv\Scripts\Activate.ps1

# Windows (CMD):
.\venv\Scripts\activate.bat

# Linux/Mac:
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

Edit `.env`:
```env
BOT_TOKEN=your_bot_token_from_botfather
ADMIN_IDS=your_numeric_telegram_user_id
RAZORPAY_KEY_ID=your_razorpay_key_id
RAZORPAY_KEY_SECRET=your_razorpay_key_secret
DATABASE_PATH=bot_data.db
BROADCAST_DELAY=0.05
MAIN_CHANNEL_ID=@your_main_channel_username
```

> 💡 **Tip:** Get your numeric Telegram User ID by messaging [@userinfobot](https://t.me/userinfobot).

---

## 📢 Channel Admin Permissions Setup

To enable automated channel posting and the join request firewall:

### 1. Main Channel (Public Broadcast Channel)
- Add your bot to your Main Channel as an **Administrator**.
- Grant the **"Post Messages"** permission.
- *(Telegram channel posts will automatically appear with the channel's name and icon, with no bot badge).*

### 2. Private Channel (Product Channel)
- Add your bot to your Private Channel as an **Administrator**.
- Grant:
  - **"Invite Users via Link"**
  - **"Manage Chat"**
- In the channel settings, you can also enable **"Restrict saving content"** so members inside the private channel cannot copy or forward media out of it.

---

## 🤖 Bot Commands

### User Commands
| Command | Description |
|---|---|
| `/start` | Start bot, view welcome message or process deep-link offer (`/start buy_{id}`) |
| `/menu` | Browse all active available channels and pricing |
| `/mypurchases` | View purchased channels and retrieve protected access buttons |
| `/verify` | Manually verify a Razorpay payment |

### Admin Commands
| Command | Description |
|---|---|
| `/post` | **Interactive Channel Broadcast Wizard** (Image, text, price & preview) |
| `/posts` | List recently published broadcast posts |
| `/setmainchannel` | Set or update default Main Channel for broadcasts |
| `/addchannel` | Add a new private channel (Name, link, price, description, chat ID) |
| `/editchannel` | Edit existing channel details or toggle active status |
| `/delchannel` | Deactivate a channel |
| `/channels` | List all channels with prices, links, and Anti-Leak status |
| `/getid` | Get numeric Chat ID of current chat or forwarded channel message |
| `/grantaccess <user_id> <ch_id>` | Grant free lifetime access to a user |
| `/broadcast` | Broadcast a message to all registered bot users |
| `/stats` | View revenue, total users, active channels, and sales breakdown |
| `/users` | List the 20 most recent users |
| `/payments <channel_id>` | View all confirmed transactions for a specific channel |
| `/ban <user_id>` | Ban a user from the bot |
| `/unban <user_id>` | Unban a user |

---

## 🚀 Step-by-Step Usage Guide

### Step 1: Add a Channel
1. Run `/addchannel` in private chat with your bot.
2. Enter Name, Invite Link, Price in ₹ (INR), and Description.
3. Enter the **Channel Chat ID** (e.g. `-1001234567890`). *(Use `/getid` or forward a message to find it).*
4. Join-Request Anti-Leak Protection is now active for this channel!

### Step 2: Set your Default Main Channel
Run:
```
/setmainchannel @MyMainCommunity
```

### Step 3: Publish a Broadcast Post to your Main Channel
1. Run `/post` in the bot.
2. Send an **Image/Photo** (or type `skip`).
3. Enter the **Text / Caption** for the announcement.
4. Select the private channel being sold from the inline buttons.
5. Enter the **Price** (or `skip` to use default).
6. Target channel confirms your default (type `use` or enter a specific channel).
7. Review the **Live Preview**.
8. Tap **"🚀 Publish to Main Channel"**.

The post appears in your public channel natively, complete with the image, formatted text, price, and an inline **"💳 Buy Access — ₹XXX"** button!

---

## 💳 Automated Payment & Unlock Flow

1. Subscriber sees the announcement in your main channel and taps **"💳 Buy Access — ₹499"**.
2. Telegram opens the bot with the exact offer pre-loaded.
3. The bot generates a Razorpay checkout order.
4. User clicks **"💳 Pay Now via Razorpay"** and completes payment (UPI, Cards, Netbanking).
5. **Auto-Approval**:
   - **Razorpay Webhook** triggers instantly upon payment.
   - **Background Poller** acts as a fail-safe (checks pending orders every 10 seconds).
   - User also has a **"🔄 Check Payment Status"** button for instant on-demand refresh.
   - **No manual payment ID entry is ever required!**
6. **Buyer Access Delivery**: The bot sends a protected message (`protect_content=True`) containing the **Join Link**. Forwarding, text selection, and saving are disabled.
7. **Instant Admin Alert**: Every admin configured in `ADMIN_IDS` instantly receives a Telegram notification containing:
   - User ID, Username, and Full Name
   - Channel Name & Channel ID
   - Price & Amount Paid (₹)
   - Razorpay Order ID & Payment ID
   - Payment Method & Timestamp
8. If any unauthorized user attempts to use a shared link, the bot automatically rejects them.

---

## 🌐 Razorpay Webhook Setup (Optional for Instant <1s Approval)

The bot includes an internal HTTP webhook server running on `WEBHOOK_PORT` (default `8000`).

1. If deploying on a VPS or using ngrok/Cloudflare tunnel:
   - Webhook URL: `https://your-domain.com/webhook/razorpay`
2. In **Razorpay Dashboard** → **Settings** → **Webhooks** → **Add New Webhook**:
   - URL: `https://your-domain.com/webhook/razorpay`
   - Secret: Set your secret (and place it in `RAZORPAY_WEBHOOK_SECRET` in `.env`)
   - Active Events:
     - `payment.captured`
     - `order.paid`
3. Save.
*(Note: Even without configuring a public webhook, the built-in background auto-poller will automatically approve payments within 10 seconds of completion!)*

---

## 📦 Tech Stack

- **Python 3.12+**
- **python-telegram-bot v21.6** (Async architecture, `ChatJoinRequestHandler`, `protect_content`)
- **Razorpay Python SDK**
- **SQLite** with Write-Ahead Logging (WAL mode)
- **python-dotenv**
