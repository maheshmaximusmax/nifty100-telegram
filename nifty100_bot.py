import requests, time, csv, io, os, pytz
from datetime import datetime

# Reads your secret keys from GitHub (you never type them here)
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID   = os.environ["CHAT_ID"]

def get_nse_data():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer": "https://www.nseindia.com",
    })
    print("Step 1: Visiting NSE to get cookies...")
    s.get(
        "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%20100",
        timeout=20
    )
    time.sleep(4)
    print("Step 2: Downloading Nifty 100 data...")
    r = s.get(
        "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20100",
        headers={
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%20100"
        },
        timeout=20
    )
    r.raise_for_status()
    return r.json()

def to_csv(data):
    rows = data.get("data", [])
    if not rows:
        return None
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=rows[0].keys())
    w.writeheader()
    w.writerows(rows)
    return out.getvalue()

def send_file(csv_text, filename, caption):
    print(f"Step 3: Sending to Telegram...")
    r = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
        data={"chat_id": CHAT_ID, "caption": caption, "parse_mode": "Markdown"},
        files={"document": (filename, csv_text.encode("utf-8"), "text/csv")},
        timeout=30
    )
    print(f"Telegram response: {r.status_code}")
    r.raise_for_status()

def send_msg(text):
    requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        data={"chat_id": CHAT_ID, "text": text},
        timeout=15
    )

ist      = pytz.timezone("Asia/Kolkata")
now      = datetime.now(ist)
date     = now.strftime("%Y-%m-%d")
time_str = now.strftime("%d %b %Y, %I:%M %p IST")

try:
    data     = get_nse_data()
    csv_text = to_csv(data)
    if not csv_text:
        raise Exception("NSE returned empty data")
    stocks = len(csv_text.splitlines()) - 1
    send_file(
        csv_text,
        f"Nifty100_{date}.csv",
        f"📊 *Nifty 100 Live Data*\n🕘 {time_str}\n📁 {stocks} stocks"
    )
    print("✅ Success! File sent to Telegram.")
except Exception as e:
    print(f"❌ Error: {e}")
    try:
        send_msg(f"⚠️ Nifty100 download failed on {date}\n\nError: {e}")
    except:
        pass
    raise
  
