import requests, time, csv, io, os, pytz
from datetime import datetime

BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID   = os.environ["CHAT_ID"]

def get_nse_data():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer": "https://www.nseindia.com",
    })
    print("Visiting NSE for cookies...")
    s.get("https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%20100", timeout=20)
    time.sleep(4)
    print("Fetching Nifty 100 data...")
    r = s.get(
        "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20100",
        headers={"Accept": "application/json",
                 "Referer": "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%20100"},
        timeout=20
    )
    r.raise_for_status()
    return r.json()

def to_csv(data):
    rows = data.get("data", [])
    if not rows:
        return None
    stock_rows = [r for r in rows if isinstance(r.get("symbol"), str) and r.get("symbol") not in ("", None)]
    if not stock_rows:
        stock_rows = rows
    all_keys = list(dict.fromkeys(k for row in stock_rows for k in row.keys()))
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=all_keys, extrasaction='ignore')
    w.writeheader()
    for row in stock_rows:
        w.writerow({k: row.get(k, "") for k in all_keys})
    return out.getvalue()

def send_file(csv_text, filename, caption):
    print(f"Sending to Telegram: {filename}")
    r = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
        data={"chat_id": CHAT_ID, "caption": caption, "parse_mode": "Markdown"},
        files={"document": (filename, csv_text.encode("utf-8"), "text/csv")},
        timeout=30
    )
    print(f"Response: {r.status_code}")
    r.raise_for_status()

def send_msg(text):
    requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        data={"chat_id": CHAT_ID, "text": text}, timeout=15
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
    send_file(csv_text, f"Nifty100_{date}.csv",
        f"📊 *Nifty 100 Live Data*\n🕘 {time_str}\n📁 {stocks} stocks")
    print("✅ Done!")
except Exception as e:
    print(f"❌ Error: {e}")
    try:
        send_msg(f"⚠️ Nifty100 download failed on {date}\n\nError: {e}")
    except:
        pass
    raise
        
