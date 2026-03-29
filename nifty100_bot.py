import requests, time, csv, io, os, pytz, json
from datetime import datetime

BOT_TOKEN   = os.environ["BOT_TOKEN"]
# RECIPIENTS is a JSON array of chat ids / channel ids e.g. ["123456","@mychannel"]
RECIPIENTS_RAW = os.environ.get("RECIPIENTS", "")
CHAT_ID_LEGACY = os.environ.get("CHAT_ID", "")

def parse_recipients():
    ids = []
    if RECIPIENTS_RAW:
        try:
            parsed = json.loads(RECIPIENTS_RAW)
            if isinstance(parsed, list):
                ids = [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            # fallback: comma separated
            ids = [x.strip() for x in RECIPIENTS_RAW.split(",") if x.strip()]
    if not ids and CHAT_ID_LEGACY:
        ids = [CHAT_ID_LEGACY]
    return ids

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

def send_to(chat_id, csv_bytes, filename, caption):
    r = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
        data={"chat_id": chat_id, "caption": caption, "parse_mode": "Markdown"},
        files={"document": (filename, csv_bytes, "text/csv")},
        timeout=30
    )
    print(f"  → {chat_id}: {r.status_code}")
    return r.ok

def send_msg(chat_id, text):
    requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        data={"chat_id": chat_id, "text": text},
        timeout=15
    )

ist      = pytz.timezone("Asia/Kolkata")
now      = datetime.now(ist)
date     = now.strftime("%Y-%m-%d")
time_str = now.strftime("%d %b %Y, %I:%M %p IST")
recipients = parse_recipients()

print(f"Recipients: {recipients}")

try:
    data     = get_nse_data()
    csv_text = to_csv(data)
    if not csv_text:
        raise Exception("NSE returned empty data")
    csv_bytes = csv_text.encode("utf-8")
    stocks    = len(csv_text.splitlines()) - 1
    caption   = f"📊 *Nifty 100 Live Data*\n🕘 {time_str}\n📁 {stocks} stocks"
    filename  = f"Nifty100_{date}.csv"
    ok = 0
    for cid in recipients:
        if send_to(cid, csv_bytes, filename, caption):
            ok += 1
    print(f"✅ Sent to {ok}/{len(recipients)} recipients")
except Exception as e:
    print(f"❌ Error: {e}")
    for cid in recipients:
        try: send_msg(cid, f"⚠️ Nifty100 download failed on {date}\n\nError: {e}")
        except: pass
    raise
    
