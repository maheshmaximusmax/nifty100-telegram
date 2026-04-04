import requests, time, csv, io, os, pytz, json, base64, glob
from datetime import datetime

BOT_TOKEN      = os.environ["BOT_TOKEN"]
RECIPIENTS_RAW = os.environ.get("RECIPIENTS", "")
CHAT_ID_LEG    = os.environ.get("CHAT_ID", "")
INDEX_NAME     = os.environ.get("INDEX_NAME", "NIFTY 100")
GH_TOKEN       = os.environ.get("GH_TOKEN", "")
GH_REPO        = os.environ.get("GH_REPO", "")  # format: username/reponame

MAX_CSV_FILES  = 5   # Auto-delete oldest when more than this many exist

GH_HEADERS = {
    "Authorization": f"Bearer {GH_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}

NSE_INDICES = {
    "NIFTY 100":"NIFTY 100","NIFTY 50":"NIFTY 50","NIFTY NEXT 50":"NIFTY NEXT 50",
    "NIFTY MIDCAP 100":"NIFTY MIDCAP 100","NIFTY SMALLCAP 100":"NIFTY SMALLCAP 100",
    "NIFTY BANK":"NIFTY BANK","NIFTY IT":"NIFTY IT","NIFTY PHARMA":"NIFTY PHARMA",
    "NIFTY AUTO":"NIFTY AUTO","NIFTY FMCG":"NIFTY FMCG","NIFTY METAL":"NIFTY METAL",
}

def parse_recipients():
    ids = []
    if RECIPIENTS_RAW:
        try:
            parsed = json.loads(RECIPIENTS_RAW)
            if isinstance(parsed, list):
                ids = [str(x).strip() for x in parsed if str(x).strip()]
        except:
            ids = [x.strip() for x in RECIPIENTS_RAW.split(",") if x.strip()]
    if not ids and CHAT_ID_LEG:
        ids = [CHAT_ID_LEG]
    return ids

def get_nse_data(index_name):
    enc = requests.utils.quote(index_name)
    url = f"https://www.nseindia.com/market-data/live-equity-market?symbol={enc}"
    api = f"https://www.nseindia.com/api/equity-stockIndices?index={index_name}"
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer": "https://www.nseindia.com",
    })
    print(f"[NSE] Visiting homepage for cookies...")
    s.get(url, timeout=20)
    time.sleep(3)
    print(f"[NSE] Fetching {index_name}...")
    r = s.get(api, headers={"Accept":"application/json","Referer":url}, timeout=20)
    r.raise_for_status()
    return r.json()

def to_csv(data):
    rows = data.get("data", [])
    if not rows: return None
    stock_rows = [r for r in rows if isinstance(r.get("symbol"), str) and r.get("symbol")]
    if not stock_rows: stock_rows = rows
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
    print(f"  → {chat_id}: HTTP {r.status_code}")
    return r.ok

def send_msg(chat_id, text):
    requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                  data={"chat_id": chat_id, "text": text}, timeout=15)

def upload_csv_to_github(csv_text, filename):
    """Upload CSV file to GitHub repo data/ folder via API."""
    if not GH_TOKEN or not GH_REPO:
        print("[Upload] No GH_TOKEN/GH_REPO set, skipping GitHub upload.")
        return False
    path = f"data/{filename}"
    url = f"https://api.github.com/repos/{GH_REPO}/contents/{path}"
    content = base64.b64encode(csv_text.encode("utf-8")).decode("ascii")
    # Check if file already exists to get its SHA (needed for update)
    sha = None
    r = requests.get(url, headers=GH_HEADERS, timeout=15)
    if r.ok:
        sha = r.json().get("sha")
        print(f"[Upload] File {filename} already exists, will update.")
    body = {"message": f"Auto-upload CSV: {filename}", "content": content}
    if sha:
        body["sha"] = sha
    r = requests.put(url, headers=GH_HEADERS, json=body, timeout=30)
    if r.ok:
        print(f"[Upload] ✅ Uploaded {filename} to GitHub ({path})")
        return True
    print(f"[Upload] ❌ Upload failed: {r.status_code} {r.text[:200]}")
    return False

def cleanup_old_csvs_on_github():
    """Keep only MAX_CSV_FILES CSVs in the repo data/ folder. Delete oldest."""
    if not GH_TOKEN or not GH_REPO:
        print("[Cleanup] No GH_TOKEN/GH_REPO set, skipping GitHub cleanup.")
        return
    try:
        r = requests.get(f"https://api.github.com/repos/{GH_REPO}/contents/data", headers=GH_HEADERS, timeout=15)
        if r.status_code == 404:
            return  # data folder doesn't exist yet
        if not r.ok:
            print(f"[Cleanup] GitHub list error: {r.status_code}")
            return
        files = [f for f in r.json() if f["name"].endswith(".csv")]
        files.sort(key=lambda x: x["name"])  # oldest first (by filename date)
        print(f"[Cleanup] Found {len(files)} CSV(s) in data/. Max allowed: {MAX_CSV_FILES}")
        while len(files) > MAX_CSV_FILES:
            oldest = files.pop(0)
            dr = requests.delete(
                f"https://api.github.com/repos/{GH_REPO}/contents/data/{oldest['name']}",
                headers=GH_HEADERS,
                json={"message": f"Auto-delete old CSV: {oldest['name']}", "sha": oldest["sha"]},
                timeout=15
            )
            print(f"[Cleanup] Deleted {oldest['name']}: HTTP {dr.status_code}")
    except Exception as e:
        print(f"[Cleanup] Error: {e}")

# ─── MAIN ───
ist      = pytz.timezone("Asia/Kolkata")
now      = datetime.now(ist)
date     = now.strftime("%Y-%m-%d")
time_str = now.strftime("%d %b %Y, %I:%M %p IST")
recipients = parse_recipients()

print(f"[BOT] Index: {INDEX_NAME} | Recipients: {recipients}")

try:
    data     = get_nse_data(INDEX_NAME)
    csv_text = to_csv(data)
    if not csv_text: raise Exception("NSE returned empty data")
    csv_bytes = csv_text.encode("utf-8")
    stocks    = len(csv_text.splitlines()) - 1
    caption   = f"📊 *{INDEX_NAME} Live Data*\n🕘 {time_str}\n📁 {stocks} stocks"
    filename  = f"{INDEX_NAME.replace(' ','_')}_{date}.csv"
    ok = 0
    for cid in recipients:
        if send_to(cid, csv_bytes, filename, caption): ok += 1
    print(f"[BOT] ✅ Sent to {ok}/{len(recipients)} recipients")
    # Upload CSV to GitHub data/ folder
    upload_csv_to_github(csv_text, filename)
    # Cleanup old CSVs from GitHub repo (keep only MAX_CSV_FILES)
    cleanup_old_csvs_on_github()
except Exception as e:
    print(f"[BOT] ❌ Error: {e}")
    for cid in recipients:
        try: send_msg(cid, f"⚠️ {INDEX_NAME} download failed on {date}\n\nError: {e}")
        except: pass
    raise
    
