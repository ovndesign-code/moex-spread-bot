import os, json, requests, pandas as pd
from datetime import datetime, timedelta

# 🔧 НАСТРОЙКИ
CONFIG = {
    "NTFY_TOPIC": "my-bonds-alert-x7k9",
    "TARGET_DV01": 100000,
    "SPREAD_THRESHOLD": 0.5,
    "FIX_THRESHOLD": 0.15,
    "COOLDOWN_MIN": 20,
    "PAIRS": [
        ("SU26233RMFS5", "SU26246RMFS7"),
        ("SU26240RMFS0", "SU26245RMFS9"),
        ("SU26245RMFS9", "SU26246RMFS7"),
        ("SU26248RMFS3", "SU26250RMFS9"),
        ("SU26250RMFS9", "SU26252RMFS5"),
        ("SU26238RMFS4", "SU26247RMFS5")
    ],
    "HISTORY_FILE": os.path.join(os.path.expanduser("~"), "bond_history.json")
}

COMMANDS = ["status", "summary", "статус", "сводка", "?", "help"]

def get_bond(secid):
    url = f"https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQOB/securities/{secid}.json"
    p = {"iss.only": "marketdata", "marketdata.columns": "YIELD,DURATION,WAPRICE,LAST"}
    try:
        r = requests.get(url, params=p, timeout=10)
        r.raise_for_status()
        d = r.json()["marketdata"]
        cols = d["columns"]; rows = d["data"]
        if not rows: return None
        idx = {c:i for i,c in enumerate(cols)}
        row = rows[0]
        return {
            "yield": float(row[idx.get("YIELD",0)] or 0),
            "duration": float(row[idx.get("DURATION",0)] or 0),
            "price": float(row[idx.get("WAPRICE",0)] or row[idx.get("LAST",0)] or 0)
        }
    except Exception as e:
        return None

def calc_dv01(dur, price):
    return (dur * price / 100) if dur > 0 and price > 0 else 0.0

def load_hist():
    if os.path.exists(CONFIG["HISTORY_FILE"]):
        with open(CONFIG["HISTORY_FILE"], "r", encoding="utf-8") as f: return json.load(f)
    return {}

def save_hist(data):
    with open(CONFIG["HISTORY_FILE"], "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)

def send_ntfy(title, msg, pr=4):
    try:
        h = {"Title": title, "Priority": str(pr), "Tags": "warning", "Content-Type": "text/plain; charset=utf-8"}
        requests.post(f"https://ntfy.sh/{CONFIG['NTFY_TOPIC']}", data=msg.encode("utf-8"), headers=h, timeout=10)
        print(f"Sent: {title}")
    except Exception as e: print(f"ntfy Error: {e}")

def check_commands():
    """Проверяет, есть ли команды от пользователя в топике"""
    try:
        r = requests.get(f"https://ntfy.sh/{CONFIG['NTFY_TOPIC']}/json?limit=5", timeout=5)
        if r.status_code != 200: return False
        
        messages = r.json()
        for msg in messages:
            text = msg.get("message", "").strip().lower()
            if text in COMMANDS:
                print(f"Command detected: {text}")
                return True
        return False
    except:
        return False

def send_summary():
    """Отправляет сводку по всем парам"""
    print("Generating summary...")
    lines = [f"STATUS REPORT | {datetime.now().strftime('%H:%M')}\n"]
    
    for id1, id2 in CONFIG["PAIRS"]:
        d1, d2 = get_bond(id1), get_bond(id2)
        if not d1 or not d2:
            lines.append(f"ERROR | {id1[2:7]}-{id2[2:7]}: no data")
            continue
        
        spread = d1["yield"] - d2["yield"]
        abs_spread = abs(spread)
        
        # Определяем статус
        if abs_spread > CONFIG["SPREAD_THRESHOLD"]:
            status = "ENTRY"
        elif abs_spread <= CONFIG["FIX_THRESHOLD"]:
            status = "FIX"
        else:
            status = "WAIT"
        
        lines.append(f"{status} | {id1[2:7]}-{id2[2:7]}: {spread*100:+.1f} bp ({d1['yield']:.2f}% vs {d2['yield']:.2f}%)")
    
    summary = "\n".join(lines)
    summary += f"\n\nThresholds: Entry >{CONFIG['SPREAD_THRESHOLD']*100:.0f} bp, Fix <={CONFIG['FIX_THRESHOLD']*100:.0f} bp"
    
    send_ntfy("PORTFOLIO STATUS", summary, pr=3)

def main():
    print(f"Check time: {datetime.now().strftime('%H:%M')}")
    hist = load_hist()
    now = datetime.now()

    # ПРОВЕРКА КОМАНД
    if check_commands():
        print("Command detected, sending summary...")
        send_summary()

    # ПРОВЕРКА СПРЕДОВ
    for id1, id2 in CONFIG["PAIRS"]:
        pk = f"{id1}_{id2}"
        d1, d2 = get_bond(id1), get_bond(id2)
        if not d1 or not d2: print(f"Skip {pk}: no data"); continue
            
        dv1 = calc_dv01(d1["duration"], d1["price"])
        dv2 = calc_dv01(d2["duration"], d2["price"])
        if dv1 <= 0 or dv2 <= 0: print(f"Skip {pk}: DV01 error"); continue
            
        spread = d1["yield"] - d2["yield"]
        abs_spread = abs(spread)
        clean = f"{id1[2:7]}-{id2[2:7]}"

        # ВХОД
        if abs_spread > CONFIG["SPREAD_THRESHOLD"]:
            last = hist.get(f"last_open_{pk}")
            if not last or (now - datetime.fromisoformat(last)).total_seconds() >= CONFIG["COOLDOWN_MIN"]*60:
                action = "Short 1 / Long 2" if spread > 0 else "Long 1 / Short 2"
                long_id, short_id = (id2, id1) if spread > 0 else (id1, id2)
                dv_long, dv_short = (dv2, dv1) if spread > 0 else (dv1, dv2)
                
                q_long = round(CONFIG["TARGET_DV01"] / dv_long)
                q_short = round(CONFIG["TARGET_DV01"] / dv_short)
                
                msg = (f"ENTRY SIGNAL | {clean}\n"
                       f"Spread: {spread*100:.1f} bp (Threshold: {CONFIG['SPREAD_THRESHOLD']*100} bp)\n"
                       f"Action: {action}\n\n"
                       f"DV01-Neutral ({CONFIG['TARGET_DV01']} RUB):\n"
                       f"Long {long_id[2:7]}: {q_long} pcs\n"
                       f"Short {short_id[2:7]}: {q_short} pcs\n\n"
                       f"Yield: {d1['yield']:.2f}% vs {d2['yield']:.2f}%")
                
                send_ntfy("ENTRY ALERT", msg, pr=4)
                hist[f"last_open_{pk}"] = now.isoformat()
                print(f"OK {clean}: Entry sent")
                continue

        # ФИКСАЦИЯ
        elif abs_spread <= CONFIG["FIX_THRESHOLD"]:
            last = hist.get(f"last_fix_{pk}")
            if not last or (now - datetime.fromisoformat(last)).total_seconds() >= CONFIG["COOLDOWN_MIN"]*60:
                msg = (f"FIX SIGNAL | {clean}\n"
                       f"Spread: {spread*100:.1f} bp (Returned to <= {CONFIG['FIX_THRESHOLD']*100} bp)\n"
                       f"Action: Close positions\n\n"
                       f"Yield: {d1['yield']:.2f}% vs {d2['yield']:.2f}%")
                
                send_ntfy("FIX ALERT", msg, pr=3)
                hist[f"last_fix_{pk}"] = now.isoformat()
                print(f"OK {clean}: Fix sent")
                continue

        print(f"Silent {clean}: Spread={spread*100:.1f} bp")

    save_hist(hist)

if __name__ == "__main__":
    main()
