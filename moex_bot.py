import os, json, requests, pandas as pd
from datetime import datetime, timedelta

# 🔐 ЗАГРУЗКА СЕКРЕТОВ
VK_TOKEN = os.environ.get('VK_GROUP_TOKEN')
VK_GROUP_ID = os.environ.get('VK_GROUP_ID')
VK_USER_ID = os.environ.get('VK_USER_ID')

CONFIG = {
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

# 📱 ДОСТУПНЫЕ КОМАНДЫ
COMMANDS = {
    "/статус": ["статус", "status"],
    "/отчёт": ["отчёт", "report", "отчет"],
    "/помощь": ["помощь", "help", "помогите"]
}

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
    except:
        return None

def calc_dv01(dur, price):
    return (dur * price / 100) if dur > 0 and price > 0 else 0.0

def load_hist():
    if os.path.exists(CONFIG["HISTORY_FILE"]):
        with open(CONFIG["HISTORY_FILE"], "r", encoding="utf-8") as f: return json.load(f)
    return {}

def save_hist(data):
    with open(CONFIG["HISTORY_FILE"], "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)

def send_vk(message):
    try:
        url = "https://api.vk.com/method/messages.send"
        params = {
            "peer_id": VK_USER_ID,
            "message": message,
            "random_id": int(datetime.now().timestamp() * 1000),
            "access_token": VK_TOKEN,
            "v": "5.131"
        }
        r = requests.post(url, params=params, timeout=10)
        result = r.json()
        if "error" in result:
            print(f"VK Error: {result['error']}")
        else:
            print("VK Sent OK")
    except Exception as e:
        print(f"VK Exception: {e}")

def is_market_open():
    """Проверка: 07:00 - 23:59 МСК, пн-пт"""
    now_msk = datetime.utcnow() + timedelta(hours=3)
    hour = now_msk.hour
    weekday = now_msk.weekday()
    if weekday >= 5: return False
    if 7 <= hour < 24: return True
    return False

def check_incoming_commands():
    """Проверяет последние сообщения на наличие команд"""
    try:
        hist = load_hist()
        last_msg_id = hist.get("last_processed_msg_id", 0)
        
        url = "https://api.vk.com/method/messages.getHistory"
        params = {
            "peer_id": VK_USER_ID,
            "count": 20,
            "access_token": VK_TOKEN,
            "v": "5.131"
        }
        r = requests.post(url, params=params, timeout=10)
        result = r.json()
        
        if "error" in result or "items" not in result.get("response", {}):
            return None
        
        # Ищем новые команды (out=0 — входящие от пользователя)
        for msg in result["response"]["items"]:
            msg_id = msg.get("id", 0)
            if msg_id <= last_msg_id:
                continue
            if msg.get("out") == 0:  # Входящее
                text = msg.get("text", "").strip().lower()
                # Проверяем на команды
                for cmd, aliases in COMMANDS.items():
                    if text == cmd.lower() or text in [a.lower() for a in aliases]:
                        # Сохраняем ID последнего обработанного
                        hist["last_processed_msg_id"] = msg_id
                        save_hist(hist)
                        print(f"Command detected: {cmd}")
                        return cmd
        return None
    except Exception as e:
        print(f"Command check error: {e}")
        return None

def send_status_report():
    """Отправляет текущую сводку по всем парам"""
    lines = [f"📊 СТАТУС | {datetime.now().strftime('%d.%m %H:%M')}\n"]
    
    for id1, id2 in CONFIG["PAIRS"]:
        d1, d2 = get_bond(id1), get_bond(id2)
        if not d1 or not d2:
            lines.append(f"❌ {id1[2:7]}-{id2[2:7]}: нет данных")
            continue
        
        spread = d1["yield"] - d2["yield"]
        abs_spread = abs(spread)
        
        if abs_spread > CONFIG["SPREAD_THRESHOLD"]:
            status = "🟢 ENTRY"
        elif abs_spread <= CONFIG["FIX_THRESHOLD"]:
            status = "🔴 FIX"
        else:
            status = "⚪ WAIT"
        
        lines.append(f"{status} | {id1[2:7]}-{id2[2:7]}: {spread*100:+.1f} bp ({d1['yield']:.2f}% vs {d2['yield']:.2f}%)")
    
    summary = "\n".join(lines)
    summary += f"\n\nПороги: Вход >{CONFIG['SPREAD_THRESHOLD']*100:.0f} bp, Фиксация <={CONFIG['FIX_THRESHOLD']*100:.0f} bp"
    
    send_vk(summary)

def send_help():
    """Отправляет справку по командам"""
    help_text = (
        "📚 ДОСТУПНЫЕ КОМАНДЫ:\n\n"
        "/статус или /report — текущая сводка по всем парам\n"
        "/отчёт — полный отчёт (как в 18:00)\n"
        "/помощь — эта справка\n\n"
        "Бот работает: Пн-Пт 07:00-23:59 МСК\n"
        "Проверка каждые 20 минут"
    )
    send_vk(help_text)

def send_daily_report():
    """Отправка ежедневного отчёта в 18:00"""
    print("Sending daily report at 18:00...")
    lines = [f"📊 ЕЖЕДНЕВНЫЙ ОТЧЁТ | {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"]
    
    for id1, id2 in CONFIG["PAIRS"]:
        d1, d2 = get_bond(id1), get_bond(id2)
        if not d1 or not d2:
            lines.append(f"❌ {id1[2:7]}-{id2[2:7]}: нет данных")
            continue
        
        spread = d1["yield"] - d2["yield"]
        abs_spread = abs(spread)
        
        if abs_spread > CONFIG["SPREAD_THRESHOLD"]:
            status = "🟢 ENTRY"
        elif abs_spread <= CONFIG["FIX_THRESHOLD"]:
            status = "🔴 FIX"
        else:
            status = "⚪ WAIT"
        
        lines.append(f"{status} | {id1[2:7]}-{id2[2:7]}: {spread*100:+.1f} bp ({d1['yield']:.2f}% vs {d2['yield']:.2f}%)")
    
    summary = "\n".join(lines)
    summary += f"\n\nПороги: Вход >{CONFIG['SPREAD_THRESHOLD']*100:.0f} bp, Фиксация <={CONFIG['FIX_THRESHOLD']*100:.0f} bp"
    
    send_vk(summary)

def main():
    print(f"Check time: {datetime.now().strftime('%H:%M')}")
    
    if not VK_TOKEN or not VK_USER_ID:
        print("ERROR: Secrets not found!")
        return

    hist = load_hist()
    now = datetime.now()
    now_msk = datetime.utcnow() + timedelta(hours=3)

    if not is_market_open():
        print("Market closed. Skipping.")
        save_hist(hist)
        return

    # 🔍 ПРОВЕРКА КОМАНД
    cmd = check_incoming_commands()
    if cmd:
        if cmd in ["/статус", "/отчёт", "/report"]:
            send_status_report()
        elif cmd == "/помощь":
            send_help()

    # 📊 ЕЖЕДНЕВНЫЙ ОТЧЁТ В 18:00
    if now_msk.hour == 18 and now_msk.minute < 5:
        send_daily_report()

    # 📈 ПРОВЕРКА СПРЕДОВ
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
                
                send_vk(msg)
                hist[f"last_open_{pk}"] = now.isoformat()
                print(f"OK {clean}: Entry sent")
                continue

        elif abs_spread <= CONFIG["FIX_THRESHOLD"]:
            last = hist.get(f"last_fix_{pk}")
            if not last or (now - datetime.fromisoformat(last)).total_seconds() >= CONFIG["COOLDOWN_MIN"]*60:
                msg = (f"FIX SIGNAL | {clean}\n"
                       f"Spread: {spread*100:.1f} bp (Returned to <= {CONFIG['FIX_THRESHOLD']*100} bp)\n"
                       f"Action: Close positions\n\n"
                       f"Yield: {d1['yield']:.2f}% vs {d2['yield']:.2f}%")
                
                send_vk(msg)
                hist[f"last_fix_{pk}"] = now.isoformat()
                print(f"OK {clean}: Fix sent")
                continue

        print(f"Silent {clean}: Spread={spread*100:.1f} bp")

    save_hist(hist)

if __name__ == "__main__":
    main()
