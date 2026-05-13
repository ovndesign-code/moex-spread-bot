import os, json, requests, pandas as pd
from datetime import datetime

# 🔧 НАСТРОЙКИ (ГОТОВО К ЗАПУСКУ)
CONFIG = {
    "NTFY_TOPIC": "my-bonds-alert-x7k9",   # ← Ваш топик
    "TARGET_DV01": 1000,                   # Риск на 1 б.п. (руб.)
    "SPREAD_THRESHOLD": 0.0,               # 🟢 Порог для ВХОДА (0.5% = 50 б.п.)
    "FIX_THRESHOLD": 0.15,                 # 🔴 Порог для ФИКСАЦИИ (0.15% = 15 б.п.)
    "COOLDOWN_MIN": 20,                    # Не дублировать один тип сигнала чаще N минут
    "PAIRS": [
        ("SU26233RMFS5", "SU26246RMFS7"),  # 26233–26246
        ("SU26240RMFS0", "SU26245RMFS9"),  # 26240–26245
        ("SU26245RMFS9", "SU26246RMFS7"),  # 26245–26246
        ("SU26248RMFS3", "SU26250RMFS9"),  # 26248–26250
        ("SU26250RMFS9", "SU26252RMFS5"),  # 26250–26252
        ("SU26238RMFS4", "SU26247RMFS5")   # 26238–26247
    ],
    "HISTORY_FILE": os.path.join(os.path.expanduser("~"), "bond_history.json")
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
    except Exception as e:
        print(f"⚠️ Ошибка {secid}: {e}")
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
        h = {"Title": title, "Priority": str(pr), "Tags": "warning", "Content-Type": "text/plain"}
        requests.post(f"https://ntfy.sh/{CONFIG['NTFY_TOPIC']}", data=msg.encode("utf-8"), headers=h, timeout=10)
        print("✅ Push отправлен")
    except Exception as e: print(f"❌ ntfy: {e}")

def main():
    print(f"⏰ Проверка: {datetime.now().strftime('%H:%M')}")
    hist = load_hist()
    now = datetime.now()

    for id1, id2 in CONFIG["PAIRS"]:
        pk = f"{id1}_{id2}"
        d1, d2 = get_bond(id1), get_bond(id2)
        if not d1 or not d2: print(f"⏭ {pk}: нет данных"); continue
            
        dv1 = calc_dv01(d1["duration"], d1["price"])
        dv2 = calc_dv01(d2["duration"], d2["price"])
        if dv1 <= 0 or dv2 <= 0: print(f"⏭ {pk}: ошибка DV01"); continue
            
        spread = d1["yield"] - d2["yield"]  # Разница в %
        abs_spread = abs(spread)
        clean = f"{id1[2:7]}–{id2[2:7]}"

        # 🟢 ЛОГИКА ВХОДА (Спред > 0.5%)
        if abs_spread > CONFIG["SPREAD_THRESHOLD"]:
            last = hist.get(f"last_open_{pk}")
            if not last or (now - datetime.fromisoformat(last)).total_seconds() >= CONFIG["COOLDOWN_MIN"]*60:
                action = "Шорт 1 / Лонг 2" if spread > 0 else "Лонг 1 / Шорт 2"
                long_id, short_id = (id2, id1) if spread > 0 else (id1, id2)
                dv_long, dv_short = (dv2, dv1) if spread > 0 else (dv1, dv2)
                
                q_long = round(CONFIG["TARGET_DV01"] / dv_long)
                q_short = round(CONFIG["TARGET_DV01"] / dv_short)
                
                msg = (f"🟢 СИГНАЛ НА ВХОД | {clean}\n"
                       f"📊 Спред: {spread*100:.1f} б.п. (порог {CONFIG['SPREAD_THRESHOLD']*100} б.п.)\n"
                       f"🎯 Действие: {action}\n\n"
                       f"📏 DV01-нейтрально ({CONFIG['TARGET_DV01']}₽):\n"
                       f"• Лонг {long_id[2:7]}: {q_long} шт.\n"
                       f"• Шорт {short_id[2:7]}: {q_short} шт.\n\n"
                       f"Yield: {d1['yield']:.2f}% vs {d2['yield']:.2f}%")
                
                send_ntfy("📊 ВХОД", msg, pr=4)
                hist[f"last_open_{pk}"] = now.isoformat()
                print(f"✅ {clean}: Вход отправлен")
                continue  # Если сработал вход, фиксацию в этом же цикле не проверяем

        # 🔴 ЛОГИКА ФИКСАЦИИ (Спред вернулся к ≤ 0.15%)
        elif abs_spread <= CONFIG["FIX_THRESHOLD"]:
            last = hist.get(f"last_fix_{pk}")
            if not last or (now - datetime.fromisoformat(last)).total_seconds() >= CONFIG["COOLDOWN_MIN"]*60:
                msg = (f"🔴 СИГНАЛ НА ФИКСАЦИЮ | {clean}\n"
                       f"📊 Спред: {spread*100:.1f} б.п. (вернулся к ≤ {CONFIG['FIX_THRESHOLD']*100} б.п.)\n"
                       f"🎯 Действие: Закрыть обе позиции\n\n"
                       f"📏 Было открыто DV01-нейтрально ({CONFIG['TARGET_DV01']}₽)\n"
                       f"Yield: {d1['yield']:.2f}% vs {d2['yield']:.2f}%\n"
                       f"💰 Проверьте P&L и закройте в терминале")
                
                send_ntfy("📊 ФИКСАЦИЯ", msg, pr=3)
                hist[f"last_fix_{pk}"] = now.isoformat()
                print(f"✅ {clean}: Фиксация отправлена")
                continue

        print(f"🔇 {clean}: Спред={spread*100:.1f} б.п. → вне зон")

    save_hist(hist)

if __name__ == "__main__":
    main()
