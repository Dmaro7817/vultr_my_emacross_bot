import pandas as pd
import os
import glob
import json

RESULTS_FOLDER = "fast_results"
OUT_JSON = "pairs_best_profitable_combo_per_pair.json"

def clean_pair_name(pair_name):
    """Оставляет только тикер до первого подчеркивания"""
    return pair_name.split('_')[0]

result_files = glob.glob(os.path.join(RESULTS_FOLDER, "*_fast_results.csv"))
if not result_files:
    print("Нет файлов с результатами!")
    exit(1)

records = []

for path in result_files:
    pair_raw = os.path.basename(path).replace('_fast_results.csv', '')
    pair = clean_pair_name(pair_raw)
    df = pd.read_csv(path)
    for side in ['long', 'short']:
        df_side = df[df['side'] == side]
        # Оставляем только реально прибыльные
        df_prof = df_side[df_side['total_profit'] > 0]
        if df_prof.empty:
            continue  # Если нет прибыльных комбинаций — пропускаем side
        # Выбираем максимальный total_profit, если несколько — тот, где больше trades
        row = df_prof.sort_values(
            by=["total_profit", "trades"], ascending=[False, False]
        ).iloc[0]
        record = {
            "param": {
                "side": side,
                "ema_dev_percent": row['ema_dev_percent'],
                "tp_percent": row['tp_percent'],
                "sl_percent": row['sl_percent']
            },
            "pairs": [
                {
                    "pair": pair,
                    "trades": int(row['trades']),
                    "mean_profit": round(float(row['mean_profit']) * 100, 4),
                    "total_profit": round(float(row['total_profit']) * 100, 4),
                    "winrate": round(float(row['winrate']) * 100, 2)
                }
            ]
        }
        records.append(record)

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)

print(f"Готово! Только прибыльные комбинации для каждой пары/side сохранены в {OUT_JSON}")

if records:
    print("Пример записи:")
    print(json.dumps(records[0], indent=2, ensure_ascii=False))