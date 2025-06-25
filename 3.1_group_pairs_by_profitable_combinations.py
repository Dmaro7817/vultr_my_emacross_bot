import pandas as pd
import os
import glob
import json

RESULTS_FOLDER = "fast_results"
OUT_JSON = "pairs_best_profitable_combo_per_pair.json"
OUT_SKIPPED = "skipped_pairs_no_stable_strategy.json"
OUT_SKIPPED_DETAILS = "skipped_details_pairs_side.json"

def clean_pair_name(pair_name):
    """Оставляет только тикер до первого подчеркивания"""
    return pair_name.split('_')[0]

result_files = glob.glob(os.path.join(RESULTS_FOLDER, "*_fast_results.csv"))
if not result_files:
    print("Нет файлов с результатами!")
    exit(1)

records = []
skipped_pairs = []
skipped_details = []

for path in result_files:
    pair_raw = os.path.basename(path).replace('_fast_results.csv', '')
    pair = clean_pair_name(pair_raw)
    df = pd.read_csv(path)
    added = False
    for side in ['long', 'short']:
        df_side = df[df['side'] == side]
        # Оставляем только реально прибыльные
        df_prof = df_side[df_side['total_profit'] > 0]
        if df_prof.empty:
            skipped_details.append({"pair": pair, "side": side})
            continue  # Если нет прибыльных комбинаций — пропускаем side

        # Стабильные параметры: trades > 30, winrate > 0.55
        df_stable = df_prof[(df_prof['trades'] > 30) & (df_prof['winrate'] > 0.55)]

        if df_stable.empty:
            skipped_details.append({"pair": pair, "side": side})
            continue  # Нет стабильных параметров — не добавляем

        # Выбираем максимальный total_profit, если несколько — тот, где больше trades
        row = df_stable.sort_values(
            by=["total_profit", "trades"], ascending=[False, False]
        ).iloc[0]

        # sl_percent всегда положительный
        sl_percent = abs(row['sl_percent'])

        record = {
            "param": {
                "side": side,
                "ema_dev_percent": row['ema_dev_percent'],
                "tp_percent": row['tp_percent'],
                "sl_percent": sl_percent
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
        added = True
    if not added:
        skipped_pairs.append(pair)

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)

with open(OUT_SKIPPED, "w", encoding="utf-8") as f:
    json.dump(skipped_pairs, f, indent=2, ensure_ascii=False)

with open(OUT_SKIPPED_DETAILS, "w", encoding="utf-8") as f:
    json.dump(skipped_details, f, indent=2, ensure_ascii=False)

print(f"Готово! Только лучшие стабильные комбинации для каждой пары/side сохранены в {OUT_JSON}")
print(f"Пары, для которых не найдено ни одной стабильной стратегии (ни long, ни short), сохранены в {OUT_SKIPPED}")
print(f"Пары и стороны, для которых не найдено стабильных стратегий, сохранены в {OUT_SKIPPED_DETAILS}")

print(f"Добавлено в best params: {len(records)}")
print(f"Добавлено в skipped_pairs (полностью пропущенные пары): {len(skipped_pairs)}")
print(f"Добавлено в skipped_details (пар/сторон без стабильных стратегий): {len(skipped_details)}")

if records:
    print("Пример записи:")
    print(json.dumps(records[0], indent=2, ensure_ascii=False))
if skipped_pairs:
    print("Пример пропущенной пары:", skipped_pairs[0])
if skipped_details:
    print("Пример пропущенной пары и стороны:", skipped_details[0])