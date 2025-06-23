import pandas as pd
import os
import glob
from collections import Counter

RESULTS_FOLDER = "fast_results"  # где лежат *_fast_results.csv
TOP_N = 5

# --- Сбор всех файлов ---
result_files = glob.glob(os.path.join(RESULTS_FOLDER, "*_fast_results.csv"))
if not result_files:
    print("Нет файлов с результатами!")
    exit(1)

# --- Для каждой пары: топ-N прибыльных комбинаций ---
pair_best = {}
all_params_sets = []

for path in result_files:
    pair = os.path.basename(path).replace('_fast_results.csv', '')
    df = pd.read_csv(path)
    df_prof = df[df['total_profit'] > 0]
    if df_prof.empty:
        print(f"{pair}: Нет прибыльных комбинаций")
        continue
    top = df_prof.sort_values("total_profit", ascending=False).head(TOP_N)
    pair_best[pair] = top
    print(f"\n{pair} Топ {TOP_N}:")
    print(top[['side','ema_dev_percent','tp_percent','sl_percent','trades','mean_profit','total_profit','winrate']])
    # Для глобального анализа сохраняем только ключевые параметры
    for _, row in df_prof.iterrows():
        # Формируем tuple из параметров для глобального поиска
        param_tuple = (row['side'], row['ema_dev_percent'], row['tp_percent'], row['sl_percent'])
        all_params_sets.append(param_tuple)

# --- Поиск универсальных прибыльных комбинаций ---
# Для этого найдём все возможные уникальные комбинации
param_counter = Counter(all_params_sets)
total_pairs = len(pair_best)

# Универсальные только те, что встречаются во всех парах
# (можно ослабить до "в >=X% парах" — заменить == total_pairs на >= int(0.8*total_pairs))
universal_candidates = [param for param, cnt in param_counter.items() if cnt == total_pairs]

if not universal_candidates:
    print("\nНет универсальных прибыльных комбинаций, которые подходят для всех пар.")
else:
    print(f"\nНайдено {len(universal_candidates)} универсальных сетапов. Теперь ищем их среднюю прибыль по всем парам...")

    # Для каждого универсального сетапа найдём среднюю прибыль по всем файлам
    avg_results = []
    for param in universal_candidates:
        total_mean = 0
        total_total = 0
        total_trades = 0
        total_winrate = 0
        n = 0
        for pair, df in pair_best.items():
            # Найдём запись с этими параметрами
            row = df[(df['side'] == param[0]) &
                     (df['ema_dev_percent'] == param[1]) &
                     (df['tp_percent'] == param[2]) &
                     (df['sl_percent'] == param[3])]
            if not row.empty:
                total_mean += row['mean_profit'].values[0]
                total_total += row['total_profit'].values[0]
                total_trades += row['trades'].values[0]
                total_winrate += row['winrate'].values[0]
                n += 1
        if n == total_pairs:
            avg_results.append({
                "side": param[0],
                "ema_dev_percent": param[1],
                "tp_percent": param[2],
                "sl_percent": param[3],
                "mean_profit_avg": total_mean / n,
                "total_profit_sum": total_total,
                "trades_sum": total_trades,
                "winrate_avg": total_winrate / n,
            })
    # Сортируем по средней прибыли
    top_universal = sorted(avg_results, key=lambda x: x['mean_profit_avg'], reverse=True)[:TOP_N]
    print(f"\nТоп {TOP_N} универсальных прибыльных комбинаций для всех пар:")
    for res in top_universal:
        print(
            f"side={res['side']}, ema_dev={res['ema_dev_percent']}, tp={res['tp_percent']}, sl={res['sl_percent']} | "
            f"mean_profit_avg={res['mean_profit_avg']:.4f}, total_profit_sum={res['total_profit_sum']:.4f}, "
            f"trades_total={res['trades_sum']}, winrate_avg={res['winrate_avg']:.2f}"
        )

    # Можно сохранить в файл:
    pd.DataFrame(top_universal).to_csv("universal_top_params.csv", index=False)