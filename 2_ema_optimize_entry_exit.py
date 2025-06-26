import pandas as pd
import numpy as np
import glob
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import itertools

# === ПАРАМЕТРЫ ДЛЯ GRIDSEARCH ===
EMA_LENGTH = 400
TP_GRID = np.arange(3, 20, 0.5)      # Быстрая сетка TP: 0.5%, 1.0%, 1.5%
SL_GRID = -np.arange(3, 10, 0.5)     # Быстрая сетка SL: -0.5%, -1.0%, -1.5%
EMA_DEV_MIN = 3
EMA_DEV_MAX = 15
EMA_DEV_STEP = 0.5

FOLDER_PATH = r"D:\my_emacross_bot_local\bybit_futures_data_multi_tf"
OUT_FOLDER = "fast_results"
os.makedirs(OUT_FOLDER, exist_ok=True)

UNIQUE_COLS = [
    "side", "ema_dev_percent", "tp_percent", "sl_percent"
]

def simulate_strategy_numpy(close, ema, ema_dev, tp, sl, side):
    n = len(close)
    trades = 0
    win = 0
    profits = []

    # 1. Найдём индексы пересечения EMA (точки отсчета)
    if side == 'long':
        crosses = (close[:-1] < ema[:-1]) & (close[1:] > ema[1:])
    else:
        crosses = (close[:-1] > ema[:-1]) & (close[1:] < ema[1:])
    cross_idxs = np.where(crosses)[0] + 1

    for cross_idx in cross_idxs:
        # 2. После пересечения ищем отклонение от EMA
        # Начинаем с cross_idx+1 чтобы не брать сам момент пересечения
        entry_idx = None
        for i in range(cross_idx+1, min(cross_idx+100, n-21)):  # ограничим максимум 100 свечей для поиска входа
            if side == 'long':
                if close[i] < ema[i] * (1 - ema_dev/100):
                    entry_idx = i
                    break
            else:
                if close[i] > ema[i] * (1 + ema_dev/100):
                    entry_idx = i
                    break
        if entry_idx is None:
            continue  # не было отклонения от EMA после пересечения

        entry_price = close[entry_idx]
        window = close[entry_idx+1:entry_idx+21]
        if len(window) < 1:
            continue

        if side == 'long':
            tp_price = entry_price * (1 + tp/100)
            sl_price = entry_price * (1 + sl/100)
            hit_tp = np.where(window >= tp_price)[0]
            hit_sl = np.where(window <= sl_price)[0]
        else:
            tp_price = entry_price * (1 - tp/100)
            sl_price = entry_price * (1 - sl/100)
            hit_tp = np.where(window <= tp_price)[0]
            hit_sl = np.where(window >= sl_price)[0]

        exit_idx = 20
        if hit_tp.size > 0 and (hit_sl.size == 0 or hit_tp[0] <= hit_sl[0]):
            exit_idx = hit_tp[0]
        elif hit_sl.size > 0:
            exit_idx = hit_sl[0]
        exit_price = window[exit_idx] if exit_idx < len(window) else window[-1]
        if side == 'long':
            trade_pnl = (exit_price - entry_price) / entry_price
        else:
            trade_pnl = (entry_price - exit_price) / entry_price
        profits.append(trade_pnl)
        trades += 1
        if trade_pnl > 0:
            win += 1
    if trades == 0:
        return 0, 0, 0, 0
    profits = np.array(profits)
    return np.mean(profits), np.sum(profits), trades, win / trades

def process_file_fast(file):
    try:
        print(f"[INFO] Начинаю обработку файла: {file}")
        df = pd.read_csv(file)
        close = df['close'].values
        ema = pd.Series(close).ewm(span=EMA_LENGTH, adjust=False).mean().values
        result_rows = []
        for side in ['long', 'short']:
            for ema_dev in np.arange(EMA_DEV_MIN, EMA_DEV_MAX+0.001, EMA_DEV_STEP):
                for tp in TP_GRID:
                    for sl in SL_GRID:
                        mean_p, total_p, trades, winr = simulate_strategy_numpy(close, ema, ema_dev, tp, sl, side)
                        if trades > 0:
                            result_rows.append({
                                "side": side,
                                "ema_dev_percent": round(ema_dev, 4),
                                "tp_percent": round(tp, 4),
                                "sl_percent": round(sl, 4),
                                "trades": trades,
                                "mean_profit": mean_p,
                                "total_profit": total_p,
                                "winrate": winr
                            })
        print(f"[INFO] Файл {file} обработан, получено {len(result_rows)} комбинаций")
        return os.path.basename(file), result_rows
    except Exception as e:
        print(f"[ERROR] Ошибка в файле {file}: {e}")
        return os.path.basename(file), []

def append_unique_rows_to_csv(df_new, out_path):
    print(f"[INFO] Проверяю уникальность и добавляю новые строки в {out_path}...")
    if os.path.exists(out_path):
        df_old = pd.read_csv(out_path)
        df_old['unique_key'] = df_old[UNIQUE_COLS].astype(str).agg('|'.join, axis=1)
        df_new['unique_key'] = df_new[UNIQUE_COLS].astype(str).agg('|'.join, axis=1)
        new_rows = df_new[~df_new['unique_key'].isin(df_old['unique_key'])]
        if not new_rows.empty:
            new_rows = new_rows.drop(columns=['unique_key'])
            new_rows.to_csv(out_path, mode='a', index=False, header=False)
        print(f"[INFO] Добавлено новых строк: {len(new_rows)}")
        return len(new_rows)
    else:
        df_new.drop(columns=['unique_key'], inplace=True, errors='ignore')
        df_new.to_csv(out_path, index=False)
        print(f"[INFO] Создан новый файл и добавлено строк: {len(df_new)}")
        return len(df_new)

if __name__ == "__main__":
    print("[START] Запуск скрипта fast_gridsearch_unique_append.py")
    file_list = glob.glob(os.path.join(FOLDER_PATH, "*.csv"))
    print(f"[INFO] Найдено файлов для обработки: {len(file_list)}")

    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_file_fast, file) for file in file_list]
        for f in tqdm(as_completed(futures), total=len(futures), desc="Files"):
            fname, rows = f.result()
            if rows:
                print(f"[INFO] Обрабатываю результаты для {fname}")
                df_out = pd.DataFrame(rows)
                out_path = os.path.join(OUT_FOLDER, fname.replace('.csv', '_fast_results.csv'))
                df_out['unique_key'] = df_out[UNIQUE_COLS].astype(str).agg('|'.join, axis=1)
                added_count = append_unique_rows_to_csv(df_out, out_path)
                if added_count > 0:
                    top = df_out.sort_values('total_profit', ascending=False).head(1)
                    print(f"[RESULT] {fname} | Добавлено новых: {added_count} | TOP: {top['side'].values[0]}, ema_dev={top['ema_dev_percent'].values[0]}, tp={top['tp_percent'].values[0]}, sl={top['sl_percent'].values[0]}, mean={top['mean_profit'].values[0]:.4f}, total={top['total_profit'].values[0]:.4f}, winrate={top['winrate'].values[0]:.2f}")
                else:
                    print(f"[RESULT] {fname} | Все комбинации уже были, ничего не добавлено.")
            else:
                print(f"[WARN] {fname} | Нет новых комбинаций или ошибка при обработке файла.")
    print("[END] Скрипт завершён.")
