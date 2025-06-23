import pandas as pd
import numpy as np
import glob
import os
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import itertools

# === ПАРАМЕТРЫ ДЛЯ GRIDSEARCH ===
EMA_LENGTH = 400
TP_GRID = np.arange(3, 10, 0.5)      # Быстрая сетка TP: 0.5%, 1.0%, 1.5%
SL_GRID = -np.arange(1, 5, 0.5)     # Быстрая сетка SL: -0.5%, -1.0%, -1.5%
EMA_DEV_MIN = 3
EMA_DEV_MAX = 15
EMA_DEV_STEP = 0.5

FOLDER_PATH = r"D:\my_emacross_bot_local\bybit_futures_data_multi_tf"
OUT_FOLDER = "fast_results"
os.makedirs(OUT_FOLDER, exist_ok=True)

def simulate_strategy_numpy(close, ema, ema_dev, tp, sl, side):
    n = len(close)
    trades = 0
    profit = 0.0
    win = 0
    profits = []
    for i in range(n - 21):
        entry = False
        if side == 'long':
            entry = close[i] < (ema[i] * (1 - ema_dev/100))
        else:
            entry = close[i] > (ema[i] * (1 + ema_dev/100))
        if entry:
            entry_price = close[i]
            window = close[i+1:i+21]
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
            exit_type = "timeout"
            if hit_tp.size > 0 and (hit_sl.size == 0 or hit_tp[0] <= hit_sl[0]):
                exit_idx = hit_tp[0]
                exit_type = "tp"
            elif hit_sl.size > 0:
                exit_idx = hit_sl[0]
                exit_type = "sl"
            exit_price = window[exit_idx] if exit_idx < len(window) else window[-1]
            if side == 'long':
                trade_pnl = (exit_price - entry_price) / entry_price
            else:
                trade_pnl = (entry_price - exit_price) / entry_price
            profits.append(trade_pnl)
            trades += 1
            if trade_pnl > 0: win += 1
    if trades == 0:
        return 0, 0, 0, 0
    profits = np.array(profits)
    return np.mean(profits), np.sum(profits), trades, win / trades

def process_file_fast(file):
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
    return os.path.basename(file), result_rows

if __name__ == "__main__":
    file_list = glob.glob(os.path.join(FOLDER_PATH, "*.csv"))
    print(f"Найдено файлов: {len(file_list)}")
    all_results = []

    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_file_fast, file) for file in file_list]
        for f in tqdm(futures, desc="Files"):
            fname, rows = f.result()
            if rows:
                df_out = pd.DataFrame(rows)
                out_path = os.path.join(OUT_FOLDER, fname.replace('.csv', '_fast_results.csv'))
                df_out.to_csv(out_path, index=False)
                top = df_out.sort_values('total_profit', ascending=False).head(1)
                print(f"{fname} | TOP: {top['side'].values[0]}, ema_dev={top['ema_dev_percent'].values[0]}, tp={top['tp_percent'].values[0]}, sl={top['sl_percent'].values[0]}, mean={top['mean_profit'].values[0]:.4f}, total={top['total_profit'].values[0]:.4f}, winrate={top['winrate'].values[0]:.2f}")