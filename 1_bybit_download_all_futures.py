import requests
import pandas as pd
import time
import os
import re
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import websocket
from collections import defaultdict

INTERVALS = ["15"]  # таймфреймы: 1m, 5m, 15m, 1h
LIMIT = 1000
SAVE_PATH = "D:/my_emacross_bot/bybit_futures_data_multi_tf/"
START_TIME = int(time.mktime(time.strptime('2025-05-01 00:00:00', '%Y-%m-%d %H:%M:%S'))) * 1000  # ms, UTC

PAIR_LIMIT = 600  # <---- Лимит пар для парсера (изменяй по необходимости)

# --- Глобальный кэш стакана ---
orderbook_snapshots = defaultdict(list)  # {symbol: [(timestamp, snapshot_dict), ...]}
orderbook_lock = threading.Lock()

def get_symbols():
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    resp = requests.get(url)
    data = resp.json()
    return [item["symbol"] for item in data["result"]["list"] if "USDT" in item["symbol"]]

def orderbook_ws_worker_multi(symbols):
    ws_url = "wss://stream.bybit.com/v5/public/linear"
    ws = websocket.WebSocket()
    ws.connect(ws_url)
    args = [f"orderbook.50.{s}" for s in symbols]
    sub_msg = json.dumps({"op": "subscribe", "args": args})
    ws.send(sub_msg)
    print(f"[WS] Подключён к стакану пар: {symbols}")
    while True:
        try:
            msg = ws.recv()
            data = json.loads(msg)
            if "topic" in data and data["topic"].startswith("orderbook"):
                topic = data["topic"]  # orderbook.50.SYMBOL
                symbol = topic.split(".")[-1]
                ts = int(time.time())
                ob = data.get('data', {})
                bids = ob.get('b', [])
                asks = ob.get('a', [])
                if bids and asks:
                    bid_price_1, bid_volume_1 = float(bids[0][0]), float(bids[0][1])
                    ask_price_1, ask_volume_1 = float(asks[0][0]), float(asks[0][1])
                    spread = ask_price_1 - bid_price_1
                    bid_sum_5 = sum(float(x[1]) for x in bids[:5])
                    ask_sum_5 = sum(float(x[1]) for x in asks[:5])
                    ob_imbalance_5 = (bid_sum_5 - ask_sum_5) / (bid_sum_5 + ask_sum_5 + 1e-9)
                    snapshot = {
                        "bid_price_1": bid_price_1, "ask_price_1": ask_price_1,
                        "bid_volume_1": bid_volume_1, "ask_volume_1": ask_volume_1,
                        "spread": spread,
                        "bid_sum_5": bid_sum_5, "ask_sum_5": ask_sum_5,
                        "ob_imbalance_5": ob_imbalance_5
                    }
                    with orderbook_lock:
                        orderbook_snapshots[symbol].append((ts, snapshot))
                        print(f"[WS] SNAPSHOT {symbol} {ts}: {snapshot}")  # <--- print добавлен
                        if len(orderbook_snapshots[symbol]) > 1200:
                            orderbook_snapshots[symbol] = orderbook_snapshots[symbol][-1200:]
        except Exception as e:
            print(f"[WS] Ошибка: {e}")
            time.sleep(2)
            try:
                ws.close()
            except:
                pass
            ws = websocket.WebSocket()
            ws.connect(ws_url)
            ws.send(sub_msg)

def fetch_ohlcv(symbol, interval, limit=1000, end=None):
    url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
    if end is not None:
        url += f"&end={end}"
    for attempt in range(5):
        try:
            resp = requests.get(url, timeout=30)
            data = resp.json()
            if data.get("result") and data["result"].get("list"):
                df = pd.DataFrame(
                    data["result"]["list"],
                    columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"]
                )
                df["timestamp"] = (df["timestamp"].astype("int64") // 1000).astype(int)
                for col in ["open", "high", "low", "close", "volume", "turnover"]:
                    df[col] = df[col].astype(float)
                df = df.sort_values("timestamp")
                return df
        except Exception as e:
            print(f"Ошибка запроса для {symbol} {interval}m (попытка {attempt+1}/5): {e}")
            time.sleep(5)
    return None

def match_orderbook_to_ohlcv(df, symbol, search_window=30):
    ob_cols = ["bid_price_1", "ask_price_1", "bid_volume_1", "ask_volume_1",
               "spread", "ob_imbalance_5", "bid_sum_5", "ask_sum_5"]
    added = {col: [] for col in ob_cols}
    with orderbook_lock:
        ob_snapshots = orderbook_snapshots[symbol][:]
    print(f"[MATCH] Для пары {symbol} собрано snapshot'ов стакана: {len(ob_snapshots)}")  # <--- print добавлен
    last_snapshot = {col: None for col in ob_cols}
    for ts in df["timestamp"]:
        nearest = None
        min_diff = float('inf')
        for ob_ts, snap in ob_snapshots:
            diff = abs(ob_ts - ts)
            if diff < min_diff and diff <= search_window:
                min_diff = diff
                nearest = snap
        if nearest:
            for col in ob_cols:
                val = nearest.get(col, None)
                added[col].append(val)
                last_snapshot[col] = val
        else:
            for col in ob_cols:
                added[col].append(last_snapshot[col])
    for col in ob_cols:
        df[col] = added[col]
    return df

def download_history(symbol, interval, start_time, limit=1000, save_path=SAVE_PATH):
    print(f"Скачиваю {symbol} {interval}m...")
    result = []
    end = int(time.time() * 1000)
    step_minutes = int(interval)
    while True:
        df = fetch_ohlcv(symbol, interval, limit, end)
        if df is None or df.empty:
            break
        df = df[df["timestamp"] * 1000 >= start_time]
        if df.empty:
            break
        df = match_orderbook_to_ohlcv(df, symbol, search_window=300)
        result.insert(0, df)
        print(f"{symbol} {interval}m: {df['timestamp'].iloc[0]} - {df['timestamp'].iloc[-1]}, {len(df)} свечей")
        oldest_timestamp = int(df["timestamp"].iloc[0]) * 1000
        if oldest_timestamp <= start_time or len(df) < limit:
            break
        if oldest_timestamp == end:
            print("Зацикливание, break")
            break
        end = oldest_timestamp - step_minutes * 60 * 1000
        time.sleep(0.05)
    non_empty_result = [df for df in result if df is not None and not df.empty]
    if non_empty_result:
        all_df = pd.concat(non_empty_result, ignore_index=True)
        all_df = all_df.drop_duplicates(subset=["timestamp"])
        all_df = all_df.sort_values("timestamp")
        filename = re.sub(r'[\\/*?:"<>|]', "_", f"{symbol}_{interval}m.csv")
        full_path = os.path.join(save_path, filename)
        try:
            print(f"Пробую сохранить {full_path}, строк: {len(all_df)}")
            all_df.to_csv(full_path, index=False)
            print(f"Сохранил {filename}, строк: {len(all_df)}")
        except Exception as e:
            print(f"Ошибка при сохранении файла {full_path}: {e}")
    else:
        print(f"Нет данных для {symbol} {interval}m")

def worker(args):
    symbol, interval, start_time, limit, save_path = args
    try:
        download_history(symbol, interval, start_time, limit, save_path)
    except Exception as e:
        print(f"Ошибка в таске {symbol} {interval}m: {e}")

if __name__ == "__main__":
    if not os.path.exists(SAVE_PATH):
        try:
            os.makedirs(SAVE_PATH)
            print(f"Создал папку {SAVE_PATH}")
        except Exception as e:
            print(f"Ошибка при создании папки {SAVE_PATH}: {e}")
            exit(1)
    symbols = get_symbols()
    print(f"Всего пар: {len(symbols)}")

    if PAIR_LIMIT > 0:
        symbols = symbols[:PAIR_LIMIT]
        print(f"Лимит парсеру: {PAIR_LIMIT}. Будет обработано пар: {len(symbols)}")

    chunk_size = 10
    ws_threads = []
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        t = threading.Thread(target=orderbook_ws_worker_multi, args=(chunk,), daemon=True)
        t.start()
        ws_threads.append(t)
        time.sleep(1)

    print("Прогреваем WebSocket 0 секунд перед парсингом свечей для накопления стаканов...")
    time.sleep(0)

    args_list = []
    for symbol in symbols:
        for interval in INTERVALS:
            args_list.append((symbol, interval, START_TIME, LIMIT, SAVE_PATH))
    max_workers = 8
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, args) for args in args_list]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Ошибка в потоке: {e}")