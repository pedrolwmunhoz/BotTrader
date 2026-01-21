import json
import math
import os
import threading
import time
from collections import deque

import websocket

# ================= CONFIGURAÇÃO =================
APP_VERSION = "0.3.4"

SYMBOL = "btcusdt"
WINDOW_TRADES = 500
SHORT_WINDOW_TRADES = 80
PRICE_WINDOW = 200
MIN_LOOKBACK = 50
MOMENTUM_LOOKBACK = 30
VOLATILITY_LOOKBACK = 60

ENTRY_THRESHOLD = 0.65
EXIT_THRESHOLD = 0.45

MAX_LOSS_PCT = 0.002
MAX_TRADE_TIME = 60
FLOW_STOP_THRESHOLD = 0.35
LEVERAGE = 3.0

PATTERN_MIN_TRADES = 8
PATTERN_BUCKET_STEP = 0.2
PATTERN_RECENCY_DAYS = 10
PATTERN_RECENCY_SECONDS = PATTERN_RECENCY_DAYS * 86400
PATTERN_STATS_REFRESH = 30

HISTORY_LOOKBACK_DAYS = 30
HISTORY_LOOKBACK_SECONDS = HISTORY_LOOKBACK_DAYS * 86400
TIME_BUCKET_SECONDS = 3600

ANALYSIS_INTERVAL_MULTIPLIER = 6.0
MIN_ANALYSIS_INTERVAL = 0.5
MAX_ANALYSIS_INTERVAL = 10.0
DEFAULT_ANALYSIS_INTERVAL = 2.0
ANALYSIS_HISTORY_MAX_ENTRIES = 5000

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
TRADE_HISTORY_FILE = os.path.join(DATA_DIR, "trade_history.jsonl")
ANALYSIS_HISTORY_FILE = os.path.join(DATA_DIR, "analysis_history.jsonl")

VOLATILITY_MAX = 0.01
MOMENTUM_SCALE = 3.0
MOMENTUM_VOL_FALLBACK = 0.001
MIN_CONFIDENCE = 0.35
DEFAULT_CONFIDENCE = 0.75

WEIGHTS = {
    "flow": 0.45,
    "short_flow": 0.30,
    "momentum": 0.15,
    "pattern": 0.10,
}

EPSILON = 1e-12
DAY_SECONDS = 86400

# ================= THREAD SAFETY =================
lock = threading.Lock()
show_realtime = threading.Event()

# ================= PATRIMÔNIO =================
INITIAL_EQUITY = 10000.0
equity = INITIAL_EQUITY
equity_leveraged = INITIAL_EQUITY
total_pnl = 0.0
total_pnl_leveraged = 0.0
total_gains = 0.0
total_losses = 0.0
total_gains_leveraged = 0.0
total_losses_leveraged = 0.0

# ================= ESTADO =================
state = "FORA"
entry_price = None
entry_time = None

buy_volume = 0.0
sell_volume = 0.0
short_buy_volume = 0.0
short_sell_volume = 0.0

trades_window = deque(maxlen=WINDOW_TRADES)
short_trades_window = deque(maxlen=SHORT_WINDOW_TRADES)
price_window = deque(maxlen=PRICE_WINDOW)
trade_history = []
analysis_history = deque(maxlen=ANALYSIS_HISTORY_MAX_ENTRIES)
last_analysis_ts = 0.0

wins = 0
losses = 0

last_price = 0.0
last_probability = 0.5
last_components = {}
entry_snapshot = None

pattern_stats_cache = {
    "bucket": None,
    "last_trade_count": 0,
    "last_refresh": 0.0,
    "stats": {},
}

MENU_TITLE = r"""
 ____        _ _____              _
| __ )  ___ | |_   _| __ __ _  __| | ___ _ __
|  _ \ / _ \| | | || '__/ _` |/ _` |/ _ \ '__|
| |_) | (_) | | | || | | (_| | (_| |  __/ |
|____/ \___/|_| |_||_|  \__,_|\__,_|\___|_|
"""

# ================= FUNÇÕES =================
def clamp(value, lower, upper):
    return max(lower, min(upper, value))


def safe_div(numerator, denominator, default=0.0):
    return numerator / denominator if abs(denominator) > EPSILON else default


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def serialize_pattern_key(pattern_key):
    if pattern_key is None:
        return None
    return list(pattern_key)


def deserialize_pattern_key(raw_value):
    if raw_value is None:
        return None
    if isinstance(raw_value, tuple):
        return raw_value
    if isinstance(raw_value, list):
        return tuple(int(value) for value in raw_value)
    return None


def load_jsonl(path):
    if not os.path.exists(path):
        return []
    entries = []
    with open(path, "r", encoding="utf-8") as handler:
        for line in handler:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def append_jsonl(path, entry):
    ensure_data_dir()
    with open(path, "a", encoding="utf-8") as handler:
        handler.write(json.dumps(entry, separators=(",", ":")) + "\n")


def normalize_trade_entry(entry):
    normalized = dict(entry)
    normalized["pattern_key"] = deserialize_pattern_key(entry.get("pattern_key"))
    return normalized


def load_trade_history():
    history = []
    for entry in load_jsonl(TRADE_HISTORY_FILE):
        history.append(normalize_trade_entry(entry))
    return history


def load_analysis_history():
    return load_jsonl(ANALYSIS_HISTORY_FILE)


def append_trade_history(trade):
    payload = dict(trade)
    payload["pattern_key"] = serialize_pattern_key(trade.get("pattern_key"))
    append_jsonl(TRADE_HISTORY_FILE, payload)


def append_analysis_history(entry):
    append_jsonl(ANALYSIS_HISTORY_FILE, entry)


def get_time_bucket(timestamp):
    if timestamp is None:
        return None
    return int((timestamp % DAY_SECONDS) // TIME_BUCKET_SECONDS)


def filter_history_by_time(trades, now, bucket):
    min_ts = now - HISTORY_LOOKBACK_SECONDS
    filtered = []
    for trade in trades:
        entry_time = trade.get("entry_time") or trade.get("timestamp") or trade.get("exit_time")
        if entry_time is None:
            continue
        if entry_time < min_ts:
            continue
        if bucket is not None and get_time_bucket(entry_time) != bucket:
            continue
        filtered.append(trade)
    return filtered


def rebuild_pattern_stats(now):
    bucket = get_time_bucket(now)
    filtered = filter_history_by_time(trade_history, now, bucket)
    stats = {}
    for trade in filtered:
        pattern_key = trade.get("pattern_key")
        if pattern_key is None:
            continue
        entry_time = trade.get("entry_time") or trade.get("timestamp") or trade.get("exit_time")
        if entry_time is None:
            continue
        pnl = trade.get("pnl", 0.0)
        pattern = stats.get(pattern_key)
        if pattern is None:
            pattern = {
                "wins": 0,
                "losses": 0,
                "trades_count": 0,
                "last_seen": entry_time,
                "recent_count": 0,
            }
            stats[pattern_key] = pattern
        pattern["trades_count"] += 1
        if pnl > 0:
            pattern["wins"] += 1
        else:
            pattern["losses"] += 1
        if entry_time > pattern["last_seen"]:
            pattern["last_seen"] = entry_time
        if now - entry_time <= PATTERN_RECENCY_SECONDS:
            pattern["recent_count"] += 1

    pattern_stats_cache["bucket"] = bucket
    pattern_stats_cache["last_trade_count"] = len(trade_history)
    pattern_stats_cache["last_refresh"] = now
    pattern_stats_cache["stats"] = stats
    return stats


def get_pattern_stats(now):
    bucket = get_time_bucket(now)
    needs_refresh = (
        pattern_stats_cache["bucket"] != bucket
        or pattern_stats_cache["last_trade_count"] != len(trade_history)
        or now - pattern_stats_cache["last_refresh"] > PATTERN_STATS_REFRESH
    )
    if needs_refresh:
        return rebuild_pattern_stats(now)
    return pattern_stats_cache["stats"]


def estimate_trade_interval():
    if len(trades_window) < 2:
        return None
    first_ts = trades_window[0].get("ts")
    last_ts = trades_window[-1].get("ts")
    if first_ts is None or last_ts is None or last_ts <= first_ts:
        return None
    return (last_ts - first_ts) / max(1, len(trades_window) - 1)


def compute_analysis_interval():
    avg_interval = estimate_trade_interval()
    if avg_interval is None:
        return DEFAULT_ANALYSIS_INTERVAL
    interval = avg_interval * ANALYSIS_INTERVAL_MULTIPLIER
    return clamp(interval, MIN_ANALYSIS_INTERVAL, MAX_ANALYSIS_INTERVAL)


def record_analysis_snapshot(now, price, probability, components):
    global last_analysis_ts

    interval = compute_analysis_interval()
    if now - last_analysis_ts < interval:
        return

    avg_interval = estimate_trade_interval()
    trade_rate = safe_div(1.0, avg_interval, None) if avg_interval else None
    snapshot = {
        "symbol": SYMBOL,
        "app_version": APP_VERSION,
        "timestamp": now,
        "price": price,
        "probability": probability,
        "confidence": components.get("confidence"),
        "volatility": components.get("volatility"),
        "signals": {
            "flow": components.get("flow"),
            "short_flow": components.get("short_flow"),
            "momentum": components.get("momentum"),
            "pattern": components.get("pattern"),
        },
        "pattern_key": serialize_pattern_key(components.get("pattern_key")),
        "pattern_win_rate": components.get("pattern_win_rate"),
        "analysis_interval": interval,
        "trade_interval_avg": avg_interval,
        "trade_rate": trade_rate,
        "time_bucket": get_time_bucket(now),
    }
    analysis_history.append(snapshot)
    append_analysis_history(snapshot)
    last_analysis_ts = now


def record_trade_history(trade):
    trade_history.append(trade)
    append_trade_history(trade)


def initialize_history():
    global trade_history, analysis_history, last_analysis_ts

    ensure_data_dir()
    trade_history = load_trade_history()
    loaded_analysis = load_analysis_history()
    analysis_history = deque(loaded_analysis, maxlen=ANALYSIS_HISTORY_MAX_ENTRIES)
    if analysis_history:
        last_entry = analysis_history[-1]
        last_analysis_ts = last_entry.get("timestamp", 0.0) or 0.0

def bucketize_signal(value, step):
    return int(round(value / step))


def compute_flow_signal(buy, sell):
    total = buy + sell
    if total <= 0:
        return None
    return clamp((buy - sell) / total, -1.0, 1.0)


def compute_returns(prices, lookback):
    if len(prices) < 2:
        return []
    start = max(1, len(prices) - lookback)
    returns = []
    for i in range(start, len(prices)):
        prev = prices[i - 1]
        if prev <= 0:
            continue
        returns.append((prices[i] - prev) / prev)
    return returns


def compute_volatility(prices):
    returns = compute_returns(prices, VOLATILITY_LOOKBACK)
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / len(returns)
    return math.sqrt(variance)


def compute_momentum_signal(prices, volatility):
    if len(prices) < MOMENTUM_LOOKBACK:
        return None
    start = len(prices) - MOMENTUM_LOOKBACK
    first = prices[start]
    last = prices[-1]
    if first <= 0:
        return None
    raw = (last - first) / first
    scale = volatility if volatility is not None else MOMENTUM_VOL_FALLBACK
    signal = math.tanh((raw / (scale + EPSILON)) * MOMENTUM_SCALE)
    return clamp(signal, -1.0, 1.0)


def build_pattern_key(flow_signal, short_flow_signal, momentum_signal):
    if flow_signal is None or short_flow_signal is None or momentum_signal is None:
        return None
    return (
        bucketize_signal(flow_signal, PATTERN_BUCKET_STEP),
        bucketize_signal(short_flow_signal, PATTERN_BUCKET_STEP),
        bucketize_signal(momentum_signal, PATTERN_BUCKET_STEP),
    )


def get_pattern_signal(pattern_key, now):
    if pattern_key is None:
        return None, 0.0, None, None

    stats = get_pattern_stats(now)
    pattern = stats.get(pattern_key)
    if not pattern:
        return None, 0.0, None, None

    if now - pattern["last_seen"] > PATTERN_RECENCY_SECONDS:
        return None, 0.0, None, None

    trades = pattern["trades_count"]
    win_rate = safe_div(pattern["wins"], trades, 0.5)
    signal = clamp(2 * (win_rate - 0.5), -1.0, 1.0)

    count_weight = min(1.0, trades / PATTERN_MIN_TRADES)
    recent_weight = min(1.0, pattern["recent_count"] / PATTERN_MIN_TRADES)
    recency_weight = clamp(
        1.0 - safe_div(now - pattern["last_seen"], PATTERN_RECENCY_SECONDS, 1.0),
        0.0,
        1.0,
    )
    confidence = count_weight * recent_weight * recency_weight
    meta = {
        "trades_count": trades,
        "recent_count": pattern["recent_count"],
        "last_seen": pattern["last_seen"],
    }
    return signal, confidence, win_rate, meta


def compute_confidence(volatility):
    if volatility is None:
        return DEFAULT_CONFIDENCE
    normalized = min(1.0, volatility / VOLATILITY_MAX)
    return MIN_CONFIDENCE + (1.0 - MIN_CONFIDENCE) * (1.0 - normalized)


def compute_probability(now):
    if len(trades_window) < MIN_LOOKBACK or len(price_window) < MIN_LOOKBACK:
        return 0.5, {"confidence": DEFAULT_CONFIDENCE}

    flow_signal = compute_flow_signal(buy_volume, sell_volume)
    short_flow_signal = compute_flow_signal(short_buy_volume, short_sell_volume)
    volatility = compute_volatility(price_window)
    momentum_signal = compute_momentum_signal(price_window, volatility)
    pattern_key = build_pattern_key(flow_signal, short_flow_signal, momentum_signal)
    pattern_signal, pattern_confidence, pattern_win_rate, pattern_meta = get_pattern_signal(
        pattern_key,
        now,
    )

    signals = {}
    weights = {}

    if flow_signal is not None:
        signals["flow"] = flow_signal
        weights["flow"] = WEIGHTS["flow"]
    if short_flow_signal is not None:
        signals["short_flow"] = short_flow_signal
        weights["short_flow"] = WEIGHTS["short_flow"]
    if momentum_signal is not None:
        signals["momentum"] = momentum_signal
        weights["momentum"] = WEIGHTS["momentum"]
    if pattern_signal is not None and pattern_confidence > 0:
        signals["pattern"] = pattern_signal
        weights["pattern"] = WEIGHTS["pattern"] * pattern_confidence

    if not weights:
        return 0.5, {
            "flow": flow_signal,
            "short_flow": short_flow_signal,
            "momentum": momentum_signal,
            "pattern": pattern_signal,
            "pattern_key": pattern_key,
            "pattern_win_rate": pattern_win_rate,
            "pattern_trades_count": pattern_meta.get("trades_count") if pattern_meta else None,
            "pattern_recent_count": pattern_meta.get("recent_count") if pattern_meta else None,
            "pattern_last_seen": pattern_meta.get("last_seen") if pattern_meta else None,
            "volatility": volatility,
            "confidence": DEFAULT_CONFIDENCE,
        }

    weighted_sum = sum(weights[name] * signals[name] for name in weights)
    weight_total = sum(weights.values())
    raw_signal = weighted_sum / weight_total
    base_prob = 0.5 + 0.5 * raw_signal

    confidence = compute_confidence(volatility)
    final_prob = 0.5 + (base_prob - 0.5) * confidence

    return clamp(final_prob, 0.0, 1.0), {
        "flow": flow_signal,
        "short_flow": short_flow_signal,
        "momentum": momentum_signal,
        "pattern": pattern_signal,
        "pattern_key": pattern_key,
        "pattern_win_rate": pattern_win_rate,
        "pattern_trades_count": pattern_meta.get("trades_count") if pattern_meta else None,
        "pattern_recent_count": pattern_meta.get("recent_count") if pattern_meta else None,
        "pattern_last_seen": pattern_meta.get("last_seen") if pattern_meta else None,
        "volatility": volatility,
        "confidence": confidence,
    }


def format_signal(value):
    return f"{value:+.2f}" if value is not None else "n/a"


def format_float(value, precision=4):
    if value is None:
        return "n/a"
    return f"{value:.{precision}f}"


def evaluate_decision(price, timestamp):
    global state, entry_price, entry_time
    global total_pnl, wins, losses
    global equity, equity_leveraged
    global total_pnl_leveraged
    global total_gains, total_losses
    global total_gains_leveraged, total_losses_leveraged
    global last_probability, last_components, entry_snapshot

    now = timestamp

    with lock:
        prob, components = compute_probability(now)
        last_probability = prob
        last_components = components
        record_analysis_snapshot(now, price, prob, components)

        if state == "FORA":
            if prob >= ENTRY_THRESHOLD:
                state = "DENTRO"
                entry_price = price
                entry_time = now
                entry_snapshot = {
                    "prob": prob,
                    "components": dict(components),
                    "pattern_key": components.get("pattern_key"),
                    "equity": equity,
                    "equity_leveraged": equity_leveraged,
                }

        elif state == "DENTRO":
            if entry_price is None or entry_time is None:
                state = "FORA"
                entry_snapshot = None
                return

            pnl_return = (price - entry_price) / entry_price
            trade_duration = now - entry_time

            stop_financeiro = pnl_return <= -MAX_LOSS_PCT
            stop_fluxo = prob <= FLOW_STOP_THRESHOLD
            stop_tempo = trade_duration >= MAX_TRADE_TIME
            exit_prob = prob <= EXIT_THRESHOLD

            if stop_financeiro or stop_fluxo or stop_tempo or exit_prob:
                entry_equity = entry_snapshot.get("equity") if entry_snapshot else equity
                entry_equity_leveraged = (
                    entry_snapshot.get("equity_leveraged") if entry_snapshot else equity_leveraged
                )
                pnl_amount = entry_equity * pnl_return
                pnl_amount_leveraged = entry_equity_leveraged * pnl_return * LEVERAGE

                total_pnl += pnl_amount
                total_pnl_leveraged += pnl_amount_leveraged
                equity = entry_equity + pnl_amount
                equity_leveraged = entry_equity_leveraged + pnl_amount_leveraged

                if pnl_amount > 0:
                    wins += 1
                    total_gains += pnl_amount
                    total_gains_leveraged += pnl_amount_leveraged
                else:
                    losses += 1
                    total_losses += abs(pnl_amount)
                    total_losses_leveraged += abs(pnl_amount_leveraged)

                entry_components = entry_snapshot["components"] if entry_snapshot else {}
                trade_entry = {
                    "symbol": SYMBOL,
                    "app_version": APP_VERSION,
                    "leverage": LEVERAGE,
                    "entry_time": entry_time,
                    "exit_time": now,
                    "entry_price": entry_price,
                    "exit_price": price,
                    "pnl": pnl_amount,
                    "pnl_amount": pnl_amount,
                    "pnl_amount_leveraged": pnl_amount_leveraged,
                    "pnl_return": pnl_return,
                    "pnl_pct": pnl_return * 100,
                    "duration": trade_duration,
                    "entry_prob": entry_snapshot["prob"] if entry_snapshot else None,
                    "exit_prob": prob,
                    "entry_equity": entry_equity,
                    "exit_equity": equity,
                    "entry_equity_leveraged": entry_equity_leveraged,
                    "exit_equity_leveraged": equity_leveraged,
                    "entry_confidence": entry_components.get("confidence"),
                    "exit_confidence": components.get("confidence"),
                    "entry_volatility": entry_components.get("volatility"),
                    "exit_volatility": components.get("volatility"),
                    "entry_momentum": entry_components.get("momentum"),
                    "exit_momentum": components.get("momentum"),
                    "entry_flow": entry_components.get("flow"),
                    "exit_flow": components.get("flow"),
                    "entry_short_flow": entry_components.get("short_flow"),
                    "exit_short_flow": components.get("short_flow"),
                    "entry_pattern_signal": entry_components.get("pattern"),
                    "exit_pattern_signal": components.get("pattern"),
                    "pattern_key": entry_snapshot["pattern_key"] if entry_snapshot else None,
                    "pattern_win_rate": entry_components.get("pattern_win_rate"),
                    "exit_pattern_win_rate": components.get("pattern_win_rate"),
                    "pattern_trades_count": entry_components.get("pattern_trades_count"),
                    "pattern_recent_count": entry_components.get("pattern_recent_count"),
                    "pattern_last_seen": entry_components.get("pattern_last_seen"),
                    "analysis_interval": compute_analysis_interval(),
                    "time_bucket": get_time_bucket(entry_time),
                    "exit_time_bucket": get_time_bucket(now),
                    "exit_reason": (
                        "STOP_FINANCEIRO" if stop_financeiro else
                        "STOP_FLUXO" if stop_fluxo else
                        "STOP_TEMPO" if stop_tempo else
                        "EXIT_PROB"
                    ),
                    "buy_volume": buy_volume,
                    "sell_volume": sell_volume,
                    "short_buy_volume": short_buy_volume,
                    "short_sell_volume": short_sell_volume,
                }

                record_trade_history(trade_entry)

                state = "FORA"
                entry_price = None
                entry_time = None
                entry_snapshot = None


def print_status():
    with lock:
        total_trades = wins + losses
        winrate = (wins / total_trades * 100) if total_trades > 0 else 0
        equity_pct = ((equity - INITIAL_EQUITY) / INITIAL_EQUITY * 100)
        equity_leveraged_pct = ((equity_leveraged - INITIAL_EQUITY) / INITIAL_EQUITY * 100)
        net_unleveraged = equity - INITIAL_EQUITY
        net_leveraged = equity_leveraged - INITIAL_EQUITY
        net_total = total_gains - total_losses
        net_total_leveraged = total_gains_leveraged - total_losses_leveraged
        confidence = last_components.get("confidence") if last_components else None
        volatility = last_components.get("volatility") if last_components else None
        pattern_key = last_components.get("pattern_key") if last_components else None
        pattern_win_rate = last_components.get("pattern_win_rate") if last_components else None
        history_count = len(trade_history)
        analysis_count = len(analysis_history)
        analysis_interval = compute_analysis_interval()
        current_bucket = get_time_bucket(time.time())
        border = "=" * 78
        section = "-" * 78

        print(f"""
{border}
BOTTRADER STATUS
{border}
STATE: {state} | PRICE: {last_price:.2f}
{section}
PROB: {last_probability:.2f} | CONF: {format_float(confidence, 2)} | VOL: {format_float(volatility, 5)}
SIGNALS: FLOW={format_signal(last_components.get("flow") if last_components else None)} SHORT={format_signal(last_components.get("short_flow") if last_components else None)} MOM={format_signal(last_components.get("momentum") if last_components else None)} PAT={format_signal(last_components.get("pattern") if last_components else None)}
PATTERN: {pattern_key if pattern_key is not None else "n/a"} | WR: {format_float(pattern_win_rate, 2)}
{section}
BUY VOL:  {buy_volume:.4f} | SHORT: {short_buy_volume:.4f}
SELL VOL: {sell_volume:.4f} | SHORT: {short_sell_volume:.4f}
{section}
TRADES: {total_trades} | ACERTOS: {wins} | ERROS: {losses} | WINRATE: {winrate:.2f}%
GANHO % (SEM ALAV.): {equity_pct:+.2f}% | PERDAS: {total_losses:.2f} | GANHOS: {total_gains:.2f}
GANHO % (ALAV. {LEVERAGE:.1f}x): {equity_leveraged_pct:+.2f}% | PERDAS: {total_losses_leveraged:.2f} | GANHOS: {total_gains_leveraged:.2f}
LÍQUIDO: {net_unleveraged:+.2f} | LÍQUIDO ALAV.: {net_leveraged:+.2f}
LÍQUIDO TOTAL: {net_total:+.2f} | LÍQUIDO TOTAL ALAV.: {net_total_leveraged:+.2f}
{section}
HISTORY: {history_count} | ANALYSIS: {analysis_count} | INTERVAL: {analysis_interval:.2f}s | BUCKET: {current_bucket}
PATRIMÔNIO: {equity:.2f} | PATRIMÔNIO ALAV.: {equity_leveraged:.2f}
VERSÃO: {APP_VERSION}
{border}
""")


def console_loop():
    while True:
        if show_realtime.is_set():
            print_status()
        time.sleep(0.5)


def print_menu():
    border = "=" * 78
    print(f"""
{border}
{MENU_TITLE.strip()}
{border}
1 - Real time
2 - Documentacao
{border}
""")


def show_documentation():
    border = "=" * 78
    print(f"""
{border}
DOCUMENTACAO
{border}
""")
    try:
        with open(os.path.join(os.path.dirname(__file__), "README.md"), "r", encoding="utf-8") as handler:
            print(handler.read().strip())
    except OSError as exc:
        print(f"Falha ao ler README: {exc}")
    print(f"\nVERSAO: {APP_VERSION}\n{border}")
    input("Pressione ENTER para voltar ao menu...")


def menu_loop():
    while True:
        print_menu()
        choice = input("Escolha uma opcao: ").strip().lower()
        if choice in ("1", "real time", "realtime"):
            show_realtime.set()
            input("Real time ativo. ENTER para voltar ao menu...")
            show_realtime.clear()
        elif choice in ("2", "documentacao", "documentação"):
            show_realtime.clear()
            show_documentation()
        else:
            print("Opcao invalida. Use 1 ou 2.")

# ================= WEBSOCKET =================
def on_message(ws, message):
    global buy_volume, sell_volume, last_price
    global short_buy_volume, short_sell_volume

    data = json.loads(message)
    price = float(data["p"])
    qty = float(data["q"])
    is_sell = data["m"]
    trade_time = data.get("T") or data.get("E")
    timestamp = trade_time / 1000 if trade_time is not None else time.time()

    with lock:
        last_price = price
        price_window.append(price)

        if len(trades_window) == trades_window.maxlen:
            old = trades_window.popleft()
            if old["side"] == "buy":
                buy_volume -= old["qty"]
            else:
                sell_volume -= old["qty"]

        if len(short_trades_window) == short_trades_window.maxlen:
            old_short = short_trades_window.popleft()
            if old_short["side"] == "buy":
                short_buy_volume -= old_short["qty"]
            else:
                short_sell_volume -= old_short["qty"]

        side = "sell" if is_sell else "buy"
        trades_window.append({"side": side, "qty": qty, "ts": timestamp})
        short_trades_window.append({"side": side, "qty": qty, "ts": timestamp})

        if side == "buy":
            buy_volume += qty
            short_buy_volume += qty
        else:
            sell_volume += qty
            short_sell_volume += qty

    evaluate_decision(price, timestamp)


def on_open(ws):
    print("WebSocket conectado")


def on_close(ws, *args):
    print("WebSocket desconectado. Reconectando...")
    time.sleep(2)
    start_ws()


def on_error(ws, error):
    print("Erro WebSocket:", error)


def start_ws():
    url = f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"
    ws = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_open=on_open,
        on_close=on_close,
        on_error=on_error
    )
    ws.run_forever()


# ================= START =================
initialize_history()
print(f"BotTrader v{APP_VERSION} iniciado para {SYMBOL}")
threading.Thread(target=start_ws, daemon=True).start()
threading.Thread(target=console_loop, daemon=True).start()

menu_loop()
