import json
import math
import threading
import time
from collections import deque

import websocket

# ================= CONFIGURAÇÃO =================
APP_VERSION = "0.2.0"

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

PATTERN_EXPIRY = 3600  # 1 hora
PATTERN_MIN_TRADES = 8
PATTERN_BUCKET_STEP = 0.2

VOLATILITY_MAX = 0.01
MOMENTUM_SCALE = 3.0
MOMENTUM_VOL_FALLBACK = 0.001
MIN_CONFIDENCE = 0.35
DEFAULT_CONFIDENCE = 0.75

WEIGHTS = {
    "flow": 0.35,
    "short_flow": 0.20,
    "momentum": 0.25,
    "pattern": 0.20,
}

EPSILON = 1e-12

# ================= THREAD SAFETY =================
lock = threading.Lock()

# ================= PATRIMÔNIO =================
INITIAL_EQUITY = 10000.0
equity = INITIAL_EQUITY
total_gains = 0.0
total_losses = 0.0

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

total_pnl = 0.0
wins = 0
losses = 0

last_price = 0.0
last_probability = 0.5
last_components = {}
entry_snapshot = None

# ================= PADRÕES VIVOS =================
historical_patterns = {}

# ================= FUNÇÕES =================
def clamp(value, lower, upper):
    return max(lower, min(upper, value))


def safe_div(numerator, denominator, default=0.0):
    return numerator / denominator if abs(denominator) > EPSILON else default


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


def get_pattern_signal(pattern_key):
    if pattern_key is None:
        return None, 0.0, None
    pattern = historical_patterns.get(pattern_key)
    if not pattern:
        return None, 0.0, None
    now = time.time()
    if now - pattern["last_seen"] > PATTERN_EXPIRY:
        return None, 0.0, None
    trades = pattern["trades_count"]
    win_rate = safe_div(pattern["wins"], trades, 0.5)
    signal = clamp(2 * (win_rate - 0.5), -1.0, 1.0)
    confidence = min(1.0, trades / PATTERN_MIN_TRADES)
    return signal, confidence, win_rate


def compute_confidence(volatility):
    if volatility is None:
        return DEFAULT_CONFIDENCE
    normalized = min(1.0, volatility / VOLATILITY_MAX)
    return MIN_CONFIDENCE + (1.0 - MIN_CONFIDENCE) * (1.0 - normalized)


def compute_probability():
    if len(trades_window) < MIN_LOOKBACK or len(price_window) < MIN_LOOKBACK:
        return 0.5, {"confidence": DEFAULT_CONFIDENCE}

    flow_signal = compute_flow_signal(buy_volume, sell_volume)
    short_flow_signal = compute_flow_signal(short_buy_volume, short_sell_volume)
    volatility = compute_volatility(price_window)
    momentum_signal = compute_momentum_signal(price_window, volatility)
    pattern_key = build_pattern_key(flow_signal, short_flow_signal, momentum_signal)
    pattern_signal, pattern_confidence, pattern_win_rate = get_pattern_signal(pattern_key)

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
        "volatility": volatility,
        "confidence": confidence,
    }


def update_patterns(trade):
    pattern_key = trade.get("pattern_key")
    if pattern_key is None:
        return

    now = time.time()
    pattern = historical_patterns.get(pattern_key)
    if pattern is None:
        historical_patterns[pattern_key] = {
            "wins": 1 if trade["pnl"] > 0 else 0,
            "losses": 0 if trade["pnl"] > 0 else 1,
            "trades_count": 1,
            "last_seen": now,
            "last_win_time": now if trade["pnl"] > 0 else 0,
        }
        return

    pattern["trades_count"] += 1
    if trade["pnl"] > 0:
        pattern["wins"] += 1
        pattern["last_win_time"] = now
    else:
        pattern["losses"] += 1
    pattern["last_seen"] = now


def format_signal(value):
    return f"{value:+.2f}" if value is not None else "n/a"


def format_float(value, precision=4):
    if value is None:
        return "n/a"
    return f"{value:.{precision}f}"


def evaluate_decision(price):
    global state, entry_price, entry_time
    global total_pnl, wins, losses
    global equity, total_gains, total_losses
    global last_probability, last_components, entry_snapshot

    now = time.time()

    with lock:
        prob, components = compute_probability()
        last_probability = prob
        last_components = components

        if state == "FORA":
            if prob >= ENTRY_THRESHOLD:
                state = "DENTRO"
                entry_price = price
                entry_time = now
                entry_snapshot = {
                    "prob": prob,
                    "components": dict(components),
                    "pattern_key": components.get("pattern_key"),
                }

        elif state == "DENTRO":
            if entry_price is None or entry_time is None:
                state = "FORA"
                entry_snapshot = None
                return

            pnl_pct = (price - entry_price) / entry_price
            trade_duration = now - entry_time

            stop_financeiro = pnl_pct <= -MAX_LOSS_PCT
            stop_fluxo = prob <= FLOW_STOP_THRESHOLD
            stop_tempo = trade_duration >= MAX_TRADE_TIME
            exit_prob = prob <= EXIT_THRESHOLD

            if stop_financeiro or stop_fluxo or stop_tempo or exit_prob:
                pnl = price - entry_price
                total_pnl += pnl
                equity += pnl

                if pnl > 0:
                    wins += 1
                    total_gains += pnl
                else:
                    losses += 1
                    total_losses += abs(pnl)

                entry_components = entry_snapshot["components"] if entry_snapshot else {}
                trade_history.append({
                    "entry_price": entry_price,
                    "exit_price": price,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct * 100,
                    "duration": trade_duration,
                    "entry_prob": entry_snapshot["prob"] if entry_snapshot else None,
                    "exit_prob": prob,
                    "entry_confidence": entry_components.get("confidence"),
                    "exit_confidence": components.get("confidence"),
                    "exit_reason": (
                        "STOP_FINANCEIRO" if stop_financeiro else
                        "STOP_FLUXO" if stop_fluxo else
                        "STOP_TEMPO" if stop_tempo else
                        "EXIT_PROB"
                    ),
                    "pattern_key": entry_snapshot["pattern_key"] if entry_snapshot else None,
                    "signals": {
                        "flow": entry_components.get("flow"),
                        "short_flow": entry_components.get("short_flow"),
                        "momentum": entry_components.get("momentum"),
                        "pattern": entry_components.get("pattern"),
                    },
                    "buy_volume": buy_volume,
                    "sell_volume": sell_volume,
                })

                update_patterns(trade_history[-1])

                state = "FORA"
                entry_price = None
                entry_time = None
                entry_snapshot = None


def print_status():
    with lock:
        total_trades = wins + losses
        winrate = (wins / total_trades * 100) if total_trades > 0 else 0
        equity_pct = ((equity - INITIAL_EQUITY) / INITIAL_EQUITY * 100)
        confidence = last_components.get("confidence") if last_components else None
        volatility = last_components.get("volatility") if last_components else None
        pattern_key = last_components.get("pattern_key") if last_components else None
        pattern_win_rate = last_components.get("pattern_win_rate") if last_components else None

        print(f"""
STATE: {state}
PRICE: {last_price:.2f}

PROB: {last_probability:.2f} | CONF: {format_float(confidence, 2)} | VOL: {format_float(volatility, 5)}
SIGNALS: FLOW={format_signal(last_components.get("flow") if last_components else None)} SHORT={format_signal(last_components.get("short_flow") if last_components else None)} MOM={format_signal(last_components.get("momentum") if last_components else None)} PAT={format_signal(last_components.get("pattern") if last_components else None)}
PATTERN: {pattern_key if pattern_key is not None else "n/a"} | WR: {format_float(pattern_win_rate, 2)}

BUY VOL:  {buy_volume:.4f} | SHORT: {short_buy_volume:.4f}
SELL VOL: {sell_volume:.4f} | SHORT: {short_sell_volume:.4f}

TRADES: {total_trades}
WINS: {wins}
LOSSES: {losses}
WINRATE: {winrate:.2f}%

PATRIMÔNIO: {equity:.2f} ({equity_pct:+.2f}%)
----------------------------------
""")


def console_loop():
    while True:
        print_status()
        time.sleep(0.5)

# ================= WEBSOCKET =================
def on_message(ws, message):
    global buy_volume, sell_volume, last_price
    global short_buy_volume, short_sell_volume

    data = json.loads(message)
    price = float(data["p"])
    qty = float(data["q"])
    is_sell = data["m"]

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
        trades_window.append({"side": side, "qty": qty})
        short_trades_window.append({"side": side, "qty": qty})

        if side == "buy":
            buy_volume += qty
            short_buy_volume += qty
        else:
            sell_volume += qty
            short_sell_volume += qty

    evaluate_decision(price)


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
print(f"BotTrader v{APP_VERSION} iniciado para {SYMBOL}")
threading.Thread(target=start_ws, daemon=True).start()
threading.Thread(target=console_loop, daemon=True).start()

while True:
    time.sleep(1)
