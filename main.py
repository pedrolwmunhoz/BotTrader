import websocket
import json
import threading
import time
from collections import deque

# ================= CONFIGURAÇÃO =================
SYMBOL = "btcusdt"
WINDOW_TRADES = 500
MIN_LOOKBACK = 50

ENTRY_THRESHOLD = 0.65
EXIT_THRESHOLD = 0.45

MAX_LOSS_PCT = 0.002
MAX_TRADE_TIME = 60
FLOW_STOP_THRESHOLD = 0.35

PATTERN_EXPIRY = 3600  # 1 hora

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

trades_window = deque(maxlen=WINDOW_TRADES)
trade_history = []

total_pnl = 0.0
wins = 0
losses = 0

last_price = 0.0

# ================= PADRÕES VIVOS =================
historical_patterns = []

# ================= FUNÇÕES =================
def calculate_probability():
    if len(trades_window) < MIN_LOOKBACK:
        return 0.5

    total = buy_volume + sell_volume
    if total == 0:
        return 0.5

    delta = (buy_volume - sell_volume) / total
    prob = 0.5 + delta / 2
    return max(0.0, min(1.0, prob))


def adjust_probability(prob):
    score = 1.0
    now = time.time()

    for pattern in historical_patterns:
        if now - pattern["last_win_time"] > PATTERN_EXPIRY:
            continue

        if (
            abs(buy_volume - pattern["buy_volume"]) < 0.01
            and abs(sell_volume - pattern["sell_volume"]) < 0.01
        ):
            score *= 1 + pattern["win_rate_recent"] / 2
        else:
            score *= 1 - pattern["loss_rate_recent"] / 2

    return max(0.0, min(1.0, prob * score))


def update_patterns(trade):
    found = False
    now = time.time()

    for pattern in historical_patterns:
        if (
            abs(trade["buy_volume"] - pattern["buy_volume"]) < 0.01
            and abs(trade["sell_volume"] - pattern["sell_volume"]) < 0.01
        ):
            pattern["trades_count"] += 1

            if trade["pnl"] > 0:
                pattern["wins"] += 1
                pattern["last_win_time"] = now
            else:
                pattern["losses"] += 1

            pattern["win_rate_recent"] = pattern["wins"] / pattern["trades_count"]
            pattern["loss_rate_recent"] = pattern["losses"] / pattern["trades_count"]
            found = True
            break

    if not found:
        historical_patterns.append({
            "buy_volume": trade["buy_volume"],
            "sell_volume": trade["sell_volume"],
            "wins": 1 if trade["pnl"] > 0 else 0,
            "losses": 0 if trade["pnl"] > 0 else 1,
            "trades_count": 1,
            "last_win_time": now if trade["pnl"] > 0 else 0,
            "win_rate_recent": 1.0 if trade["pnl"] > 0 else 0.0,
            "loss_rate_recent": 0.0 if trade["pnl"] > 0 else 1.0
        })


def evaluate_decision(price):
    global state, entry_price, entry_time
    global total_pnl, wins, losses
    global equity, total_gains, total_losses

    prob = adjust_probability(calculate_probability())
    now = time.time()

    with lock:
        if state == "FORA":
            if prob >= ENTRY_THRESHOLD:
                state = "DENTRO"
                entry_price = price
                entry_time = now

        elif state == "DENTRO":
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

                trade_history.append({
                    "entry_price": entry_price,
                    "exit_price": price,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct * 100,
                    "duration": trade_duration,
                    "exit_reason": (
                        "STOP_FINANCEIRO" if stop_financeiro else
                        "STOP_FLUXO" if stop_fluxo else
                        "STOP_TEMPO" if stop_tempo else
                        "EXIT_PROB"
                    ),
                    "buy_volume": buy_volume,
                    "sell_volume": sell_volume
                })

                update_patterns(trade_history[-1])

                state = "FORA"
                entry_price = None
                entry_time = None


def print_status():
    with lock:
        total_trades = wins + losses
        winrate = (wins / total_trades * 100) if total_trades > 0 else 0
        equity_pct = ((equity - INITIAL_EQUITY) / INITIAL_EQUITY * 100)

        print(f"""
STATE: {state}
PRICE: {last_price:.2f}

BUY VOL:  {buy_volume:.4f}
SELL VOL: {sell_volume:.4f}

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

    data = json.loads(message)
    price = float(data["p"])
    qty = float(data["q"])
    is_sell = data["m"]

    with lock:
        last_price = price

        if len(trades_window) == trades_window.maxlen:
            old = trades_window.popleft()
            if old["side"] == "buy":
                buy_volume -= old["qty"]
            else:
                sell_volume -= old["qty"]

        side = "sell" if is_sell else "buy"
        trades_window.append({"side": side, "qty": qty})

        if side == "buy":
            buy_volume += qty
        else:
            sell_volume += qty

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
threading.Thread(target=start_ws, daemon=True).start()
threading.Thread(target=console_loop, daemon=True).start()

while True:
    time.sleep(1)
