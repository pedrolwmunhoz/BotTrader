import json
import math
import os
import threading
import time
from collections import deque

import websocket

# ================= CONFIGURAÇÃO =================
APP_VERSION = "0.4.0"

SYMBOL = "btcusdt"  # BTC/USDT Futuros USDⓈ-M
WINDOW_TRADES = 200  # janela curta para entrada
HOLD_WINDOW_TRADES = 800  # janela longa para permanência/saída
SHORT_WINDOW_TRADES = 80
PRICE_WINDOW = 200
MIN_LOOKBACK = 50
MOMENTUM_LOOKBACK = 30
VOLATILITY_LOOKBACK = 60

ENTRY_THRESHOLD = 0.65
POTENTIAL_WEIGHT = 0.15  # Peso do potencial na probabilidade final (15%)
EXIT_THRESHOLD = 0.45

# Zonas de fluxo (manter a mão em pullbacks)
FLOW_TREND_THRESHOLD = ENTRY_THRESHOLD
FLOW_HOLD_THRESHOLD = EXIT_THRESHOLD

# Delta acumulado e reversão confirmada
CUM_DELTA_WINDOW = 120
CUM_DELTA_EXIT_THRESHOLD = -0.12
AGGRESSION_RATIO_EXIT = 0.45
REVERSAL_MIN_SIGNALS = 2
REVERSAL_MIN_SIGNALS_STRICT = 3

# Falha estrutural e absorção
FAIL_HIGH_PCT = 0.0015
ABSORPTION_LOOKBACK = 20
ABSORPTION_STALL_PCT = 0.0003
ABSORPTION_FLOW_THRESHOLD = 0.25
VOLUME_SURGE_MULTIPLIER = 1.4
VOLUME_FLOW_THRESHOLD = 0.2

MAX_LOSS_PCT = 0.002
MAX_TRADE_TIME = 60
FLOW_STOP_THRESHOLD = 0.35
LEVERAGE = 3.0
MIN_EQUITY_TO_TRADE = 10.0  # Saldo mínimo para operar
MIN_PRICE = 1.0  # Preço mínimo válido (proteção contra dados zerados)

# Taxas Binance (Futuros USDⓈ-M - Taker)
# Taker: 0.04% por operação
# Entrada: 0.04% | Saída: 0.04% = 0.08% total por trade
BINANCE_FEE_RATE = 0.0004  # 0.04% por operação (taker)
SLIPPAGE_RATE = 0.0  # Takers não têm slippage significativo (ordens market)
MIN_PROFIT_POTENTIAL = 0.0008  # 0.08% mínimo de potencial de lucro para compensar taxas (entrada + saída)
MIN_EXIT_PROFIT_PCT = MIN_PROFIT_POTENTIAL  # evita sair com lucro menor que taxas

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
total_fees_unleveraged = 0.0
total_fees_leveraged = 0.0
total_fees_unleveraged = 0.0
total_fees_leveraged = 0.0
total_fees_unleveraged = 0.0
total_fees_leveraged = 0.0

# ================= ESTADO =================
state = "FORA"
entry_price = None
entry_time = None
peak_price = None

buy_volume = 0.0
sell_volume = 0.0
short_buy_volume = 0.0
short_sell_volume = 0.0
hold_buy_volume = 0.0
hold_sell_volume = 0.0

trades_window = deque(maxlen=WINDOW_TRADES)
short_trades_window = deque(maxlen=SHORT_WINDOW_TRADES)
hold_trades_window = deque(maxlen=HOLD_WINDOW_TRADES)
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

DOC_TEXT = """
##############################################
#           MANUAL BOTTRADER (VGA)           #
##############################################

DOCUMENTACAO TECNICA DETALHADA

1) Fluxo de dados (WebSocket)
O bot consome trades em tempo real do endpoint:
wss://stream.binance.com:9443/ws/{SYMBOL}@trade
Campos usados:
- p (preco), q (quantidade), m (trade agressor: True = venda)
- T (timestamp do trade) ou E (event time)

Esses trades alimentam quatro janelas:
- trades_window: janela curta de entrada (WINDOW_TRADES)
- hold_trades_window: janela longa de permanencia (HOLD_WINDOW_TRADES)
- short_trades_window: janela curta (SHORT_WINDOW_TRADES)
- price_window: janela de precos (PRICE_WINDOW)

2) Sinal de fluxo (order flow)
O fluxo mede o desequilibrio entre volume comprador e vendedor:
flow = (buy_volume - sell_volume) / (buy_volume + sell_volume)
Esse sinal reflete quem esta dominando o book agressor.
O valor e "clampado" para o intervalo [-1, 1].
Agora usamos duas janelas:
- fluxo de entrada (curta) para timing
- fluxo de permanencia (longa) para sair com menos ruido

Por que funciona:
quando o volume agressor de compra domina, o preco tende
a manter pressao de alta no curto prazo, e o inverso para venda.

3) Sinal de fluxo curto
Mesmo calculo do fluxo, mas usando short_trades_window.
Ele captura mudancas rapidas de microestrutura que o fluxo longo pode diluir.

4) Sinal de fluxo longo (hold)
Mesmo calculo do fluxo, mas usando hold_trades_window.
Ele guia a permanencia e a saida para evitar stopar em ruido.

5) Volatilidade
Calcula-se o desvio padrao dos retornos recentes:
return[i] = (price[i] - price[i-1]) / price[i-1]
volatility = std(return)
Ela entra como fator de reducao de confianca.

Por que funciona:
maior volatilidade indica maior ruido, reduzindo confianca na direcao.

6) Momentum
O momentum mede a variacao relativa entre o primeiro e o ultimo preco da janela:
raw = (last - first) / first
signal = tanh((raw / (volatility + EPSILON)) * MOMENTUM_SCALE)
Se a volatilidade nao estiver disponivel, usa-se MOMENTUM_VOL_FALLBACK.

Por que funciona:
tendencias sustentadas geram retornos consistentes,
e o tanh evita explodir em regimes extremos.

7) Padroes historicos
Os sinais (flow, short_flow, momentum) sao discretizados:
bucket = round(signal / PATTERN_BUCKET_STEP)
pattern_key = (bucket_flow, bucket_short_flow, bucket_momentum)
O historico e filtrado por:
- janela recente (HISTORY_LOOKBACK_DAYS)
- bucket de tempo atual (TIME_BUCKET_SECONDS)

Para cada padrao:
win_rate = wins / trades
pattern_signal = clamp(2 * (win_rate - 0.5), -1, 1)

Confianca do padrao:
count_weight = min(1, trades / PATTERN_MIN_TRADES)
recent_weight = min(1, recent_count / PATTERN_MIN_TRADES)
recency_weight = 1 - (now - last_seen) / PATTERN_RECENCY_SECONDS
confidence = count_weight * recent_weight * recency_weight

Padroes antigos (fora de PATTERN_RECENCY_DAYS) sao ignorados.

Por que funciona:
padroes validos precisam ser frequentes e atuais,
evitando vieses de dados muito antigos.

8) Combinacao dos sinais e pesos
Pesos atuais (WEIGHTS):
- flow = 0.45
- short_flow = 0.30
- momentum = 0.15
- pattern = 0.10

Sinal combinado:
raw_signal = sum(weight_i * signal_i) / sum(weight_i)
base_prob = 0.5 + 0.5 * raw_signal

Confianca:
normalized = min(1, volatility / VOLATILITY_MAX)
confidence = MIN_CONFIDENCE + (1 - MIN_CONFIDENCE) * (1 - normalized)

Probabilidade final:
final_prob = 0.5 + (base_prob - 0.5) * confidence

Por que funciona:
o peso maior em fluxo privilegia o dado mais atual
do WebSocket, enquanto a confianca reduz a agressividade em mercados ruidosos.

9) Espacamento do historico de analise
Para evitar excesso de registros, o intervalo e adaptativo:
avg_interval = tempo_medio_entre_trades
analysis_interval = clamp(avg_interval * ANALYSIS_INTERVAL_MULTIPLIER,
                          MIN_ANALYSIS_INTERVAL, MAX_ANALYSIS_INTERVAL)

10) Regras de entrada e saida
Entrada:
- prob >= ENTRY_THRESHOLD

Saida (mantendo a mao em pullbacks):
- stop financeiro: pnl_return <= -MAX_LOSS_PCT (proteção de risco)
- estados de fluxo:
  - TREND: prob >= 0.65 (segura)
  - HOLD:  0.45 <= prob < 0.65 (zona de ruido/absorção, não sai)
  - EXIT:  prob < 0.45 (só sai se houver prova)
- fluxo de permanencia usa janela longa (hold_trades_window)
- reversão confirmada: precisa de pelo menos 2 sinais:
  - falha em fazer novo high (pullback com momentum <= 0)
  - delta acumulado negativo + agressão compradora fraca
  - absorção no lado vendedor (volume alto, preço parado)
  - volume contra o preço (surto de volume vendedor)
- buffer de lucro: se o lucro for menor que as taxas (MIN_EXIT_PROFIT_PCT),
  exige 3 sinais de reversão para evitar sair com ganho residual
- stop por tempo: só é usado quando o fluxo está em EXIT

Por que funciona:
combina risco maximo, deterioracao de fluxo e tempo maximo por trade.

11) Patrimonio e alavancagem
O retorno percentual por trade:
pnl_return = (exit_price - entry_price) / entry_price
Sem alavancagem:
pnl_amount = entry_equity * pnl_return
equity = entry_equity + pnl_amount
Com alavancagem:
pnl_amount_leveraged = entry_equity_leveraged * pnl_return * LEVERAGE
equity_leveraged = entry_equity_leveraged + pnl_amount_leveraged

Ganhos, perdas, acertos, liquido e patrimonio sao mostrados no console.
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
        "flow_state": components.get("flow_state"),
        "hold_flow": components.get("hold_flow"),
        "hold_prob": components.get("hold_prob"),
        "cum_delta_ratio": components.get("cum_delta_ratio"),
        "aggression_ratio": components.get("aggression_ratio"),
        "reversal_count": components.get("reversal_count"),
        "reversal_required": components.get("reversal_required"),
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


def compute_cum_delta(trades, lookback):
    if not trades:
        return None, None, None, None
    trades_list = list(trades)
    start_index = max(0, len(trades_list) - lookback)
    buy = 0.0
    sell = 0.0
    for trade in trades_list[start_index:]:
        qty = trade.get("qty", 0.0)
        if qty <= 0 or not math.isfinite(qty):
            continue
        if trade.get("side") == "buy":
            buy += qty
        else:
            sell += qty
    total = buy + sell
    if total <= 0:
        return None, None, None, None
    cum_delta = buy - sell
    ratio = cum_delta / total
    aggression_ratio = buy / total
    return cum_delta, total, ratio, aggression_ratio


def get_flow_state(probability):
    if probability >= FLOW_TREND_THRESHOLD:
        return "TREND"
    if probability >= FLOW_HOLD_THRESHOLD:
        return "HOLD"
    return "EXIT"


def compute_returns(prices, lookback):
    if len(prices) < 2:
        return []
    start = max(1, len(prices) - lookback)
    returns = []
    for i in range(start, len(prices)):
        prev = prices[i - 1]
        curr = prices[i]
        # Proteção contra preços inválidos
        if prev <= MIN_PRICE or curr <= MIN_PRICE or not math.isfinite(prev) or not math.isfinite(curr):
            continue
        returns.append((curr - prev) / prev)
    return returns


def compute_price_change(prices, lookback):
    if len(prices) < lookback:
        return None
    first = prices[-lookback]
    last = prices[-1]
    if first <= MIN_PRICE or last <= MIN_PRICE or not math.isfinite(first) or not math.isfinite(last):
        return None
    return (last - first) / first


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
    # Proteção contra preços inválidos
    if first <= MIN_PRICE or last <= MIN_PRICE or not math.isfinite(first) or not math.isfinite(last):
        return None
    raw = (last - first) / first
    scale = volatility if volatility is not None else MOMENTUM_VOL_FALLBACK
    signal = math.tanh((raw / (scale + EPSILON)) * MOMENTUM_SCALE)
    return clamp(signal, -1.0, 1.0)


def compute_reversal_signals(price, components, cum_delta_ratio, aggression_ratio, peak_price_value):
    signals = {}
    short_flow_signal = components.get("short_flow")
    momentum_signal = components.get("momentum")

    delta_against = (
        cum_delta_ratio is not None
        and aggression_ratio is not None
        and cum_delta_ratio <= CUM_DELTA_EXIT_THRESHOLD
        and aggression_ratio <= AGGRESSION_RATIO_EXIT
    )
    signals["delta_against"] = delta_against

    failed_high = False
    if peak_price_value is not None and peak_price_value > MIN_PRICE:
        pullback = (peak_price_value - price) / peak_price_value
        if pullback >= FAIL_HIGH_PCT and (momentum_signal is not None and momentum_signal <= 0):
            failed_high = True
    signals["failed_high"] = failed_high

    absorption_sell = False
    price_change_short = compute_price_change(price_window, ABSORPTION_LOOKBACK)
    if (
        price_change_short is not None
        and abs(price_change_short) <= ABSORPTION_STALL_PCT
        and short_flow_signal is not None
        and short_flow_signal <= -ABSORPTION_FLOW_THRESHOLD
    ):
        absorption_sell = True
    signals["absorption_sell"] = absorption_sell

    volume_against = False
    if len(short_trades_window) >= 5 and len(hold_trades_window) >= 5:
        short_total = short_buy_volume + short_sell_volume
        long_total = hold_buy_volume + hold_sell_volume
        short_avg = safe_div(short_total, len(short_trades_window), 0.0)
        long_avg = safe_div(long_total, len(hold_trades_window), 0.0)
        if (
            long_avg > 0
            and short_avg >= long_avg * VOLUME_SURGE_MULTIPLIER
            and short_flow_signal is not None
            and short_flow_signal <= -VOLUME_FLOW_THRESHOLD
            and (momentum_signal is None or momentum_signal <= 0)
        ):
            volume_against = True
    signals["volume_against"] = volume_against

    return signals


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
            "estimated_move_pct": 0.0,  # Sem sinais, potencial zero
        }

    weighted_sum = sum(weights[name] * signals[name] for name in weights)
    weight_total = sum(weights.values())
    raw_signal = weighted_sum / weight_total
    base_prob = 0.5 + 0.5 * raw_signal

    confidence = compute_confidence(volatility)
    base_final_prob = 0.5 + (base_prob - 0.5) * confidence

    # Calcular potencial de lucro estimado (probabilístico)
    # Usa a probabilidade e movimento esperado para calcular valor esperado
    # Valor esperado = (prob × ganho_esperado) - ((1 - prob) × perda_esperada)
    vol_adj = volatility if volatility is not None and volatility > 0 else MOMENTUM_VOL_FALLBACK
    momentum_factor = abs(momentum_signal) if momentum_signal is not None else 0.0
    flow_factor = abs(flow_signal) if flow_signal is not None else 0.0
    signal_strength = (abs(raw_signal) * 0.6 + momentum_factor * 0.3 + flow_factor * 0.1)
    
    # Movimento esperado se der certo (baseado na força do sinal)
    expected_gain_pct = signal_strength * vol_adj * confidence * 3.0  # Movimento esperado em % se acertar
    # Perda esperada se der errado (stop loss)
    expected_loss_pct = MAX_LOSS_PCT  # 0.2% (stop loss)
    
    # Valor esperado probabilístico: (prob × ganho) - ((1 - prob) × perda)
    # Mas como só entra se prob >= 0.65, ajusta para considerar apenas probabilidades altas
    prob_adjusted = (base_final_prob - 0.5) * 2.0  # Normaliza para 0-1 quando prob >= 0.5
    expected_value_pct = (prob_adjusted * expected_gain_pct) - ((1.0 - prob_adjusted) * expected_loss_pct)
    
    estimated_move_pct = max(0.0, expected_value_pct)  # Potencial líquido esperado
    
    # Ajustar probabilidade final baseado no potencial
    # Potencial > 0.08% aumenta prob, potencial < 0.08% diminui prob
    potential_factor = clamp((estimated_move_pct / MIN_PROFIT_POTENTIAL), 0.5, 2.0)  # 0.5x a 2.0x
    potential_adjustment = (potential_factor - 1.0) * POTENTIAL_WEIGHT  # Ajuste de -7.5% a +15%
    final_prob = clamp(base_final_prob + potential_adjustment, 0.0, 1.0)
    
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
            "estimated_move_pct": estimated_move_pct,
        }


def format_signal(value):
    return f"{value:+.2f}" if value is not None else "n/a"


def format_float(value, precision=4):
    if value is None:
        return "n/a"
    return f"{value:.{precision}f}"


def evaluate_decision(price, timestamp):
    global state, entry_price, entry_time, peak_price
    global total_pnl, wins, losses
    global equity, equity_leveraged
    global total_pnl_leveraged
    global total_gains, total_losses
    global total_gains_leveraged, total_losses_leveraged
    global total_fees_unleveraged, total_fees_leveraged
    global last_probability, last_components, entry_snapshot

    # Ignora completamente se preço inválido - não analisa nada
    if price is None or price <= MIN_PRICE or not math.isfinite(price):
        return  # Ignora completamente, não processa nada

    now = timestamp

    with lock:
        prob, components = compute_probability(now)
        hold_flow_signal = compute_flow_signal(hold_buy_volume, hold_sell_volume)
        hold_prob = 0.5 + 0.5 * hold_flow_signal if hold_flow_signal is not None else prob
        flow_state = get_flow_state(hold_prob)
        _, _, cum_delta_ratio, aggression_ratio = compute_cum_delta(hold_trades_window, CUM_DELTA_WINDOW)

        components["flow_state"] = flow_state
        components["hold_flow"] = hold_flow_signal
        components["hold_prob"] = hold_prob
        components["cum_delta_ratio"] = cum_delta_ratio
        components["aggression_ratio"] = aggression_ratio
        components["reversal_count"] = 0
        components["reversal_signals"] = {}

        last_probability = prob
        last_components = components

        if state == "FORA":
            # Validar saldo antes de entrar
            can_enter = equity >= MIN_EQUITY_TO_TRADE

            if can_enter and prob >= ENTRY_THRESHOLD:
                # Validação adicional: preço deve ser válido
                if price is None or price <= MIN_PRICE or not math.isfinite(price):
                    return  # Preço inválido, não entra
                
                state = "DENTRO"
                entry_price = price
                entry_time = now
                peak_price = price
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
                peak_price = None
                return

            # Proteção contra preços inválidos durante o trade
            if price is None or price <= MIN_PRICE or not math.isfinite(price):
                return  # Ignora atualizações com preço inválido, não faz nada

            # Proteção no cálculo de PnL
            if entry_price <= MIN_PRICE or not math.isfinite(entry_price):
                return  # Entry price inválido, ignora
            
            pnl_return = (price - entry_price) / entry_price
            # Proteção contra PnL inválido
            if not math.isfinite(pnl_return):
                return  # PnL inválido, ignora
            
            trade_duration = now - entry_time

            if peak_price is None or price > peak_price:
                peak_price = price

            required_signals = REVERSAL_MIN_SIGNALS
            if pnl_return > 0 and pnl_return < MIN_EXIT_PROFIT_PCT:
                required_signals = REVERSAL_MIN_SIGNALS_STRICT

            reversal_signals = compute_reversal_signals(
                price,
                components,
                cum_delta_ratio,
                aggression_ratio,
                peak_price,
            )
            reversal_count = sum(1 for active in reversal_signals.values() if active)
            reversal_confirmed = reversal_count >= required_signals
            components["reversal_count"] = reversal_count
            components["reversal_signals"] = reversal_signals
            components["reversal_required"] = required_signals
            components["peak_price"] = peak_price

            stop_financeiro = pnl_return <= -MAX_LOSS_PCT
            stop_fluxo = prob <= FLOW_STOP_THRESHOLD and reversal_confirmed
            stop_tempo = trade_duration >= MAX_TRADE_TIME and flow_state == "EXIT"
            exit_reversao = reversal_confirmed and flow_state != "TREND"

            if stop_financeiro or stop_fluxo or stop_tempo or exit_reversao:
                entry_equity = entry_snapshot.get("equity") if entry_snapshot else equity
                entry_equity_leveraged = (
                    entry_snapshot.get("equity_leveraged") if entry_snapshot else equity_leveraged
                )
                pnl_amount = entry_equity * pnl_return
                pnl_amount_leveraged = entry_equity_leveraged * pnl_return * LEVERAGE

                # Calcular taxas deste trade (Taker)
                # Taker: 0.04% entrada + 0.04% saída = 0.08% total por trade
                trade_fee_unleveraged = entry_equity * BINANCE_FEE_RATE * 2  # Entrada + Saída
                trade_fee_leveraged = entry_equity_leveraged * LEVERAGE * BINANCE_FEE_RATE * 2  # Entrada + Saída
                total_fees_unleveraged += trade_fee_unleveraged
                total_fees_leveraged += trade_fee_leveraged

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
                reversal_flags = [name for name, active in reversal_signals.items() if active]
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
                    "entry_flow_state": entry_components.get("flow_state"),
                    "exit_flow_state": flow_state,
                    "entry_hold_flow": entry_components.get("hold_flow"),
                    "exit_hold_flow": hold_flow_signal,
                    "entry_hold_prob": entry_components.get("hold_prob"),
                    "exit_hold_prob": hold_prob,
                    "exit_cum_delta_ratio": cum_delta_ratio,
                    "exit_aggression_ratio": aggression_ratio,
                    "exit_reversal_count": reversal_count,
                    "exit_reversal_required": required_signals,
                    "exit_reversal_signals": reversal_flags,
                    "peak_price": peak_price,
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
                        "REVERSAO_CONFIRMADA" if exit_reversao else
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
                peak_price = None

        record_analysis_snapshot(now, price, prob, components)


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
        gains_pct = (total_gains / INITIAL_EQUITY * 100) if INITIAL_EQUITY > 0 else 0
        gains_leveraged_pct = (total_gains_leveraged / INITIAL_EQUITY * 100) if INITIAL_EQUITY > 0 else 0
        losses_pct = (total_losses / INITIAL_EQUITY * 100) if INITIAL_EQUITY > 0 else 0
        losses_leveraged_pct = (total_losses_leveraged / INITIAL_EQUITY * 100) if INITIAL_EQUITY > 0 else 0
        
        # Taxas já estão acumuladas nas variáveis globais (atualizadas a cada trade)
        fees_pct_unleveraged = (total_fees_unleveraged / INITIAL_EQUITY * 100) if INITIAL_EQUITY > 0 else 0
        fees_pct_leveraged = (total_fees_leveraged / INITIAL_EQUITY * 100) if INITIAL_EQUITY > 0 else 0
        
        # Patrimônio líquido após taxas
        equity_after_fees = equity - total_fees_unleveraged
        equity_leveraged_after_fees = equity_leveraged - total_fees_leveraged
        
        confidence = last_components.get("confidence") if last_components else None
        volatility = last_components.get("volatility") if last_components else None
        pattern_key = last_components.get("pattern_key") if last_components else None
        pattern_win_rate = last_components.get("pattern_win_rate") if last_components else None
        flow_state = last_components.get("flow_state") if last_components else None
        cum_delta_ratio = last_components.get("cum_delta_ratio") if last_components else None
        aggression_ratio = last_components.get("aggression_ratio") if last_components else None
        reversal_count = last_components.get("reversal_count") if last_components else None
        reversal_required = last_components.get("reversal_required") if last_components else None
        hold_flow = last_components.get("hold_flow") if last_components else None
        hold_prob = last_components.get("hold_prob") if last_components else None
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
STATE: {state} | FLOW: {flow_state if flow_state else "n/a"} | PRICE: {last_price:.2f}
{section}
PROB: {last_probability:.2f} | CONF: {format_float(confidence, 2)} | VOL: {format_float(volatility, 5)}
REV: {reversal_count if reversal_count is not None else "n/a"}/{reversal_required if reversal_required is not None else "n/a"} | CΔ: {format_signal(cum_delta_ratio)} | AGR: {format_float(aggression_ratio, 2)}
HOLD: FLOW={format_signal(hold_flow)} PROB={format_float(hold_prob, 2)}
SIGNALS: FLOW={format_signal(last_components.get("flow") if last_components else None)} SHORT={format_signal(last_components.get("short_flow") if last_components else None)} MOM={format_signal(last_components.get("momentum") if last_components else None)} PAT={format_signal(last_components.get("pattern") if last_components else None)}
PATTERN: {pattern_key if pattern_key is not None else "n/a"} | WR: {format_float(pattern_win_rate, 2)}
{section}
BUY VOL:  {buy_volume:.4f} | SHORT: {short_buy_volume:.4f}
SELL VOL: {sell_volume:.4f} | SHORT: {short_sell_volume:.4f}
{section}
TRADES: {total_trades} | ACERTOS: {wins} | ERROS: {losses} | WINRATE: {winrate:.2f}%
GANHOS: +${total_gains:.2f} ({gains_pct:+.2f}%) (sem alav) | +${total_gains_leveraged:.2f} ({gains_leveraged_pct:+.2f}%) (com alav)
PERDAS: -${total_losses:.2f} ({losses_pct:+.2f}%) (sem alav) | -${total_losses_leveraged:.2f} ({losses_leveraged_pct:+.2f}%) (com alav)
TAXAS: -${total_fees_unleveraged:.2f} ({fees_pct_unleveraged:+.2f}%) (sem alav) | -${total_fees_leveraged:.2f} ({fees_pct_leveraged:+.2f}%) (com alav)
TOTAL PATRIMÔNIO LÍQUIDO: ${equity:.2f} | ${equity_leveraged:.2f}
TOTAL PATRIMÔNIO APÓS TAXAS: ${equity_after_fees:.2f} | ${equity_leveraged_after_fees:.2f}
HISTORY: {history_count}
{section}
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


def get_page_size():
    try:
        height = os.get_terminal_size().lines
    except OSError:
        height = 24
    return max(10, height - 6)


def paginate_text(text, title):
    lines = text.splitlines()
    total = len(lines)
    if total == 0:
        print("README vazio.")
        input("Pressione ENTER para voltar ao menu...")
        return

    page_size = get_page_size()
    index = 0
    border = "=" * 78

    while True:
        end = min(total, index + page_size)
        print(f"\n{border}\n{title} ({index + 1}-{end} de {total})\n{border}")
        for line in lines[index:end]:
            print(line)
        print(border)

        prompt = "N=proxima, P=anterior, Q=sair: "
        choice = input(prompt).strip().lower()
        if choice in ("q", "s", "sair", "exit"):
            break
        if choice in ("p", "prev", "anterior"):
            index = max(0, index - page_size)
            continue
        if choice in ("n", "next", "proxima", ""):
            if end >= total:
                break
            index = end


def show_documentation():
    paginate_text(DOC_TEXT, "MANUAL - BOTTRADER")


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
    global hold_buy_volume, hold_sell_volume

    data = json.loads(message)
    price = float(data["p"])
    qty = float(data["q"])
    is_sell = data["m"]
    trade_time = data.get("T") or data.get("E")
    timestamp = trade_time / 1000 if trade_time is not None else time.time()

    # Proteção contra preços inválidos ou zerados - ignora completamente o tick
    if price is None or price <= MIN_PRICE or not math.isfinite(price):
        return  # Ignora completamente, não processa nada
    
    if qty is None or qty <= 0 or not math.isfinite(qty):
        return  # Ignora completamente, não processa nada

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

        if len(hold_trades_window) == hold_trades_window.maxlen:
            old_hold = hold_trades_window.popleft()
            if old_hold["side"] == "buy":
                hold_buy_volume -= old_hold["qty"]
            else:
                hold_sell_volume -= old_hold["qty"]

        side = "sell" if is_sell else "buy"
        trades_window.append({"side": side, "qty": qty, "ts": timestamp})
        short_trades_window.append({"side": side, "qty": qty, "ts": timestamp})
        hold_trades_window.append({"side": side, "qty": qty, "ts": timestamp})

        if side == "buy":
            buy_volume += qty
            short_buy_volume += qty
            hold_buy_volume += qty
        else:
            sell_volume += qty
            short_sell_volume += qty
            hold_sell_volume += qty

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
    # WebSocket para Futuros USDⓈ-M (não Spot)
    # Em futuros, os trades são contratos, não moedas físicas
    url = f"wss://fstream.binance.com/ws/{SYMBOL}@trade"
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
