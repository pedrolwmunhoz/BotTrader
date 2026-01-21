# BotTrader

Bot de trading em tempo real para Binance baseado em probabilidades e pesos.
As decisoes de entrada e saida sao geradas a partir de sinais de fluxo de
ordens, momentum de preco e padroes historicos, com ajuste por volatilidade.

## Versao
- 0.3.5

## Requisitos
- Python 3.10+
- websocket-client

Instalacao:
pip install websocket-client

## Como executar
python main.py

## Como a probabilidade e calculada
A probabilidade final parte de um sinal ponderado entre:
- fluxo (desequilibrio entre volume de compra e venda)
- fluxo curto (janela curta para detectar mudancas rapidas)
- momentum (variacao de preco na janela configurada)
- padroes (taxa de acerto de cenarios similares)

Cada sinal gera um valor entre -1 e 1. Os pesos sao normalizados e o resultado
e convertido para o intervalo [0, 1]. A confianca e reduzida quando a
volatilidade esta alta, aproximando a probabilidade de 0.5.

## Documentacao tecnica detalhada

### 1) Fluxo de dados (WebSocket)
O bot consome trades em tempo real do endpoint:
`wss://stream.binance.com:9443/ws/{SYMBOL}@trade`.
Campos usados:
- `p` (preco), `q` (quantidade), `m` (trade agressor: True = venda)
- `T` (timestamp do trade) ou `E` (event time)

Esses trades alimentam tres janelas:
- `trades_window`: janela principal (WINDOW_TRADES)
- `short_trades_window`: janela curta (SHORT_WINDOW_TRADES)
- `price_window`: janela de precos (PRICE_WINDOW)

### 2) Sinal de fluxo (order flow)
O fluxo mede o desequilibrio entre volume comprador e vendedor:
```
flow = (buy_volume - sell_volume) / (buy_volume + sell_volume)
```
Esse sinal reflete quem esta dominando o book agressor.
O valor e "clampado" para o intervalo [-1, 1].

**Por que funciona:** quando o volume agressor de compra domina, o preco tende
a manter pressao de alta no curto prazo, e o inverso para venda.

### 3) Sinal de fluxo curto
Mesmo calculo do fluxo, mas usando `short_trades_window`.
Ele captura mudancas rapidas de microestrutura que o fluxo longo pode diluir.

### 4) Volatilidade
Calcula-se o desvio padrao dos retornos recentes:
```
return[i] = (price[i] - price[i-1]) / price[i-1]
volatility = std(return)
```
Ela entra como fator de reducao de confianca.

**Por que funciona:** maior volatilidade indica maior ruido, reduzindo
confianca na direcao.

### 5) Momentum
O momentum mede a variacao relativa entre o primeiro e o ultimo preco da janela:
```
raw = (last - first) / first
signal = tanh((raw / (volatility + EPSILON)) * MOMENTUM_SCALE)
```
Se a volatilidade nao estiver disponivel, usa-se `MOMENTUM_VOL_FALLBACK`.

**Por que funciona:** tendencias sustentadas geram retornos consistentes,
e o `tanh` evita explosoes em regimes extremos.

### 6) Padroes historicos
Os sinais (flow, short_flow, momentum) sao discretizados:
```
bucket = round(signal / PATTERN_BUCKET_STEP)
pattern_key = (bucket_flow, bucket_short_flow, bucket_momentum)
```
O historico e filtrado por:
- janela recente (`HISTORY_LOOKBACK_DAYS`)
- bucket de tempo atual (`TIME_BUCKET_SECONDS`)

Para cada padrao:
```
win_rate = wins / trades
pattern_signal = clamp(2 * (win_rate - 0.5), -1, 1)
```
Confianca do padrao:
```
count_weight = min(1, trades / PATTERN_MIN_TRADES)
recent_weight = min(1, recent_count / PATTERN_MIN_TRADES)
recency_weight = 1 - (now - last_seen) / PATTERN_RECENCY_SECONDS
confidence = count_weight * recent_weight * recency_weight
```
Padroes antigos (fora de `PATTERN_RECENCY_DAYS`) sao ignorados.

**Por que funciona:** padroes validos precisam ser frequentes e atuais,
evitando vieses de dados muito antigos.

### 7) Combinacao dos sinais e pesos
Pesos atuais (WEIGHTS):
- flow = 0.45
- short_flow = 0.30
- momentum = 0.15
- pattern = 0.10

Sinal combinado:
```
raw_signal = sum(weight_i * signal_i) / sum(weight_i)
base_prob = 0.5 + 0.5 * raw_signal
```

Confianca:
```
normalized = min(1, volatility / VOLATILITY_MAX)
confidence = MIN_CONFIDENCE + (1 - MIN_CONFIDENCE) * (1 - normalized)
```

Probabilidade final:
```
final_prob = 0.5 + (base_prob - 0.5) * confidence
```

**Por que funciona:** o peso maior em fluxo privilegia o dado mais atual
do WebSocket, enquanto a confianca reduz a agressividade em mercados ruidosos.

### 8) Espacamento do historico de analise
Para evitar excesso de registros, o intervalo e adaptativo:
```
avg_interval = tempo_medio_entre_trades
analysis_interval = clamp(avg_interval * ANALYSIS_INTERVAL_MULTIPLIER,
                          MIN_ANALYSIS_INTERVAL, MAX_ANALYSIS_INTERVAL)
```

### 9) Regras de entrada e saida
Entrada:
- `prob >= ENTRY_THRESHOLD`

Saida:
- `pnl_return <= -MAX_LOSS_PCT` (stop financeiro)
- `prob <= FLOW_STOP_THRESHOLD` (stop por fluxo)
- `trade_duration >= MAX_TRADE_TIME` (stop tempo)
- `prob <= EXIT_THRESHOLD`

**Por que funciona:** combina risco maximo, deterioracao de fluxo e tempo
maximo por trade.

### 10) Patrimonio e alavancagem
O retorno percentual por trade:
```
pnl_return = (exit_price - entry_price) / entry_price
```
Sem alavancagem:
```
pnl_amount = entry_equity * pnl_return
equity = entry_equity + pnl_amount
```
Com alavancagem:
```
pnl_amount_leveraged = entry_equity_leveraged * pnl_return * LEVERAGE
equity_leveraged = entry_equity_leveraged + pnl_amount_leveraged
```

Ganhos, perdas, acertos, liquido e patrimonio sao mostrados no console.

## Historico e analise temporal
- O bot grava historico completo de negocios em data/trade_history.jsonl.
- O historico de analise de probabilidade fica em data/analysis_history.jsonl.
- Ao analisar uma nova acao, o bot consulta o historico total e filtra por:
  - janela recente (HISTORY_LOOKBACK_DAYS)
  - bucket de tempo atual (TIME_BUCKET_SECONDS)
- Padroes so entram no peso se continuam se repetindo ate os dias atuais
  (PATTERN_RECENCY_DAYS e PATTERN_MIN_TRADES).
- O espacamento do historico de probabilidades e adaptado ao volume de dados
  com ANALYSIS_INTERVAL_MULTIPLIER, MIN_ANALYSIS_INTERVAL e MAX_ANALYSIS_INTERVAL.

## Patrimonio e alavancagem
- Patrimonio sem alavancagem e calculado por variacao percentual do preco
  (composto por operacao) com base em INITIAL_EQUITY.
- Patrimonio com alavancagem usa o mesmo retorno percentual multiplicado por
  LEVERAGE para comparacao.
- Ganhos, perdas, acertos e liquido sao exibidos separadamente no status.

## Configuracao rapida
Principais parametros em main.py:
- SYMBOL, WINDOW_TRADES, SHORT_WINDOW_TRADES, PRICE_WINDOW
- ENTRY_THRESHOLD, EXIT_THRESHOLD
- MAX_LOSS_PCT, MAX_TRADE_TIME, FLOW_STOP_THRESHOLD
- WEIGHTS, VOLATILITY_MAX, MOMENTUM_LOOKBACK, PATTERN_BUCKET_STEP
- PATTERN_RECENCY_DAYS, HISTORY_LOOKBACK_DAYS, TIME_BUCKET_SECONDS
- ANALYSIS_INTERVAL_MULTIPLIER, MIN_ANALYSIS_INTERVAL, MAX_ANALYSIS_INTERVAL
- DATA_DIR, TRADE_HISTORY_FILE, ANALYSIS_HISTORY_FILE

## Pesos do WebSocket
- Os sinais de fluxo (flow e short_flow) tem peso maior para refletir os trades
  em tempo real vindos do WebSocket.

## Menu
- O bot inicia com um menu no console.
- Opcoes: Real time (status em tempo real) e Documentacao.

## Observacoes
- Sem mocks: o bot consome dados reais via WebSocket.
- Consulte CHANGELOG.md para detalhes de alteracoes.