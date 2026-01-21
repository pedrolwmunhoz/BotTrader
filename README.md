# BotTrader

Bot de trading em tempo real para Binance baseado em probabilidades e pesos.
As decisoes de entrada e saida sao geradas a partir de sinais de fluxo de
ordens, momentum de preco e padroes historicos, com ajuste por volatilidade.

## Versao
- 0.3.4

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