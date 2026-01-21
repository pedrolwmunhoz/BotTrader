# BotTrader

Bot de trading em tempo real para Binance baseado em probabilidades e pesos.
As decisoes de entrada e saida sao geradas a partir de sinais de fluxo de
ordens, momentum de preco e padroes historicos, com ajuste por volatilidade.

## Versao
- 0.2.0

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

## Configuracao rapida
Principais parametros em main.py:
- SYMBOL, WINDOW_TRADES, SHORT_WINDOW_TRADES, PRICE_WINDOW
- ENTRY_THRESHOLD, EXIT_THRESHOLD
- MAX_LOSS_PCT, MAX_TRADE_TIME, FLOW_STOP_THRESHOLD
- WEIGHTS, VOLATILITY_MAX, MOMENTUM_LOOKBACK, PATTERN_BUCKET_STEP

## Observacoes
- Sem mocks: o bot consome dados reais via WebSocket.
- Consulte CHANGELOG.md para detalhes de alteracoes.