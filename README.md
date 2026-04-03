# Solana Signal System v2.0

Event-Driven Low-Latency On-Chain Signal System (Solana → Telegram)

## Architecture

```
Helius Webhook (Solana) → FastAPI (ingestion) + DexScreener WS (price)
                              ↓
                        Redis Streams (8 partitions)
                              ↓
                    Consumer Workers (stateful)
                         ↓
    FlowEngine ← PriceEngine ← TokenState
                         ↓
    WalletIntelligence → SignalEngine → AntiManipulation
                         ↓
                   Delivery (Telegram + REST)
```

## Quick Start

```bash
# 1. Copy environment
cp .env.template .env
# Edit .env with your tokens

# 2. Build and run
docker-compose -f infra/docker-compose.yml up -d

# 3. Check logs
docker-compose -f infra/docker-compose.yml logs -f
```

## Modules

| Module | Description |
|--------|-------------|
| `api/` | FastAPI webhook ingestion |
| `consumers/` | Stream consumers and engines |
| `wallet_intelligence/` | Wallet tracking and alpha scoring |
| `backtest/` | Historical backtesting engine |
| `bot/` | Telegram delivery bot |
| `infra/` | Docker Compose infrastructure |

## Environment Variables

See `.env.template` for all configuration options.

## KPI Targets

- Latency p95 < 1000ms
- Deduplication: 0 duplicates
- Precision ≥ 0.60
- Profit Factor ≥ 1.25
- Uptime > 99%
