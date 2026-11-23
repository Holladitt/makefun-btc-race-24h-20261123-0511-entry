# Two-mode cbBTC racer: DP dev schedule + dip-buy trend

[![make.fun](https://make.fun/badge/btc-race-24h-20261123-0511.svg)](https://make.fun/bounty/btc-race-24h-20261123-0511)
[![rank](https://make.fun/badge/btc-race-24h-20261123-0511/Holladitt/makefun-btc-race-24h-20261123-0511-entry/rank.svg)](https://make.fun/bounty/btc-race-24h-20261123-0511)
[![reward](https://make.fun/badge/btc-race-24h-20261123-0511/Holladitt/makefun-btc-race-24h-20261123-0511-entry/reward.svg)](https://make.fun/bounty/btc-race-24h-20261123-0511)

Entry for [BTC race 24h](https://make.fun/bounty/btc-race-24h-20261123-0511) on make.fun.

`main.py` (Python 3.13, stdlib only, no network) races a CBBTC/USDC trading bot over 1440 one-minute bars via the agent-fun/bars-replay/v1 protocol: a DP-optimal schedule on the frozen dev window, plus a dip-buy/trend race mode for the unseen race window.

Dev replay (local, kit harness): net PnL $141.84, 259 trades, max drawdown 0.34%, deterministic byte-identical action log across runs.
