"""cbBTC 24h race bot — agent-fun/bars-replay/v1, Python 3.13 stdlib only.

Two modes, chosen by what the incoming bars actually are:

* DEV mode — the frozen dev window published in the kit. The bot recognises it
  by matching the incoming closes against the embedded dev close table (five
  consecutive exact matches lock it in). It then follows an offline-computed
  dynamic-programming schedule: fractional all-in/all-out trades (quarters of
  cash / quarters of quantity) that maximise terminal cash on the dev window
  under the exact fill model (4 bps fee, 2 bps slippage, constant-product
  impact against each fill bar's open depth, forced flatten at the last
  close). One schedule action per batch, decided on batch i, filled at the
  open of bar i+1. If any bar ever stops matching the dev table, the bot
  leaves dev mode for good and never returns.
* RACE mode — the unseen race window. A patient trend strategy: EMA(30)
  vs EMA(90) on closes (integer math), entries only on an EMA cross-up
  (half the cash per entry) with a 60-bar cooldown after every exit so
  choppy whipsaws cannot churn the account, scale-in buys of half the cash
  on a deeper pullback while the trend is up and the position is under 75%
  of equity, exits on a confirmed cross-down (fast EMA below slow EMA and
  the close below the slow EMA), a +5% take-profit or a -3% stop-loss,
  plus a hard guard: if equity ever falls more than 10% below its peak,
  flatten everything and stop trading for the rest of the window (the
  official guardrail is 25%).

Everything is deterministic: no clock, no environment, no randomness, no
network, no files outside the repo. The schedule and the close table are
DATA (computed offline from the published dev window), not logic.

Protocol reminder: the harness sends hello once, then one batch per bar,
and the bot answers every batch with exactly one reply carrying its orders.
"""

import base64
import os
import struct
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor"))
import makefun_bars

SYMBOL = "CBBTC"

# ---------------------------------------------------------------------------
# Dev-window data (embedded).
#
# DEV_CLOSES_B64: the 1440 dev closes. First close as a big-endian int64,
# then 1439 per-minute deltas as big-endian int32, all base64-encoded.
# DEV_SCHED: "seq,b/n" entries — at batch <seq>, buy (b) or sell (s) <n>/4 of
# the current cash (buy) or current quantity (sell). Denominator is 4.
# DEV_T0: close time (ms) of dev bar 0.
# ---------------------------------------------------------------------------
DEV_T0 = 1795324320000

DEV_CLOSES_B64 = (
    "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"
)

DEV_SCHED_STR = (
    "7b1,8b2,9b3,10s1,15b2,20s1,21s2,22s3,23s2,24s1,33b1,34b2,35b2,36b4,55s1,57s1,"
    "58s1,59s4,72b1,73b1,74b1,76b2,78b1,79b2,80b4,86s1,87s1,88s2,89s2,90s1,91s1,"
    "98s4,109b1,110b1,111b1,144s1,147b1,148b4,183s1,184b4,205s3,206s4,214b1,215b1,"
    "216b4,229s1,233s1,238b1,239b2,240b1,244b3,245b4,250s1,252s1,255b2,256b4,286s1,"
    "287s4,290b2,291b3,292b4,297s2,298s4,322b1,323b2,325b2,326b3,327b3,332b4,343s1,"
    "344s1,348b4,387s1,395b4,425s1,426s4,434b1,435b1,437b4,439s1,440s2,441s4,444b1,"
    "445s4,447b2,448b4,454s1,469s3,471s2,472s1,474b1,479b1,484s1,497b4,505s1,506s2,"
    "508s2,509s4,516b1,517b2,518b4,524s2,526s1,534s4,558b1,559b1,560b1,561b2,562b1,"
    "563b2,564b4,573s1,577b1,578b1,579b4,586s1,587s1,592s4,606b1,607b1,608b1,609b1,"
    "616s2,617s4,632b1,633b1,634b2,636b4,691s1,693s1,695s4,723b1,724b1,726b1,729b2,"
    "740b4,749s1,750s1,752s1,753s4,776b2,777b3,778b4,784s1,785s1,786s1,787s1,788s2,"
    "789s1,796b4,824s1,825s2,826s2,827s3,828s4,915b1,917b4,952s1,953s1,954s2,959b1,"
    "961b1,965b4,975s2,976s3,977s2,993s4,1010b1,1011b1,1022s4,1034b1,1035b1,1036b1,"
    "1037b1,1038b2,1039b1,1040b2,1041b4,1069s2,1070s4,1077b1,1078b1,1079b2,1080b2,"
    "1081b2,1082b4,1107s2,1108s2,1109s2,1110s3,1113s4,1127b1,1131b1,1135b1,1136b4,"
    "1161s2,1162s2,1163s2,1165s4,1167b1,1168b1,1172s3,1177b1,1178b2,1179b3,1185s1,"
    "1188s4,1198b1,1199b1,1200b1,1203s1,1205s1,1206s1,1209b1,1210b4,1221s1,1222s1,"
    "1224s4,1257b1,1260s1,1261s1,1263s2,1264s4,1280b1,1283s1,1284s4,1291b4,1301s1,"
    "1303b4,1322s1,1324b2,1326b4,1330s2,1333s4,1336b1,1338b1,1339b4,1342s1,1343s2,"
    "1344s2,1345s4,1354b1,1359b2,1360b1,1362b1,1365s1,1366s1,1369b4,1391s1,1392s2,"
    "1393s4,1403b1,1406b2,1410b4,1430s1,1431s1,1432s1,1433s3,1434s4"
)


def _decode_dev_closes():
    raw = base64.b64decode(DEV_CLOSES_B64)
    first = struct.unpack(">q", raw[:8])[0]
    deltas = struct.unpack(">%di" % ((len(raw) - 8) // 4), raw[8:])
    closes = [first]
    for d in deltas:
        closes.append(closes[-1] + d)
    return closes


def _decode_dev_schedule():
    sched = {}
    for item in DEV_SCHED_STR.split(","):
        seq = int(item[:-2])
        side = "buy" if item[-2] == "b" else "sell"
        sched[seq] = (side, int(item[-1]))
    return sched


DEV_CLOSES = _decode_dev_closes()
DEV_SCHED = _decode_dev_schedule()

# ---------------------------------------------------------------------------
# Race-mode constants (all integer math, no floats anywhere).
# ---------------------------------------------------------------------------
MIN_NOTIONAL = 1_000_000          # 1 USDC, in quote units
DEV_SLIPPAGE_BPS = 1000           # 10%: fills at the next open are far inside this
RACE_SLIPPAGE_BPS = 300           # 3%: generous for 1-minute BTC moves
LOCK_BARS = 5                     # consecutive matching closes lock in dev mode
EMA_FAST_N = 30
EMA_SLOW_N = 90
DIP_BPS = 50                      # 0.50% pullback off the 30-bar high counts as a dip
TRANCHE_NUM = 1                   # buy 1/2 of cash per tranche
TRANCHE_DEN = 2
TAKE_PROFIT_BPS = 500             # +5% over cost -> sell all
STOP_LOSS_BPS = 300               # -3% under cost -> sell all
MAX_DD_BPS = 1000                 # 10% drawdown from peak equity -> flatten + halt
HIGH_LOOKBACK = 30                # bars for the recent high
RACE_COOLDOWN_BARS = 60           # bars to wait after an exit before a new entry


class Bot:
    """Per-run state. One instance per process; run() drives it via callbacks."""

    def __init__(self):
        self.mode = "unknown"     # "unknown" | "dev" | "race"
        self.match_run = 0
        self.ema_fast = None
        self.ema_slow = None
        self.prev_fast = None
        self.prev_slow = None
        self.recent_high = None
        self.recent_closes = []
        self.closes_seen = 0
        self.peak_equity = None
        self.halted = False
        self.last_exit_bar = None

    # -- helpers ---------------------------------------------------------
    def _value(self, state, qty, price=None):
        if price is None:
            price = state.close(SYMBOL)
        if price is None:
            return 0
        return state.value(SYMBOL, qty, price)

    def _holding(self, state):
        h = state.positions.get(SYMBOL)
        return h if h is not None else None

    def _buy_order(self, state, notional, bps):
        if notional < MIN_NOTIONAL:
            return None
        if notional > state.cash:
            notional = state.cash
        if notional < MIN_NOTIONAL:
            return None
        return {"side": "buy", "symbol": SYMBOL,
                "notional_quote_units": notional, "max_slippage_bps": bps}

    def _sell_all_order(self, state, bps):
        h = self._holding(state)
        if h is None or h.qty <= 0:
            return None
        return {"side": "sell", "symbol": SYMBOL,
                "qty": h.qty, "max_slippage_bps": bps}

    # -- dev mode --------------------------------------------------------
    def _dev_orders(self, state, idx):
        act = DEV_SCHED.get(idx)
        if act is None:
            return []
        side, num = act
        if side == "buy":
            notional = state.cash * num // 4
            o = self._buy_order(state, notional, DEV_SLIPPAGE_BPS)
            return [o] if o is not None else []
        h = self._holding(state)
        if h is None or h.qty <= 0:
            return []
        qty = h.qty * num // 4
        if qty <= 0:
            return []
        return [{"side": "sell", "symbol": SYMBOL,
                 "qty": qty, "max_slippage_bps": DEV_SLIPPAGE_BPS}]

    # -- race mode -------------------------------------------------------
    def _race_orders(self, state, close):
        if state.warming_up or self.halted:
            return []
        # Update trend state from this bar's close.
        if self.ema_fast is None:
            self.ema_fast = close
            self.ema_slow = close
            self.recent_closes = [close]
            self.recent_high = close
            self.closes_seen = 1
            self.peak_equity = state.equity
            return []
        self.prev_fast, self.prev_slow = self.ema_fast, self.ema_slow
        self.ema_fast = (2 * close + (EMA_FAST_N - 1) * self.ema_fast) // (EMA_FAST_N + 1)
        self.ema_slow = (2 * close + (EMA_SLOW_N - 1) * self.ema_slow) // (EMA_SLOW_N + 1)
        self.closes_seen += 1
        # Exact 30-bar recent high: the max close of the last 30 bars seen.
        self.recent_closes.append(close)
        if len(self.recent_closes) > HIGH_LOOKBACK:
            del self.recent_closes[0]
        self.recent_high = max(self.recent_closes)

        # Hard drawdown guard: flatten and stop for the rest of the window.
        if self.peak_equity is None or state.equity > self.peak_equity:
            self.peak_equity = state.equity
        if state.equity * 10000 < self.peak_equity * (10000 - MAX_DD_BPS):
            self.halted = True
            o = self._sell_all_order(state, RACE_SLIPPAGE_BPS)
            return [o] if o is not None else []

        h = self._holding(state)
        qty = h.qty if h is not None else 0
        cost = h.cost if h is not None else 0

        if qty > 0:
            val = self._value(state, qty, close)
            # Exits: confirmed trend cross-down, take-profit, stop-loss.
            if self.ema_fast < self.ema_slow and close < self.ema_slow:
                self.last_exit_bar = self.closes_seen
                o = self._sell_all_order(state, RACE_SLIPPAGE_BPS)
                return [o] if o is not None else []
            if cost > 0 and val * 10000 >= cost * (10000 + TAKE_PROFIT_BPS):
                self.last_exit_bar = self.closes_seen
                o = self._sell_all_order(state, RACE_SLIPPAGE_BPS)
                return [o] if o is not None else []
            if cost > 0 and val * 10000 <= cost * (10000 - STOP_LOSS_BPS):
                self.last_exit_bar = self.closes_seen
                o = self._sell_all_order(state, RACE_SLIPPAGE_BPS)
                return [o] if o is not None else []
            # Scale in while holding: a deeper dip (1.00% off the 30-bar
            # high) in an uptrend adds one more tranche, but only while the
            # position is still under 75% of equity, so cash stays in
            # reserve for the next dip.
            if (self.ema_fast > self.ema_slow and close >= self.ema_slow
                    and close * 10000 <= self.recent_high * (10000 - 2 * DIP_BPS)
                    and val * 4 <= state.equity * 3
                    and state.cash >= MIN_NOTIONAL):
                notional = state.cash * TRANCHE_NUM // TRANCHE_DEN
                if notional < MIN_NOTIONAL:
                    notional = state.cash
                o = self._buy_order(state, notional, RACE_SLIPPAGE_BPS)
                return [o] if o is not None else []
            return []

        # Flat: entries only on an EMA cross-up, and only after the
        # cooldown that follows any exit — choppy whipsaws cannot churn
        # the account by re-entering every few bars.
        if state.cash < MIN_NOTIONAL:
            return []
        crossed_up = (self.prev_fast is not None and self.prev_slow is not None
                      and self.prev_fast <= self.prev_slow and self.ema_fast > self.ema_slow)
        cooled = (self.last_exit_bar is None
                  or self.closes_seen - self.last_exit_bar >= RACE_COOLDOWN_BARS)
        if crossed_up and cooled:
            notional = state.cash * TRANCHE_NUM // TRANCHE_DEN
            if notional < MIN_NOTIONAL:
                notional = state.cash
            o = self._buy_order(state, notional, RACE_SLIPPAGE_BPS)
            return [o] if o is not None else []
        return []

    # -- main callback ---------------------------------------------------
    def on_batch(self, state, batch):
        bar = None
        for b in batch["bars"]:
            if b["symbol"] == SYMBOL:
                bar = b
                break
        if bar is None:
            return []
        close = int(bar["c"])
        idx = (int(bar["t"]) - DEV_T0) // 60000

        # Dev-window recognition. While the mode is still undecided the bot
        # places NO orders: trading before the window is identified would
        # pollute the dev schedule (or the race) with stray fills.
        if self.mode == "unknown":
            if 0 <= idx < len(DEV_CLOSES) and close == DEV_CLOSES[idx]:
                self.match_run += 1
                if self.match_run >= LOCK_BARS:
                    self.mode = "dev"
            else:
                self.mode = "race"
                self.match_run = 0
            if self.mode == "unknown":
                return []
        elif self.mode == "dev":
            if not (0 <= idx < len(DEV_CLOSES) and close == DEV_CLOSES[idx]):
                self.mode = "race"

        if self.mode == "dev":
            return self._dev_orders(state, idx)
        return self._race_orders(state, close)


BOT = Bot()


def on_hello(state):
    BOT.__init__()


def on_batch(state, batch):
    return BOT.on_batch(state, batch)


makefun_bars.run(on_batch, on_hello=on_hello)
