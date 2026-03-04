"""
==============================================================================
SEPTILLION  |  Multi-Symbol Live Engine  |  M1
==============================================================================
TWO-WINDOW UNIFIED ENGINE

  WINDOW A — FIX_WINDOW (4pm London WM/Reuters Fix)
    Broker hours: 15-17 (GMT+2)  |  UTC: 13-15
    Mechanism: Institutional hedging at known timestamp
    Symbols (13):
      EURJPY   WR=86.0%  E=+1.523  n=164
      EURGBP   WR=83.1%  E=+1.506  n=89
      EURCAD   WR=82.8%  E=+1.498  n=58
      AUDJPY   WR=83.7%  E=+1.465  n=300
      EURAUD   WR=82.2%  E=+1.443  n=157
      GBPJPY   WR=84.2%  E=+1.440  n=146
      USDCHF   WR=81.7%  E=+1.397  n=60
      USDJPY   WR=79.9%  E=+1.377  n=159
      EURUSD   WR=78.9%  E=+1.374  n=71
      GBPAUD   WR=78.9%  E=+1.366  n=171
      AUDUSD   WR=78.3%  E=+1.337  n=180
      GBPUSD   WR=77.2%  E=+1.314  n=57
      NZDUSD   WR=73.0%  E=+1.200  n=174
    Params: vq=q70 sq=q30 slb=5 sam=0.1 bam=0.5 vm=2.0 buf=0.2 rr=2.0
    Overfit defense: 13/13 pairs WR>=70%, avg E=+1.403

  WINDOW B — MONDAY_ASIA (Monday Asia Open)
    Broker hours: Mon 01-09 (GMT+2)  |  UTC: Sun 23 - Mon 07
    Mechanism: Weekend gap fills + first liquidity sweep of week
    Symbols (6):
      EURAUD   WR=75.6%  E=+1.210  n=291
      AUDJPY   WR=74.9%  E=+1.190  n=243
      GBPJPY   WR=74.0%  E=+1.156  n=315
      USDJPY   WR=71.8%  E=+1.144  n=248
      EURJPY   WR=71.6%  E=+1.116  n=285
      AUDUSD   WR=70.5%  E=+1.093  n=298
    Params: vq=q60 sq=q30 slb=5 sam=0.3 bam=0.5 vm=2.0 buf=0.2 rr=2.0

ARCHITECTURE:
  - One MT5 connection, one bar-close loop
  - Each (symbol, window) pair is an independent state machine
  - Broker cross-check per symbol on every bar
  - Risk: 1% per trade per (symbol, window) combination
  - All exits: SL/TP server-side + early exits via manual close

PARITY AUDIT vs BACKTEST (edge_multi_pair.py):
  ✓ Regime: rvol_30 <= rvol_qVQ AND bar_range <= spread_qSQ
  ✓ Sweep: roll_lo/hi over lookback=SLB, atr_mult=SAM, + asian range sweeps
  ✓ Displacement: body >= BAM*ATR with body_ratio >= 0.70 OR vol spike + close_pos
  ✓ db_next: signal fires if displacement on signal bar OR next bar
  ✓ SL: sweep extreme - buffer*ATR (long) or + buffer*ATR (short)
  ✓ TP: SL distance * rr_ratio
  ✓ Early exit E1: VWAP adverse cross after hold >= 3
  ✓ Early exit E2: 3 consecutive adverse candles after hold >= 3
  ✓ Session force-close at window boundary
  ✓ Max hold: 60 bars
  ✓ Cooldown: 10 bars between entries per (symbol, window)
  ✓ Max trades: 3 per session per (symbol, window)
  ✓ Asian range: broker hours 1-9 (Tokyo session)
  ✓ FIX_WINDOW asian range: previous day 1-9 (same corrected Tokyo range)
  ✓ MONDAY_ASIA asian range: previous day/week 1-9

MAGIC: 202603040  (unique — Vigintillion=202602260, Centillion=202603030)
COMMENT: "Septillion"
LOG: septillion_live.log

PORTFOLIO STATS (combined, 6.5yr backtest):
  Fix Window:   1,786 trades | avg WR=80.8% | avg E=+1.403 | ~275/yr
  Monday Asia:  1,384 trades | avg WR=73.1% | avg E=+1.135 | ~213/yr
  Combined:    ~3,170 trades | ~488/yr | ~9.4/week
==============================================================================
"""

import os, sys, io, time, logging, datetime
import numpy as np
import pandas as pd
from logging.handlers import RotatingFileHandler

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    print("ERROR: MetaTrader5 not installed.  pip install MetaTrader5")
    sys.exit(1)

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("SEPTILLION")
logger.setLevel(logging.INFO)
_fh = RotatingFileHandler("septillion_live.log", maxBytes=10_000_000, backupCount=5, encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_fh)
_sh = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace"))
_sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_sh)

# ── MT5 connection ────────────────────────────────────────────────────────────
TERMINAL_PATH = os.environ.get("MT5_TERMINAL_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe")
LOGIN         = int(os.environ.get("MT5_LOGIN", 0))
PASSWORD      = os.environ.get("MT5_PASSWORD", "")
SERVER        = os.environ.get("MT5_SERVER", "")

# ── Engine identity ───────────────────────────────────────────────────────────
MAGIC   = 202603040    # UNIQUE — Vigintillion=202602260, Centillion=202603030
COMMENT = "Septillion"

# ── Strategy constants (shared across both windows) ───────────────────────────
RISK_PER_TRADE         = 0.06  # 1% per trade per (symbol, window)
MAX_HOLD               = 60
VWAP_WINDOW            = 10
SL_MULTIPLIER          = 1.0
COOLDOWN_BARS          = 10
MAX_TRADES_PER_SESSION = 3
FETCH_BARS             = 50_000

# ── Window definitions ────────────────────────────────────────────────────────
# Each window: name, params, symbols, broker-hour window function, asian_hours
WINDOWS = {
    "FIX": {
        "description": "4pm London Fix | 15-17 broker (13-15 UTC)",
        "params": {
            "vol_threshold_q": "q70",
            "spread_q":        "q30",
            "sweep_lookback":  5,
            "sweep_atr_mult":  0.1,
            "body_atr_mult":   0.5,
            "vol_mult":        2.0,
            "buffer_atr":      0.2,
            "rr_ratio":        2.0,
        },
        "symbols": [
            "EURJPY", "EURGBP", "EURCAD", "AUDJPY", "EURAUD",
            "GBPJPY", "USDCHF", "USDJPY", "EURUSD", "GBPAUD",
            "AUDUSD", "GBPUSD", "NZDUSD",
        ],
        # broker GMT+2: 15-17 = 4pm-6pm London fix window
        "window_fn": lambda h, dow: (h >= 15) & (h < 17),
        "asian_hours": (1, 9),
    },
    "ASIA": {
        "description": "Monday Asia Open | Mon 01-09 broker (Sun 23-Mon 07 UTC)",
        "params": {
            "vol_threshold_q": "q60",
            "spread_q":        "q30",
            "sweep_lookback":  5,
            "sweep_atr_mult":  0.3,
            "body_atr_mult":   0.5,
            "vol_mult":        2.0,
            "buffer_atr":      0.2,
            "rr_ratio":        2.0,
        },
        "symbols": [
            "EURAUD", "AUDJPY", "GBPJPY", "USDJPY", "EURJPY", "AUDUSD",
        ],
        # Monday=0 in pandas dayofweek, broker hours 1-9
        "window_fn": lambda h, dow: (dow == 0) & (h >= 1) & (h < 9),
        "asian_hours": (1, 9),
    },
}

STATE_FLAT        = "FLAT"
STATE_IN_POSITION = "IN_POSITION"


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — INDICATORS
# ══════════════════════════════════════════════════════════════════════════════

def compute_atr(h, l, c, period=14):
    tr = np.maximum(h - l,
         np.maximum(np.abs(h - np.roll(c, 1)),
                    np.abs(l - np.roll(c, 1))))
    tr[0] = h[0] - l[0]
    return pd.Series(tr).rolling(period).mean().values

def rolling_realized_vol(c, window):
    lr = np.diff(np.log(np.maximum(c, 1e-9)), prepend=np.log(c[0]))
    return pd.Series(lr).rolling(window).std().values

def rolling_quantile(arr, window, q):
    return pd.Series(arr).rolling(window, min_periods=window // 2).quantile(q).values

def micro_vwap(h, l, c, v, window):
    tp      = (h + l + c) / 3.0
    cum_tpv = pd.Series(tp * v).rolling(window).sum().values
    cum_v   = pd.Series(v.astype(float)).rolling(window).sum().values
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(cum_v > 0, cum_tpv / cum_v, c)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — INDICATOR COMPUTATION
#  NOTE: compute_indicators takes the window name to build the correct in_window
#  mask. Each (symbol, window) call gets indicators with the right window flag.
# ══════════════════════════════════════════════════════════════════════════════

def compute_indicators(df, window_name):
    o = df["open"].values
    h = df["high"].values
    l = df["low"].values
    c = df["close"].values
    v = df["tick_volume"].values
    n = len(c)

    ind = {"o": o, "h": h, "l": l, "c": c, "v": v}
    ind["atr14"]   = compute_atr(h, l, c, 14)
    ind["rvol_30"] = rolling_realized_vol(c, 30)

    VOL_LB = min(28_800, n // 2)
    for q in [30, 40, 50, 60, 70]:
        ind[f"rvol_q{q}"] = rolling_quantile(ind["rvol_30"], VOL_LB, q / 100)

    bar_range = h - l
    ind["bar_range"] = bar_range
    SPR_LB = min(43_200, n // 2)
    for q in [20, 30, 40, 50]:
        ind[f"spread_q{q}"] = rolling_quantile(bar_range, SPR_LB, q / 100)

    ind["vol_mean_60"] = pd.Series(v.astype(float)).rolling(60, min_periods=10).mean().values
    ind["vwap"]        = micro_vwap(h, l, c, v, VWAP_WINDOW)

    body = np.abs(c - o); rng = h - l
    with np.errstate(divide="ignore", invalid="ignore"):
        ind["body_ratio"] = np.where(rng > 0, body / rng, 0.0)
        ind["close_pos"]  = np.where(rng > 0, (c - l) / rng, 0.5)

    dt    = pd.DatetimeIndex(df["time"])
    hours = dt.hour
    dow   = dt.dayofweek

    wdef = WINDOWS[window_name]
    ind["in_window"] = wdef["window_fn"](hours, dow)

    ind["times"]  = df["time"].values
    ind["dates"]  = np.array(dt.date)
    ind["hours"]  = np.array(hours)
    return ind


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — ASIAN RANGE TRACKER
#  Tracks broker hours 1-9 (Tokyo session, GMT+2)
#  Stores previous completed day's range and serves it during the trade window
# ══════════════════════════════════════════════════════════════════════════════

class AsianRangeTracker:
    def __init__(self):
        self.today_date = None
        self.today_hi   = -np.inf
        self.today_lo   = +np.inf
        self.prev_hi    = np.nan
        self.prev_lo    = np.nan

    def update(self, bar_time, bar_h, bar_l):
        if hasattr(bar_time, "date"):
            d    = bar_time.date()
            hour = bar_time.hour
        else:
            dt   = datetime.datetime.fromtimestamp(
                       bar_time.astype("int64") // 1_000_000_000,
                       tz=datetime.timezone.utc)
            d    = dt.date()
            hour = dt.hour

        if d != self.today_date:
            if self.today_date is not None and self.today_hi > -np.inf:
                self.prev_hi = self.today_hi
                self.prev_lo = self.today_lo
            self.today_date = d
            self.today_hi   = -np.inf
            self.today_lo   = +np.inf

        # broker hours 1-9 = Tokyo session (GMT+2 broker)
        if 1 <= hour < 9:
            self.today_hi = max(self.today_hi, bar_h)
            self.today_lo = min(self.today_lo, bar_l)

    def get(self):
        return self.prev_hi, self.prev_lo


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — SIGNAL DETECTION
#  Per-window params passed in. Logic identical to backtest.
# ══════════════════════════════════════════════════════════════════════════════

def compute_displacement_flags(ind, i, params):
    atr     = ind["atr14"][i]
    c_i     = ind["c"][i]; o_i = ind["o"][i]
    body    = abs(c_i - o_i)
    disp_Ab = (c_i > o_i) and (body >= params["body_atr_mult"] * atr) and (ind["body_ratio"][i] >= 0.70)
    disp_As = (c_i < o_i) and (body >= params["body_atr_mult"] * atr) and (ind["body_ratio"][i] >= 0.70)
    vs      = ind["v"][i] >= params["vol_mult"] * ind["vol_mean_60"][i]
    disp_Bb = vs and (ind["close_pos"][i] >= 0.80)
    disp_Bs = vs and (ind["close_pos"][i] <= 0.20)
    return (disp_Ab or disp_Bb), (disp_As or disp_Bs)


def check_entry_signal(ind, asian_hi, asian_lo, last_exit_bar, sess_count, prev_disp, params):
    i = len(ind["c"]) - 1

    if not ind["in_window"][i]:
        return None, None, None, None

    if i - last_exit_bar < COOLDOWN_BARS:
        return None, None, None, None

    if sess_count >= MAX_TRADES_PER_SESSION:
        return None, None, None, None

    vq = params["vol_threshold_q"].replace("q", "")
    sq = params["spread_q"].replace("q", "")
    if not ((ind["rvol_30"][i] <= ind[f"rvol_q{vq}"][i]) and
            (ind["bar_range"][i] <= ind[f"spread_q{sq}"][i])):
        return None, None, None, None

    N    = params["sweep_lookback"]
    mult = params["sweep_atr_mult"]
    atr  = ind["atr14"][i]
    start = max(0, i - N)

    roll_lo = ind["l"][start:i].min() if i > start else np.nan
    roll_hi = ind["h"][start:i].max() if i > start else np.nan
    h_i     = ind["h"][i]; l_i = ind["l"][i]; c_i = ind["c"][i]

    bull_gen   = (not np.isnan(roll_lo)) and (l_i < roll_lo - mult * atr) and (c_i > roll_lo)
    bear_gen   = (not np.isnan(roll_hi)) and (h_i > roll_hi + mult * atr) and (c_i < roll_hi)
    has_range  = not (np.isnan(asian_hi) or np.isnan(asian_lo))
    bull_asian = has_range and (l_i < asian_lo - mult * atr * 0.5) and (c_i > asian_lo)
    bear_asian = has_range and (h_i > asian_hi + mult * atr * 0.5) and (c_i < asian_hi)
    sweep_bull_cur = bull_gen or bull_asian
    sweep_bear_cur = bear_gen or bear_asian

    disp_bull_cur, disp_bear_cur = compute_displacement_flags(ind, i, params)

    prev_sweep_bull, prev_sweep_bear = prev_disp

    long_cond  = (sweep_bull_cur and disp_bull_cur) or (prev_sweep_bull and disp_bull_cur)
    short_cond = (sweep_bear_cur and disp_bear_cur) or (prev_sweep_bear and disp_bear_cur)

    if not (long_cond or short_cond):
        return None, None, None, (sweep_bull_cur, sweep_bear_cur)

    if long_cond and short_cond:
        return None, None, None, (sweep_bull_cur, sweep_bear_cur)

    direction = "long" if long_cond else "short"
    buf       = params["buffer_atr"]
    rr        = params["rr_ratio"]

    if direction == "long":
        sl_price = (roll_lo - buf * atr) if not np.isnan(roll_lo) else (c_i - SL_MULTIPLIER * atr)
    else:
        sl_price = (roll_hi + buf * atr) if not np.isnan(roll_hi) else (c_i + SL_MULTIPLIER * atr)

    return direction, sl_price, rr, (sweep_bull_cur, sweep_bear_cur)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 5 — EARLY EXIT
# ══════════════════════════════════════════════════════════════════════════════

def check_early_exit(ind, trade_state, sym, win):
    i   = len(ind["c"]) - 1
    dir = trade_state["direction"]

    trade_state["hold_count"] += 1
    hc = trade_state["hold_count"]

    if hc >= MAX_HOLD:
        logger.info(f"  [{sym}/{win}] EXIT: max hold ({MAX_HOLD} bars)")
        return True

    if not ind["in_window"][i]:
        logger.info(f"  [{sym}/{win}] EXIT: session boundary")
        return True

    if i >= 1 and hc >= 3:
        cur_c  = ind["c"][i];   cur_vwap  = ind["vwap"][i]
        prev_c = ind["c"][i-1]; prev_vwap = ind["vwap"][i-1]
        if dir == "long"  and (cur_c < cur_vwap)  and (prev_c >= prev_vwap):
            logger.info(f"  [{sym}/{win}] EXIT: VWAP cross (long)")
            return True
        if dir == "short" and (cur_c > cur_vwap)  and (prev_c <= prev_vwap):
            logger.info(f"  [{sym}/{win}] EXIT: VWAP cross (short)")
            return True

    adverse = (ind["c"][i] < ind["o"][i]) if dir == "long" else (ind["c"][i] > ind["o"][i])
    if adverse:
        trade_state["consec_adverse"] += 1
    else:
        trade_state["consec_adverse"] = 0

    if trade_state["consec_adverse"] >= 3 and hc >= 3:
        logger.info(f"  [{sym}/{win}] EXIT: 3 consec adverse candles")
        return True

    return False


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 6 — ORDER EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

def compute_lot_size(symbol, entry_price, sl_price, balance):
    sym_info = mt5.symbol_info(symbol)
    if sym_info is None:
        logger.error(f"symbol_info({symbol}) returned None")
        return None

    pip_value_per_lot = sym_info.trade_tick_value / sym_info.trade_tick_size
    risk_amount       = balance * RISK_PER_TRADE
    stop_dist         = abs(entry_price - sl_price)

    if stop_dist < 1e-9:
        logger.error(f"[{symbol}] Stop distance zero")
        return None

    raw = risk_amount / (stop_dist * pip_value_per_lot)
    lot = max(sym_info.volume_min,
              min(sym_info.volume_max,
                  round(raw / sym_info.volume_step) * sym_info.volume_step))
    return lot


def send_entry_order(symbol, direction, sl_price, tp_price, lot, win):
    tick  = mt5.symbol_info_tick(symbol)
    price = tick.ask if direction == "long" else tick.bid
    otype = mt5.ORDER_TYPE_BUY if direction == "long" else mt5.ORDER_TYPE_SELL

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       symbol,
        "volume":       lot,
        "type":         otype,
        "price":        price,
        "sl":           sl_price,
        "tp":           tp_price,
        "deviation":    10,
        "magic":        MAGIC,
        "comment":      f"{COMMENT}_{win}",
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(req)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        logger.error(f"[{symbol}/{win}] Entry FAILED retcode={getattr(result,'retcode',None)} {getattr(result,'comment','')}")
        return None
    logger.info(f"[{symbol}/{win}] ENTRY {direction.upper()} lot={lot} price={price:.5f} sl={sl_price:.5f} tp={tp_price:.5f} ticket={result.order}")
    return result


def send_close_order(symbol, position, win):
    otype = mt5.ORDER_TYPE_SELL if position.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
    tick  = mt5.symbol_info_tick(symbol)
    price = tick.bid if position.type == mt5.ORDER_TYPE_BUY else tick.ask

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       symbol,
        "volume":       position.volume,
        "type":         otype,
        "position":     position.ticket,
        "price":        price,
        "deviation":    10,
        "magic":        MAGIC,
        "comment":      "early_exit",
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(req)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        logger.error(f"[{symbol}/{win}] Close FAILED retcode={getattr(result,'retcode',None)}")
        return False
    logger.info(f"[{symbol}/{win}] CLOSED ticket={position.ticket} price={price:.5f}")
    return True


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 7 — STATE RECONSTRUCTION (on restart with open position)
# ══════════════════════════════════════════════════════════════════════════════

def reconstruct_state(symbol, position, df):
    entry_time = datetime.datetime.fromtimestamp(position.time, tz=datetime.timezone.utc)
    now_utc    = datetime.datetime.now(tz=datetime.timezone.utc)
    hold_count = max(0, int((now_utc - entry_time).total_seconds() / 60))
    direction  = "long" if position.type == mt5.ORDER_TYPE_BUY else "short"
    sl_price   = position.sl
    ep         = position.price_open
    sl_dist    = abs(ep - sl_price) if sl_price else 0.01

    consec = 0
    for k in range(min(3, len(df))):
        idx = -(k + 1)
        adv = (df["close"].values[idx] < df["open"].values[idx]) if direction == "long" \
              else (df["close"].values[idx] > df["open"].values[idx])
        if adv:
            consec += 1
        else:
            break

    logger.info(f"[{symbol}] RECOVERED: dir={direction} entry={ep:.5f} hold={hold_count}bars")
    return {
        "direction":      direction,
        "entry_price":    ep,
        "sl_price":       sl_price,
        "tp_price":       position.tp,
        "sl_dist":        sl_dist,
        "lot":            position.volume,
        "risk_amount":    sl_dist * position.volume * (mt5.symbol_info(symbol).trade_tick_value / mt5.symbol_info(symbol).trade_tick_size),
        "hold_count":     hold_count,
        "consec_adverse": consec,
        "ticket":         position.ticket,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 8 — PER-(SYMBOL, WINDOW) STATE
# ══════════════════════════════════════════════════════════════════════════════

def make_slot_state():
    """One state machine per (symbol, window) combination."""
    return {
        "state":         STATE_FLAT,
        "trade_state":   None,
        "last_exit_bar": -(COOLDOWN_BARS + 1),
        "sess_date":     None,
        "sess_count":    0,
        "asian":         AsianRangeTracker(),
        "prev_sweep":    (False, False),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 9 — METRICS
# ══════════════════════════════════════════════════════════════════════════════

class Metrics:
    def __init__(self):
        self.slots   = {}   # (sym, win) -> {trades, wins, total_r}
        self.peak    = None
        self.max_dd  = 0.0
        self.last_h  = None

    def ensure(self, sym, win):
        key = (sym, win)
        if key not in self.slots:
            self.slots[key] = {"trades": 0, "wins": 0, "total_r": 0.0}

    def record(self, sym, win, r, balance):
        self.ensure(sym, win)
        d = self.slots[(sym, win)]
        d["trades"] += 1
        d["wins"]   += 1 if r > 0 else 0
        d["total_r"] += r
        if self.peak is None or balance > self.peak:
            self.peak = balance
        if self.peak and self.peak > 0:
            self.max_dd = max(self.max_dd, (self.peak - balance) / self.peak)

    def hourly_report(self, balance):
        tot_t = sum(d["trades"] for d in self.slots.values())
        tot_w = sum(d["wins"]   for d in self.slots.values())
        tot_r = sum(d["total_r"] for d in self.slots.values())
        wr    = tot_w / tot_t if tot_t else 0.0
        exp   = tot_r / tot_t if tot_t else 0.0
        logger.info(
            f"\n{'='*60}\n[HOURLY REPORT — SEPTILLION]\n"
            f"  Total trades : {tot_t}\n"
            f"  Win rate     : {wr:.1%}\n"
            f"  Expectancy   : {exp:+.2f}R\n"
            f"  Total R      : {tot_r:+.1f}\n"
            f"  Max DD       : {self.max_dd:.1%}\n"
            f"  Equity       : {balance:,.2f}\n"
        )
        # Per-window summary
        for win in ["FIX", "ASIA"]:
            win_slots = {k: v for k, v in self.slots.items() if k[1] == win}
            wt = sum(d["trades"] for d in win_slots.values())
            ww = sum(d["wins"]   for d in win_slots.values())
            wr_ = sum(d["total_r"] for d in win_slots.values())
            if wt > 0:
                logger.info(f"  [{win}] n={wt}  WR={ww/wt:.1%}  totalR={wr_:+.1f}")
        # Per-slot detail
        for (s, w), d in sorted(self.slots.items()):
            if d["trades"] > 0:
                swr  = d["wins"] / d["trades"]
                sexp = d["total_r"] / d["trades"]
                logger.info(f"    {s:8s}/{w:4s}  n={d['trades']:>4}  WR={swr:.1%}  E={sexp:+.3f}R  totalR={d['total_r']:+.1f}")
        logger.info('='*60)

    def check_hourly(self, balance):
        h = datetime.datetime.now(datetime.timezone.utc).hour
        if self.last_h is None:
            self.last_h = h
        if h != self.last_h:
            self.last_h = h
            self.hourly_report(balance)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 10 — DATA FETCH
# ══════════════════════════════════════════════════════════════════════════════

def fetch_bars(symbol, n=FETCH_BARS):
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M1, 0, n + 1)
    if rates is None:
        err  = mt5.last_error()
        info = mt5.symbol_info(symbol)
        if info is None:
            info_str = "symbol_info=None (symbol unknown to terminal)"
        else:
            info_str = (
                f"visible={info.visible}  trade_mode={info.trade_mode}  "
                f"spread={info.spread}  digits={info.digits}  "
                f"path={info.path}"
            )
        logger.warning(
            f"[{symbol}] fetch returned None — "
            f"error=({err[0]}, '{err[1]}') | {info_str}"
        )
        return None
    if len(rates) < 50:
        logger.warning(f"[{symbol}] only {len(rates)} bars")
        return None
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    return df.iloc[:-1]   # drop forming bar


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 10b — BAR CLOCK
#  Uses first FIX symbol as clock reference (always in 14-pair universe)
# ══════════════════════════════════════════════════════════════════════════════

_CLOCK_SYM = WINDOWS["FIX"]["symbols"][0]   # EURJPY

def get_last_closed_bar_time():
    rates = mt5.copy_rates_from_pos(_CLOCK_SYM, mt5.TIMEFRAME_M1, 0, 2)
    if rates is not None and len(rates) >= 2:
        return pd.Timestamp(rates[1]["time"], unit="s")
    return None

def wait_for_new_bar(last_bar_time):
    while True:
        t = get_last_closed_bar_time()
        if t is not None and t > last_bar_time:
            return t
        time.sleep(5)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 11 — PROCESS ONE (SYMBOL, WINDOW) SLOT PER BAR
# ══════════════════════════════════════════════════════════════════════════════

def process_slot(sym, win, slot_st, metrics, df, balance, bar_count):
    """
    df is pre-fetched and shared across all windows for the same symbol
    within a single bar iteration to avoid double-fetching.
    """
    if df is None:
        return

    params = WINDOWS[win]["params"]
    ind    = compute_indicators(df, win)
    i      = len(ind["c"]) - 1

    slot_st["asian"].update(df["time"].iloc[-1], df["high"].iloc[-1], df["low"].iloc[-1])
    asian_hi, asian_lo = slot_st["asian"].get()

    bar_date = df["time"].iloc[-1].date()
    if bar_date != slot_st["sess_date"]:
        slot_st["sess_date"]  = bar_date
        slot_st["sess_count"] = 0

    # ── Broker state check ────────────────────────────────────────────────
    positions = mt5.positions_get(symbol=sym)
    # Filter by magic AND comment containing window tag so FIX and ASIA
    # positions on the same symbol don't interfere with each other
    win_comment = f"{COMMENT}_{win}"
    positions = [p for p in positions if p.magic == MAGIC and win_comment in (p.comment or "")] if positions else []

    if positions and slot_st["state"] == STATE_FLAT:
        logger.warning(f"[{sym}/{win}] Broker has position but state=FLAT — correcting")
        slot_st["trade_state"] = reconstruct_state(sym, positions[0], df)
        slot_st["state"]       = STATE_IN_POSITION

    elif not positions and slot_st["state"] == STATE_IN_POSITION:
        logger.info(f"[{sym}/{win}] Position closed server-side (SL/TP)")
        if slot_st["trade_state"]:
            deals = mt5.history_deals_get(position=slot_st["trade_state"].get("ticket", 0))
            if deals and len(deals) >= 2:
                ep        = slot_st["trade_state"]["entry_price"]
                sl_dist   = slot_st["trade_state"]["sl_dist"]
                direction = slot_st["trade_state"]["direction"]
                close_p   = deals[-1].price
                sign      = 1 if direction == "long" else -1
                r_mult    = sign * (close_p - ep) / sl_dist if sl_dist > 0 else 0.0
            else:
                r_mult = 0.0
            metrics.record(sym, win, r_mult, balance)
            logger.info(f"[{sym}/{win}] TRADE CLOSED (server): R={r_mult:+.3f}")
        slot_st["state"]         = STATE_FLAT
        slot_st["trade_state"]   = None
        slot_st["last_exit_bar"] = i

    # ── Entry logic ───────────────────────────────────────────────────────
    if slot_st["state"] == STATE_FLAT:
        direction, sl_price, rr, new_sweep = check_entry_signal(
            ind, asian_hi, asian_lo,
            slot_st["last_exit_bar"], slot_st["sess_count"],
            slot_st["prev_sweep"], params
        )
        if new_sweep is not None:
            slot_st["prev_sweep"] = new_sweep
        else:
            # Update sweep state even when not in window
            mult  = params["sweep_atr_mult"]
            N     = params["sweep_lookback"]
            atr   = ind["atr14"][i]
            start = max(0, i - N)
            roll_lo = ind["l"][start:i].min() if i > start else np.nan
            roll_hi = ind["h"][start:i].max() if i > start else np.nan
            h_i = ind["h"][i]; l_i = ind["l"][i]; c_i = ind["c"][i]
            has_range = not (np.isnan(asian_hi) or np.isnan(asian_lo))
            bull = ((not np.isnan(roll_lo)) and (l_i < roll_lo - mult*atr) and (c_i > roll_lo)) or \
                   (has_range and (l_i < asian_lo - mult*atr*0.5) and (c_i > asian_lo))
            bear = ((not np.isnan(roll_hi)) and (h_i > roll_hi + mult*atr) and (c_i < roll_hi)) or \
                   (has_range and (h_i > asian_hi + mult*atr*0.5) and (c_i < asian_hi))
            slot_st["prev_sweep"] = (bull, bear)

        if direction is not None:
            # Re-fetch positions to guard against race with other windows on same symbol
            positions = mt5.positions_get(symbol=sym)
            positions = [p for p in positions if p.magic == MAGIC and win_comment in (p.comment or "")] if positions else []
            if positions:
                logger.warning(f"[{sym}/{win}] Skipping entry — position already exists")
            else:
                c_last  = df["close"].iloc[-1]
                lot     = compute_lot_size(sym, c_last, sl_price, balance)
                if lot is None:
                    logger.error(f"[{sym}/{win}] Lot calc failed")
                else:
                    sl_dist  = abs(c_last - sl_price)
                    tp_price = (c_last + sl_dist * rr) if direction == "long" else (c_last - sl_dist * rr)
                    result   = send_entry_order(sym, direction, sl_price, tp_price, lot, win)
                    if result:
                        time.sleep(0.5)
                        pos_new = mt5.positions_get(symbol=sym)
                        pos_new = [p for p in pos_new if p.magic == MAGIC and win_comment in (p.comment or "")] if pos_new else []
                        if pos_new:
                            ap          = pos_new[0].price_open
                            actual_sl   = pos_new[0].sl
                            actual_tp   = pos_new[0].tp
                            actual_dist = abs(ap - actual_sl)
                        else:
                            ap = c_last; actual_sl = sl_price
                            actual_tp = tp_price; actual_dist = sl_dist

                        slot_st["trade_state"] = {
                            "direction":      direction,
                            "entry_price":    ap,
                            "sl_price":       actual_sl,
                            "tp_price":       actual_tp,
                            "sl_dist":        actual_dist,
                            "lot":            lot,
                            "hold_count":     0,
                            "consec_adverse": 0,
                            "ticket":         result.order,
                        }
                        slot_st["state"]       = STATE_IN_POSITION
                        slot_st["sess_count"] += 1

    elif slot_st["state"] == STATE_IN_POSITION:
        should_exit = check_early_exit(ind, slot_st["trade_state"], sym, win)
        if should_exit:
            positions = mt5.positions_get(symbol=sym)
            positions = [p for p in positions if p.magic == MAGIC and win_comment in (p.comment or "")] if positions else []
            if positions:
                closed = send_close_order(sym, positions[0], win)
                if closed:
                    close_p   = df["close"].iloc[-1]
                    ep        = slot_st["trade_state"]["entry_price"]
                    sl_dist   = slot_st["trade_state"]["sl_dist"]
                    sign      = 1 if slot_st["trade_state"]["direction"] == "long" else -1
                    r_mult    = sign * (close_p - ep) / sl_dist if sl_dist > 0 else 0.0
                    metrics.record(sym, win, r_mult, balance)
                    logger.info(f"[{sym}/{win}] TRADE CLOSED (early): R={r_mult:+.3f}")
                    slot_st["state"]         = STATE_FLAT
                    slot_st["trade_state"]   = None
                    slot_st["last_exit_bar"] = i
            else:
                slot_st["state"]       = STATE_FLAT
                slot_st["trade_state"] = None


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 12 — MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════

def run_live():
    if not mt5.initialize(path=TERMINAL_PATH, login=LOGIN, password=PASSWORD, server=SERVER):
        raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")

    acct = mt5.account_info()
    logger.info(f"MT5 connected | account={acct.login} | balance={acct.balance:.2f}")
    logger.info(f"Engine: SEPTILLION | Magic: {MAGIC}")
    logger.info(f"FIX  symbols ({len(WINDOWS['FIX']['symbols'])}): {WINDOWS['FIX']['symbols']}")
    logger.info(f"ASIA symbols ({len(WINDOWS['ASIA']['symbols'])}): {WINDOWS['ASIA']['symbols']}")
    logger.info(f"FIX  params: {WINDOWS['FIX']['params']}")
    logger.info(f"ASIA params: {WINDOWS['ASIA']['params']}")
    logger.info(f"RISK_PER_TRADE: {RISK_PER_TRADE:.6%} per (symbol, window)")
    logger.info(f"Trade windows:  FIX 15-17 broker | ASIA Mon 01-09 broker")
    logger.info(f"Asian range:    01-09 broker (Tokyo)")
    logger.info("=" * 60)

    # ── Symbol diagnostic ──────────────────────────────────────────────────
    all_syms_needed = set(WINDOWS["FIX"]["symbols"]) | set(WINDOWS["ASIA"]["symbols"])
    logger.info("=== SYMBOL DIAGNOSTIC ===")
    for sym in sorted(all_syms_needed):
        info = mt5.symbol_info(sym)
        if info is None:
            avail = mt5.symbols_get()
            cands = [s.name for s in avail if sym[:3] in s.name or sym[3:] in s.name] if avail else []
            logger.warning(f"  {sym}: NOT FOUND. Possible: {cands[:10]}")
        else:
            tick       = mt5.symbol_info_tick(sym)
            rates_test = mt5.copy_rates_from_pos(sym, mt5.TIMEFRAME_M1, 0, 5)
            logger.info(
                f"  {sym}: visible={info.visible}  spread={info.spread}  "
                f"digits={info.digits}  "
                f"tick={'OK' if tick else 'None'}  "
                f"bars_test={'OK len='+str(len(rates_test)) if rates_test is not None else 'None — '+str(mt5.last_error())}"
            )
    logger.info("=== END DIAGNOSTIC ===")

    # ── Build slot state dict: {(sym, win): state} ────────────────────────
    # Key insight: FIX and ASIA share symbols but are INDEPENDENT state machines.
    # A position in EURJPY/FIX has no interaction with EURJPY/ASIA.
    slot_states = {}
    for win, wdef in WINDOWS.items():
        for sym in wdef["symbols"]:
            slot_states[(sym, win)] = make_slot_state()

    metrics   = Metrics()
    bar_count = 0

    # ── Startup: recover any open positions ───────────────────────────────
    for (sym, win), slot_st in slot_states.items():
        win_comment = f"{COMMENT}_{win}"
        positions = mt5.positions_get(symbol=sym)
        if positions:
            for pos in positions:
                if pos.magic == MAGIC and win_comment in (pos.comment or ""):
                    df_init = fetch_bars(sym, 200)
                    if df_init is not None:
                        slot_st["trade_state"] = reconstruct_state(sym, pos, df_init)
                        slot_st["state"]       = STATE_IN_POSITION
                        logger.info(f"[{sym}/{win}] STARTUP: recovered open position ticket={pos.ticket}")
                    break

    # ── Seed bar clock ────────────────────────────────────────────────────
    last_bar_time = get_last_closed_bar_time()
    if last_bar_time is None:
        last_bar_time = pd.Timestamp.utcnow()
    logger.info(f"Seeded bar time: {last_bar_time} — waiting for next bar close...")

    while True:
        try:
            new_bar_time  = wait_for_new_bar(last_bar_time)
            last_bar_time = new_bar_time
            bar_count    += 1

            logger.info(f"── BAR {bar_count} | {new_bar_time} ──────────────────────────────────")

            balance = mt5.account_info().balance

            # ── Fetch each unique symbol once, reuse for both windows ─────
            # This avoids double-fetching EURJPY 50k bars twice per bar
            unique_syms = list(all_syms_needed)
            bar_data    = {}
            for sym in unique_syms:
                bar_data[sym] = fetch_bars(sym)

            # ── Process all (symbol, window) slots ─────────────────────────
            for (sym, win), slot_st in slot_states.items():
                process_slot(sym, win, slot_st, metrics, bar_data[sym], balance, bar_count)

            metrics.check_hourly(balance)

        except KeyboardInterrupt:
            logger.info("Shutdown — exiting")
            break
        except Exception as e:
            logger.exception(f"Main loop error: {e}")
            time.sleep(30)

    mt5.shutdown()
    logger.info("Disconnected. Septillion engine stopped.")
    if mt5.account_info():
        metrics.hourly_report(mt5.account_info().balance)
    else:
        metrics.hourly_report(0)


if __name__ == "__main__":
    run_live()
