# ============================================
# scanner_core.py  —  Daily options scanner with rate-limit protection & caching
# ============================================

import os, math, json, time, random, pickle, warnings
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore", category=FutureWarning)

# ---------- Optional: requests_cache ----------
REQUESTS_CACHE_ENABLED = False
try:
    import requests_cache
    # 15 分鐘 HTTP 層級快取（若已安裝）
    requests_cache.install_cache(
        cache_name="yf_http_cache",
        backend="sqlite",
        expire_after=15 * 60
    )
    REQUESTS_CACHE_ENABLED = True
except Exception:
    REQUESTS_CACHE_ENABLED = False

# ============================================
# Global Config
# ============================================

NAV = 100_000
MAX_LOSS_PER_TRADE = 0.005 * NAV      # <= $500
MIN_POP = 0.70                        # OP style: > 70%
MIN_CR_ML = 0.33                      # credit / max_loss
MIN_CREDIT_DOLLARS = 60               # 最低收權利金
RISK_FREE = 0.045                     # annualized
MIN_DTE, MAX_DTE = 25, 45             # 目標到期區間
TOP_N = 15                            # 掃描前 15 名
EXCEL_PATH = "sim_trades.xlsx"

# Scanner thresholds
UNIVERSE = "sp500"                    # 或指定 list
MIN_PRICE = 10                        # 避免太低價股
MIN_DOLLAR_VOL = 5e7                  # 20D 平均 $ 成交額 >= $50m
MIN_CHAIN_VOL = 2_000                 # 到期合計成交量（call+put）>= 2000
MIN_CHAIN_OI  = 5_000                 # 到期合計 OI（call+put）>= 5000

# Spread knobs
PUT_DELTA_TARGETS  = [0.20, 0.25, 0.30, 0.35]
CALL_DELTA_TARGETS = [0.30, 0.35, 0.40, 0.45]

# Rate-limit & cache
CACHE_DIR = Path(".yf_cache"); CACHE_DIR.mkdir(exist_ok=True)
CHAIN_TTL_SEC   = 15 * 60
OPTLIST_TTL_SEC = 6 * 60 * 60
HIST_TTL_SEC    = 30 * 60

RATE_DELAY_BASE = 0.7   # 每次抓期權鏈後 sleep 秒數（再加抖動）
MAX_RETRIES     = 4     # 429 退避最大重試
BACKOFF_BASE    = 1.5   # 退避倍率

LOG_DIR = Path("run_logs"); LOG_DIR.mkdir(exist_ok=True)

# HTTP metrics（估算層級；yfinance 內部仍可能有多個請求）
HTTP_CALLS = 0
HTTP_429S  = 0
SLEEP_SEC_ACCUM = 0.0

def _now_utc():
    return datetime.now(timezone.utc)

def _sleep(sec: float):
    global SLEEP_SEC_ACCUM
    time.sleep(sec)
    SLEEP_SEC_ACCUM += sec

def _safe_key(s: str) -> str:
    return s.replace("/", "_").replace(":", "_").replace(" ", "_")

def _cache_get(key: str, ttl_seconds: int):
    """取 15 分鐘檔案快取；拿不到或逾時則回 None。"""
    p = CACHE_DIR / f"{_safe_key(key)}.pkl"
    if not p.exists(): return None
    age = _now_utc() - datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    if age.total_seconds() > ttl_seconds: return None
    try:
        with open(p, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None

def _cache_set(key: str, obj):
    p = CACHE_DIR / f"{_safe_key(key)}.pkl"
    try:
        with open(p, "wb") as f:
            pickle.dump(obj, f)
    except Exception:
        pass

def _count_http():
    """粗略計數一次「外部取數呼叫」"""
    global HTTP_CALLS
    HTTP_CALLS += 1

def _with_retry(callable_fn, *args, **kwargs):
    """429/限流的退避重試封裝；其他錯誤也會嘗試幾次。"""
    global HTTP_429S
    for attempt in range(MAX_RETRIES):
        try:
            return callable_fn(*args, **kwargs)
        except Exception as e:
            msg = str(e).lower()
            # 判斷是否可能是 rate-limit 類
            is429 = ("429" in msg) or ("too many requests" in msg) or ("rate" in msg and "limit" in msg)
            if is429:
                HTTP_429S += 1
            # 指數退避 + 隨機抖動
            delay = (BACKOFF_BASE ** attempt) + random.random() * 0.6
            _sleep(delay)
            if attempt == MAX_RETRIES - 1:
                return None
    return None

# ============================================
# Math / BS + Greeks + POP
# ============================================

def norm_cdf(x): return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))
def norm_pdf(x): return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

def d1(S, K, r, sigma, T):
    if min(S, K, sigma, T) <= 0: return None
    return (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))

def d2(S, K, r, sigma, T):
    a = d1(S, K, r, sigma, T)
    return a - sigma * math.sqrt(T) if a is not None else None

def greeks_call(S, K, r, sigma, T):
    _d1 = d1(S, K, r, sigma, T); _d2 = d2(S, K, r, sigma, T)
    if _d1 is None or _d2 is None: return None
    delta = norm_cdf(_d1)
    vega  = S * norm_pdf(_d1) * math.sqrt(T)      # per 1.00 vol, per share
    return delta, vega

def greeks_put(S, K, r, sigma, T):
    _d1 = d1(S, K, r, sigma, T); _d2 = d2(S, K, r, sigma, T)
    if _d1 is None or _d2 is None: return None
    delta = norm_cdf(_d1) - 1.0
    vega  = S * norm_pdf(_d1) * math.sqrt(T)
    return delta, vega

def pop_short_put(S, K, iv, T):
    _d2 = d2(S, K, RISK_FREE, iv, T)
    return norm_cdf(_d2) if _d2 is not None else None  # P[S_T > K]

def pop_short_call(S, K, iv, T):
    _d2 = d2(S, K, RISK_FREE, iv, T)
    return norm_cdf(-_d2) if _d2 is not None else None # P[S_T < K]

# ============================================
# Yahoo helpers with cache + rate-limit
# ============================================

def yf_history(ticker, period="3mo", interval="1d") -> pd.DataFrame:
    key = f"hist:{ticker}:{period}:{interval}"
    obj = _cache_get(key, HIST_TTL_SEC)
    if obj is not None:
        return obj
    _count_http()
    def _do():
        return yf.Ticker(ticker).history(period=period, interval=interval)
    df = _with_retry(_do)
    if isinstance(df, pd.DataFrame) and not df.empty:
        _cache_set(key, df)
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

def yf_options_list(ticker) -> List[str]:
    key = f"opts:{ticker}"
    obj = _cache_get(key, OPTLIST_TTL_SEC)
    if obj is not None:
        return obj
    _count_http()
    def _do():
        return yf.Ticker(ticker).options
    exps = _with_retry(_do)
    if isinstance(exps, list) and exps:
        _cache_set(key, exps)
        return exps
    return []

def yf_option_chain(ticker, exp) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """回傳 (calls, puts)，15 分鐘快取 + 每次抓完 sleep 0.5~1.0 秒。"""
    key = f"chain:{ticker}:{exp}"
    obj = _cache_get(key, CHAIN_TTL_SEC)
    if obj is not None:
        return obj.get("calls", pd.DataFrame()), obj.get("puts", pd.DataFrame())
    _count_http()
    def _do():
        return yf.Ticker(ticker).option_chain(exp)
    ch = _with_retry(_do)
    # 固定節流（再加隨機抖動）
    _sleep(RATE_DELAY_BASE + random.random() * 0.5)
    if ch is None:
        return pd.DataFrame(), pd.DataFrame()
    calls, puts = ch.calls.copy(), ch.puts.copy()
    _cache_set(key, {"calls": calls, "puts": puts})
    return calls, puts

# ============================================
# Universe & scanner
# ============================================

def get_universe():
    if isinstance(UNIVERSE, list):
        return UNIVERSE
    # 嘗試 Wikipedia S&P 500；失敗就 fallback
    try:
        _count_http()
        tbls = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        df = tbls[0]
        syms = sorted(df["Symbol"].unique().tolist())
        syms = [s.replace(".", "-") for s in syms]
        return syms
    except Exception:
        return ["AAPL","MSFT","NVDA","AMD","TSLA","META","GOOGL","AMZN","NFLX","XOM","JPM","BAC","SPY","QQQ","TQQQ","AVGO","COST","WMT","PEP","UNH"]

def near_dte_exp(ticker, min_dte=21, max_dte=60) -> Optional[pd.Timestamp]:
    exps = yf_options_list(ticker)
    if not exps: return None
    today = _now_utc().date()
    best, gap = None, 10**9
    for e in exps:
        try: d = datetime.strptime(e, "%Y-%m-%d").date()
        except: continue
        dte = (d - today).days
        if min_dte <= dte <= max_dte:
            return pd.Timestamp(d)
        if dte >= min_dte and (dte - max_dte) < gap:
            best, gap = pd.Timestamp(d), dte - max_dte
    return best

def option_liq_iv_snapshot(t) -> Optional[dict]:
    h = yf_history(t, period="3mo", interval="1d")
    if h.empty: return None
    px = float(h["Close"].iloc[-1])
    if not np.isfinite(px) or px < MIN_PRICE: return None

    # 20D $ volume
    dollar_vol = float((h["Close"] * h["Volume"]).rolling(20).mean().iloc[-1])
    if not np.isfinite(dollar_vol) or dollar_vol < MIN_DOLLAR_VOL:
        return None

    exp = near_dte_exp(t, 21, 60)
    if exp is None: return None

    calls, puts = yf_option_chain(t, exp.strftime("%Y-%m-%d"))
    if calls.empty or puts.empty: return None

    # ATM IV 中位數
    strikes = puts["strike"]
    k_idx = (strikes - px).abs().argsort()[:3]
    atm_strikes = set(strikes.iloc[k_idx].tolist())
    ivs = pd.concat([
        calls[calls["strike"].isin(atm_strikes)]["impliedVolatility"],
        puts[puts["strike"].isin(atm_strikes)]["impliedVolatility"]
    ]).replace(0, np.nan).dropna()
    iv = float(np.nanmedian(ivs)) if len(ivs) else np.nan

    chain_vol = float(calls["volume"].fillna(0).sum() + puts["volume"].fillna(0).sum())
    chain_oi  = float(calls["openInterest"].fillna(0).sum() + puts["openInterest"].fillna(0).sum())
    if chain_vol < MIN_CHAIN_VOL or chain_oi < MIN_CHAIN_OI:
        return None

    # 14D ATR%
    atr_pct = float((h["High"] - h["Low"]).rolling(14).mean().iloc[-1] / px)

    return dict(
        ticker=t, price=px, exp=str(exp.date()),
        iv=iv, chain_vol=chain_vol, chain_oi=chain_oi,
        dollar_vol=dollar_vol, atr_pct=atr_pct
    )

def scan_liquid_high_iv(top_n=TOP_N):
    rows=[]
    for t in get_universe():
        try:
            snap = option_liq_iv_snapshot(t)
            if snap: rows.append(snap)
        except Exception:
            continue
    if not rows:
        return [], pd.DataFrame()
    df = pd.DataFrame(rows).dropna(subset=["iv"])
    for col in ["iv","atr_pct","dollar_vol","chain_vol","chain_oi"]:
        m, s = df[col].mean(), df[col].std() + 1e-9
        df[col+"_z"] = (df[col]-m)/s
    df["score"] = (
        0.45*df["iv_z"] +
        0.25*df["atr_pct_z"] +
        0.30*((df["dollar_vol_z"]+df["chain_vol_z"]+df["chain_oi_z"])/3)
    )
    picks = df.sort_values("score", ascending=False).head(top_n)
    return picks["ticker"].tolist(), picks

# ============================================
# Helpers: sector, momentum/flow
# ============================================

def get_sector(ticker) -> str:
    try:
        # get_info 也會打外部；這裡直接用快取過的歷史 & fallback
        info = yf.Ticker(ticker).get_info()
        sec = info.get("sector") or "Unknown"
        return sec
    except Exception:
        return "Unknown"

def momentum_volume_features(ticker) -> Tuple[float, float]:
    try:
        h = yf_history(ticker, period="6mo", interval="1d")
        if len(h) < 40: return 0.0, 0.0
        ret20 = h["Close"].pct_change(20).iloc[-1]
        mom_series = h["Close"].pct_change(20).dropna()
        momentum_z = (ret20 - mom_series.mean()) / (mom_series.std() + 1e-9)
        vol = h["Volume"].astype(float)
        volume_z = (vol.iloc[-1] - vol.rolling(20).mean().iloc[-1]) / (vol.rolling(20).std().iloc[-1] + 1e-9)
        return float(momentum_z), float(volume_z)
    except Exception:
        return 0.0, 0.0

# ============================================
# Candidate model
# ============================================

@dataclass
class Candidate:
    ticker: str
    sector: str
    strategy: str
    legs: str
    exp: str
    dte: int
    pop: float
    credit: float     # $ per spread
    max_loss: float   # $ per spread
    net_delta_shares: float
    net_vega_dollars: float   # $ per 1.00 vol change
    momentum_z: float
    flow_z: float
    S: float

# ============================================
# Build spreads per ticker
# ============================================

def _theo_price(opt_type, S, K, iv, T, r=RISK_FREE):
    _d1 = d1(S, K, r, iv, T); _d2 = d2(S, K, r, iv, T)
    if _d1 is None or _d2 is None: return np.nan
    if opt_type == "call":
        return S*norm_cdf(_d1) - K*math.exp(-r*T)*norm_cdf(_d2)
    else:
        return K*math.exp(-r*T)*norm_cdf(-_d2) - S*norm_cdf(-_d1)

def _prepare_chain(df: pd.DataFrame, opt_type: str, S: float, T: float) -> pd.DataFrame:
    df = df.copy()
    # IV 補值：0 -> NaN -> 中位數 -> fallback 0.5
    if "impliedVolatility" not in df.columns or df["impliedVolatility"].isna().all() or (df["impliedVolatility"]==0).all():
        df["impliedVolatility"] = 0.5
    else:
        med = df["impliedVolatility"].replace(0, np.nan).median()
        if np.isnan(med): med = 0.5
        df["impliedVolatility"] = df["impliedVolatility"].replace(0, np.nan).fillna(med)

    def _px(row):
        b = float(row.get("bid", np.nan)); a = float(row.get("ask", np.nan))
        if np.isfinite(b) and np.isfinite(a) and b>0 and a>0: return (a+b)/2.0
        last = float(row.get("lastPrice", np.nan))
        if np.isfinite(last) and last>0: return last
        iv = float(row.get("impliedVolatility", np.nan)); K = float(row["strike"])
        if not np.isfinite(iv) or iv<=0: return np.nan
        return _theo_price(opt_type, S, K, iv, T)

    df["_px"] = df.apply(_px, axis=1)

    # Delta
    if opt_type == "put":
        df["_delta"] = df.apply(lambda r: greeks_put(S, float(r["strike"]), RISK_FREE, float(r["impliedVolatility"]), T)[0], axis=1)
    else:
        df["_delta"] = df.apply(lambda r: greeks_call(S, float(r["strike"]), RISK_FREE, float(r["impliedVolatility"]), T)[0], axis=1)
    return df

def _detect_step(strikes: pd.Series) -> float:
    s = np.sort(strikes.unique())
    diffs = np.diff(s)
    diffs = diffs[diffs>0]
    return float(np.min(diffs)) if len(diffs) else 1.0

def build_spreads_for_ticker(ticker) -> List[Candidate]:
    out=[]
    exp = near_dte_exp(ticker, MIN_DTE, MAX_DTE)
    if exp is None: return out
    dte = (exp.date() - _now_utc().date()).days
    T   = dte / 365.0

    h = yf_history(ticker, period="6mo", interval="1d")
    if h.empty: return out
    S = float(h["Close"].iloc[-1])
    momentum_z, flow_z = momentum_volume_features(ticker)

    calls, puts = yf_option_chain(ticker, exp.strftime("%Y-%m-%d"))
    if calls.empty and puts.empty: return out

    sector = get_sector(ticker)

    def find_put_spread():
        df = _prepare_chain(puts, "put", S, T)
        if df.empty: return None
        step = _detect_step(df["strike"])
        min_w, max_w = step, min(5.0, step * 2)
        best = None
        for tgt in PUT_DELTA_TARGETS:
            band = 0.06
            cand = df[df["_delta"].abs().between(tgt-band, tgt+band)]
            if cand.empty:
                cand = df.loc[(S - df["strike"]).abs().sort_values().index[:3]]
            for _, short in cand.iterrows():
                short_mid = float(short["_px"])
                if not (short_mid > 0): continue
                for k in df["strike"].sort_values(ascending=False):
                    if k >= short["strike"]: continue
                    width = float(short["strike"] - k)
                    if width < min_w-1e-9 or width > max_w+1e-9: continue
                    lr = df[df["strike"]==k]
                    if lr.empty: continue
                    long_mid = float(lr["_px"].iloc[0])
                    if not (long_mid > 0): continue
                    credit   = (short_mid - long_mid) * 100.0
                    max_loss = width * 100.0 - credit
                    if max_loss <= 0 or max_loss > MAX_LOSS_PER_TRADE: continue
                    ratio = credit / max_loss
                    if ratio < MIN_CR_ML or credit < MIN_CREDIT_DOLLARS:
                        continue
                    iv = float(short["impliedVolatility"])
                    pop = pop_short_put(S, float(short["strike"]), iv, T)
                    gS  = greeks_put(S, float(short["strike"]), RISK_FREE, iv, T)
                    gL  = greeks_put(S, float(k),              RISK_FREE, iv, T)
                    if pop is None or gS is None or gL is None: continue
                    if pop < MIN_POP: continue
                    net_delta_sh     = -gS[0]*100 + gL[0]*100
                    net_vega_dollars = (-gS[1]*100 + gL[1]*100)
                    score = pop * credit
                    legs = f"Short {ticker} {exp.date()} {short['strike']}P / Long {k}P"
                    tup = ("Short Put Credit Spread", legs, credit, max_loss, pop, net_delta_sh, net_vega_dollars, score)
                    if (best is None) or (score > best[-1]): best = tup
        if best is None: return None
        return best[:-1]

    def find_call_spread():
        df = _prepare_chain(calls, "call", S, T)
        if df.empty: return None
        step = _detect_step(df["strike"])
        min_w, max_w = step, min(5.0, step * 2)
        best = None
        for tgt in CALL_DELTA_TARGETS:
            band = 0.06
            cand = df[df["_delta"].between(tgt-band, tgt+band)]
            if cand.empty:
                cand = df.loc[(df["strike"] - S).abs().sort_values().index[:3]]
            for _, short in cand.iterrows():
                short_mid = float(short["_px"])
                if not (short_mid > 0): continue
                for k in df["strike"].sort_values(ascending=True):
                    if k <= short["strike"]: continue
                    width = float(k - short["strike"])
                    if width < min_w-1e-9 or width > max_w+1e-9: continue
                    lr = df[df["strike"]==k]
                    if lr.empty: continue
                    long_mid = float(lr["_px"].iloc[0])
                    if not (long_mid > 0): continue
                    credit   = (short_mid - long_mid) * 100.0
                    max_loss = width * 100.0 - credit
                    if max_loss <= 0 or max_loss > MAX_LOSS_PER_TRADE: continue
                    ratio = credit / max_loss
                    if ratio < MIN_CR_ML or credit < MIN_CREDIT_DOLLARS:
                        continue
                    iv = float(short["impliedVolatility"])
                    pop = pop_short_call(S, float(short["strike"]), iv, T)
                    gS  = greeks_call(S, float(short["strike"]), RISK_FREE, iv, T)
                    gL  = greeks_call(S, float(k),              RISK_FREE, iv, T)
                    if pop is None or gS is None or gL is None: continue
                    if pop < MIN_POP: continue
                    net_delta_sh     = -gS[0]*100 + gL[0]*100
                    net_vega_dollars = (-gS[1]*100 + gL[1]*100)
                    score = pop * credit
                    legs = f"Short {ticker} {exp.date()} {short['strike']}C / Long {k}C"
                    tup = ("Short Call Credit Spread", legs, credit, max_loss, pop, net_delta_sh, net_vega_dollars, score)
                    if (best is None) or (score > best[-1]): best = tup
        if best is None: return None
        return best[:-1]

    m = (h["Close"].pct_change(20).iloc[-1] if len(h)>21 else 0.0)
    if m >= 0:
        p = find_put_spread();  c = find_call_spread()
    else:
        c = find_call_spread(); p = find_put_spread()

    if p:
        s, legs, credit, max_loss, pop, d_sh, v_d = p
        out.append(Candidate(ticker, sector, s, legs, exp.strftime("%Y-%m-%d"), dte, float(pop), float(credit), float(max_loss),
                             float(d_sh), float(v_d), momentum_z, flow_z, S))
    if c:
        s, legs, credit, max_loss, pop, d_sh, v_d = c
        out.append(Candidate(ticker, sector, s, legs, exp.strftime("%Y-%m-%d"), dte, float(pop), float(credit), float(max_loss),
                             float(d_sh), float(v_d), momentum_z, flow_z, S))
    return out

# ============================================
# Filters, portfolio constraints, ranking
# ============================================

def passes_hard_filters(c: Candidate) -> bool:
    if c.pop < MIN_POP: return False
    if c.max_loss <= 0: return False
    if (c.credit / c.max_loss) < MIN_CR_ML: return False
    if c.max_loss > MAX_LOSS_PER_TRADE: return False
    if c.credit < MIN_CREDIT_DOLLARS: return False
    return True

def exposure_ok(selected: List[Candidate], new: Candidate, nav=NAV, base_delta: float = 0.0, base_vega: float = 0.0) -> bool:
    tot_d = base_delta
    tot_v = base_vega
    for x in selected:
        tot_d += x.net_delta_shares * x.S
        tot_v += x.net_vega_dollars
    tot_d += new.net_delta_shares * new.S
    tot_v += new.net_vega_dollars
    delta_ratio = tot_d / nav
    vega_ratio  = tot_v / nav
    if not (-0.30 <= delta_ratio <= 0.30): return False
    if vega_ratio < -0.05: return False
    return True

def rank_candidates(candidates: List[Candidate]) -> List[Tuple[Candidate, float, str]]:
    out=[]
    for c in candidates:
        score = float(c.pop * c.credit) * (1.0 + 0.03*max(0.0, c.momentum_z) + 0.02*max(0.0, c.flow_z))
        thesis = ("IV偏高、動能穩健，賣價外賣權價差收權利金。"
                  if "Put" in c.strategy else
                  "IV偏高、動能轉弱，賣價外買權價差賺取時間價值。")
        out.append((c, score, thesis[:30]))
    return sorted(out, key=lambda x: x[1], reverse=True)

# ============================================
# Book & MTM
# ============================================

def load_positions() -> pd.DataFrame:
    if os.path.exists(EXCEL_PATH):
        try:
            return pd.read_excel(EXCEL_PATH, sheet_name="Positions")
        except Exception:
            pass
    cols=["TradeID","Date","Ticker","Strategy","Legs","Exp","DTE_Orig",
          "EntryCredit","MaxLoss","Qty","Status","Thesis",
          "Spot_Entry","Delta_Sh","Vega_$","Sector","SpreadNow","PnL$"]
    return pd.DataFrame(columns=cols)

def mark_to_market(row: pd.Series) -> Tuple[float,float]:
    ticker=row["Ticker"]; exp=row["Exp"]; legs=row["Legs"]; qty=int(row["Qty"])
    try:
        k_short = float(legs.split()[3].rstrip("PC"))
        is_put  = legs.split()[3].endswith("P")
        k_long  = float(legs.split()[-1].rstrip("PC"))
    except Exception:
        return (np.nan, np.nan)
    calls, puts = yf_option_chain(ticker, exp)
    df = puts if is_put else calls
    def mid(rr):
        b=float(rr.get("bid",np.nan)); a=float(rr.get("ask",np.nan))
        if np.isnan(b) or np.isnan(a) or b<=0 or a<=0: return np.nan
        return (a+b)/2.0
    row_s = df[df["strike"]==k_short]
    row_l = df[df["strike"]==k_long]
    if row_s.empty or row_l.empty: return (np.nan, np.nan)
    short_mid = mid(row_s.iloc[0]); long_mid = mid(row_l.iloc[0])
    if np.isnan(short_mid) or np.isnan(long_mid): return (np.nan, np.nan)
    spread_now = (short_mid - long_mid) * 100.0
    entry = float(row["EntryCredit"])
    pnl = (entry - spread_now) * qty
    return spread_now, pnl

def save_book(positions: pd.DataFrame, todays: pd.DataFrame, scan_table: Optional[pd.DataFrame]=None):
    with pd.ExcelWriter(EXCEL_PATH, engine="xlsxwriter") as w:
        positions.to_excel(w, index=False, sheet_name="Positions")
        todays.to_excel(w, index=False, sheet_name="TodaysPicks")
        if scan_table is not None and not scan_table.empty:
            scan_table.to_excel(w, index=False, sheet_name="ScanSnapshot")



# ============================================
# Run-once main
# ============================================


def empty_positions_df():
    cols=["TradeID","Date","Ticker","Strategy","Legs","Exp","DTE_Orig",
          "EntryCredit","MaxLoss","Qty","Status","Thesis",
          "Spot_Entry","Delta_Sh","Vega_$","Sector","SpreadNow","PnL$"]
    return pd.DataFrame(columns=cols)

def run_once(base_positions: Optional[pd.DataFrame]=None):
    global HTTP_CALLS, HTTP_429S, SLEEP_SEC_ACCUM
    HTTP_CALLS = 0; HTTP_429S = 0; SLEEP_SEC_ACCUM = 0.0
    t0 = time.time()

    # A) Scan universe
    tickers, scan_snapshot = scan_liquid_high_iv(TOP_N)
    print(f"Universe picks (top {TOP_N}):", tickers)

    # B) Load existing + MTM（優先用外部傳入的 base_positions）
    if base_positions is not None and isinstance(base_positions, pd.DataFrame):
        book = base_positions.copy()
    else:
        book = load_positions()

    if not book.empty:
        mtm_price, mtm_pnl = [], []
        for _, r in book.iterrows():
            if str(r.get("Status","")) == "OPEN":
                p_now, pnl = mark_to_market(r)
                mtm_price.append(p_now); mtm_pnl.append(pnl)
            else:
                mtm_price.append(np.nan); mtm_pnl.append(np.nan)
        book["SpreadNow"] = mtm_price
        book["PnL$"] = mtm_pnl

    open_legs = set(book.loc[book["Status"]=="OPEN", "Legs"]) if not book.empty else set()

    # 既有 OPEN 的籃子曝險
    def existing_open_exposure(book_df: pd.DataFrame) -> Tuple[float, float]:
        if book_df.empty: return 0.0, 0.0
        opens = book_df[book_df["Status"]=="OPEN"].copy()
        if opens.empty: return 0.0, 0.0
        tickers = opens["Ticker"].unique().tolist()
        S_map: Dict[str, float] = {}
        for t in tickers:
            try:
                S_map[t] = float(yf_history(t, period="1d", interval="1d")["Close"].iloc[-1])
            except Exception:
                last = opens.loc[opens["Ticker"]==t, "Spot_Entry"].iloc[-1]
                S_map[t] = float(last) if pd.notna(last) else 0.0
        total_delta_dollars = 0.0
        total_vega_dollars  = 0.0
        for _, r in opens.iterrows():
            S_now = S_map.get(r["Ticker"], float(r.get("Spot_Entry", 0.0)) or 0.0)
            total_delta_dollars += float(r.get("Delta_Sh", 0.0)) * S_now
            total_vega_dollars  += float(r.get("Vega_$", 0.0))
        return total_delta_dollars, total_vega_dollars

    base_delta_dollars, base_vega_dollars = existing_open_exposure(book)

    # C) Build candidates only for scanned tickers
    candidates: List[Candidate] = []
    for t in tickers:
        try:
            candidates += build_spreads_for_ticker(t)
        except Exception:
            continue

    # D) Hard filters
    candidates = [c for c in candidates if passes_hard_filters(c)]

    # E) Ranking + sector cap + basket exposure + avoid duplicate legs
    todays = []
    sector_counts: Dict[str,int] = {}
    selected: List[Candidate] = []
    ranked = rank_candidates(candidates)

    for c, score, thesis in ranked:
        if len(selected) >= 5: break
        if c.legs in open_legs: 
            continue
        if not exposure_ok(selected, c, NAV, base_delta=base_delta_dollars, base_vega=base_vega_dollars):
            continue
        if sector_counts.get(c.sector, 0) >= 2:
            continue
        selected.append(c)
        sector_counts[c.sector] = sector_counts.get(c.sector, 0) + 1
        todays.append({
            "Ticker": c.ticker, "Strategy": c.strategy, "Legs": c.legs,
            "POP": round(c.pop,3), "Credit($)": round(c.credit,2),
            "MaxLoss($)": round(c.max_loss,2), "DTE": c.dte,
            "Delta_Sh": round(c.net_delta_shares,2), "Vega_$": round(c.net_vega_dollars,2),
            "Thesis": thesis[:30], "Sector": c.sector
        })

    todays_df = pd.DataFrame(todays)

    # F) Paper BUY append
    if not todays_df.empty:
        start_id = 1 if book.empty else int(pd.to_numeric(book["TradeID"], errors="coerce").fillna(0).max()) + 1
        rows=[]
        for i, r in todays_df.iterrows():
            rows.append({
                "TradeID": start_id + i,
                "Date": _now_utc().strftime("%Y-%m-%d %H:%M"),
                "Ticker": r["Ticker"], "Strategy": r["Strategy"], "Legs": r["Legs"],
                "Exp": r["Legs"].split()[2], "DTE_Orig": r["DTE"],
                "EntryCredit": r["Credit($)"], "MaxLoss": r["MaxLoss($)"],
                "Qty": 1, "Status": "OPEN", "Thesis": r["Thesis"],
                "Spot_Entry": float(yf_history(r["Ticker"], period="1d", interval="1d")["Close"].iloc[-1]),
                "Delta_Sh": r["Delta_Sh"], "Vega_$": r["Vega_$"],
                "Sector": r["Sector"], "SpreadNow": np.nan, "PnL$": np.nan
            })
        book = pd.concat([book, pd.DataFrame(rows)], ignore_index=True)

    # G) Save Excel（本地；Cloud Run 版用 app.py 會寫 GCS）
    save_book(book, todays_df, scan_snapshot)

    # H) Metrics & logging（stdout 由 Cloud Run 收）
    elapsed = max(1e-6, time.time() - t0)
    rps = HTTP_CALLS / elapsed
    print(f"\nCandidates: {len(candidates)} | Selected today: {len(todays_df)}")
    print(f"HTTP calls (approx): {HTTP_CALLS} | 429s caught: {HTTP_429S} | avg RPS: {rps:.3f} | sleep_accum: {SLEEP_SEC_ACCUM:.1f}s")
    print(f"Excel saved -> {EXCEL_PATH}")

    # 回傳三個 DataFrame，給 app.py 上傳到 GCS
    return book, todays_df, scan_snapshot


# --- Jupyter: 呼叫 run_once() 即可 ---
# run_once()
