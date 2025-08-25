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

import io
from google.cloud import storage

GCS_BUCKET = os.getenv("GCS_BUCKET")                     # 例如 "my-bucket"
GCS_LATEST = os.getenv("GCS_LATEST", "sim_trades/latest.xlsx")
GCS_ARCHIVE_PREFIX = os.getenv("GCS_ARCHIVE_PREFIX", "sim_trades/archive/")

def gcs_download_latest():
    if not GCS_BUCKET: return False
    client = storage.Client()
    b = client.bucket(GCS_BUCKET).blob(GCS_LATEST)
    if b.exists(client):
        b.download_to_filename(EXCEL_PATH)
        return True
    return False

def gcs_upload_workbook(sheets: Dict[str, pd.DataFrame]):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        for name, df in sheets.items():
            df.to_excel(w, index=False, sheet_name=name)
    bio.seek(0)

    if not GCS_BUCKET:
        with open(EXCEL_PATH, "wb") as f:
            f.write(bio.getbuffer())
        return

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    # 1) 最新版（覆蓋即可）
    blob_latest = bucket.blob(GCS_LATEST)
    bio.seek(0)
    blob_latest.upload_from_file(
        bio, rewind=True,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # 2) 歸檔版（不覆蓋）：檔名含秒 + 隨機碼
    stamp = _now_utc().strftime("%Y-%m-%d_%H%M%S")
    uniq  = f"{random.randint(1000,9999)}"
    arch_key = f"{GCS_ARCHIVE_PREFIX}sim_trades_{stamp}_{uniq}.xlsx"
    blob_arch = bucket.blob(arch_key)
    bio.seek(0)
    blob_arch.upload_from_file(
        bio, rewind=True,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


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
EXCEL_PATH = os.getenv("EXCEL_PATH", "/tmp/sim_trades.xlsx")


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
CACHE_DIR = Path(os.getenv("CACHE_DIR", "/tmp/.yf_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHAIN_TTL_SEC   = 15 * 60
OPTLIST_TTL_SEC = 6 * 60 * 60
HIST_TTL_SEC    = 30 * 60

RATE_DELAY_BASE = 0.4   # 每次抓期權鏈後 sleep 秒數（再加抖動）
MAX_RETRIES     = 6     # 429 退避最大重試
BACKOFF_BASE    = 1.8   # 退避倍率

LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp/run_logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# HTTP metrics（估算層級；yfinance 內部仍可能有多個請求）
HTTP_CALLS = 0
HTTP_429S  = 0
SLEEP_SEC_ACCUM = 0.0

# ----- Entry/Exit rules & slippage -----
TAKE_PROFIT_PCT   = 0.50   # 目標：吃到 50% 權利金就平倉
STOP_LOSS_MULT    = 2.00   # 風險：虧損達 2x 權利金就停損
EXIT_AT_DTE_LE    = 7      # 到期前 N 天全部了結
SLIPPAGE_PCT      = 0.05   # 進出場滑價（5%）：賣出少收、買回多付
NAV0              = NAV    # 初始資金，用來計算 NAV 曲線


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


OPTIONS_MAX_TRIES  = 2      # 只試 2 次
OPTIONS_BACKOFFS   = [0.35, 0.80]   # 兩次失敗各睡這麼久（秒）

def yf_options_list(ticker) -> List[str]:
    key = f"opts:{ticker}"
    obj = _cache_get(key, OPTLIST_TTL_SEC)
    if obj is not None:
        return obj

    for attempt in range(OPTIONS_MAX_TRIES):
        _count_http()
        exps = []
        try:
            exps = yf.Ticker(ticker).options or []
        except Exception:
            exps = []

        if exps:                      # 成功 -> 回存 + 小睡一下
            _cache_set(key, exps)
            _sleep(0.10 + random.random()*0.15)
            return exps

        # 空清單視為軟限流：輕退避（固定幾百毫秒，不要指數級別）
        _sleep(OPTIONS_BACKOFFS[attempt] + random.random()*0.20)

    return []   # 兩次都空就放棄，不要把空結果快取



def yf_option_chain(ticker, exp):
    key = f"chain:{ticker}:{exp}"
    obj = _cache_get(key, CHAIN_TTL_SEC)
    if obj is not None:
        return obj.get("calls", pd.DataFrame()), obj.get("puts", pd.DataFrame())

    _count_http()
    def _do(): return yf.Ticker(ticker).option_chain(exp)
    ch = _with_retry(_do)
    if ch is None:
        # Fallback：再試一次直呼
        try:
            ch = yf.Ticker(ticker).option_chain(exp)
        except Exception:
            ch = None

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

# 放在檔案頂部
SKIP = {"hist":0,"price":0,"dollar_vol":0,"exp":0,"chain":0,"iv_nan":0,"liq":0}

def option_liq_iv_snapshot(t):
    h = yf_history(t, period="3mo", interval="1d")
    if h.empty:
        SKIP["hist"] += 1; return None
    px = float(h["Close"].iloc[-1])
    if not np.isfinite(px) or px < MIN_PRICE:
        SKIP["price"] += 1; return None

    dollar_vol = float((h["Close"] * h["Volume"]).rolling(20).mean().iloc[-1])
    if not np.isfinite(dollar_vol) or dollar_vol < MIN_DOLLAR_VOL:
        SKIP["dollar_vol"] += 1; return None

    exp = near_dte_exp(t, 21, 60)
    if exp is None:
        SKIP["exp"] += 1; return None

    calls, puts = yf_option_chain(t, exp.strftime("%Y-%m-%d"))
    if calls.empty or puts.empty:
        SKIP["chain"] += 1; return None

    strikes = puts["strike"]; k_idx = (strikes - px).abs().argsort()[:3]
    atm = set(strikes.iloc[k_idx].tolist())
    ivs = pd.concat([
        calls[calls["strike"].isin(atm)]["impliedVolatility"],
        puts[puts["strike"].isin(atm)]["impliedVolatility"]
    ]).replace(0, np.nan).dropna()
    if ivs.empty:
        SKIP["iv_nan"] += 1; return None
    iv = float(np.nanmedian(ivs))

    chain_vol = float(calls["volume"].fillna(0).sum() + puts["volume"].fillna(0).sum())
    chain_oi  = float(calls["openInterest"].fillna(0).sum() + puts["openInterest"].fillna(0).sum())
    if chain_vol < MIN_CHAIN_VOL or chain_oi < MIN_CHAIN_OI:
        SKIP["liq"] += 1; return None

    return dict(ticker=t, price=px, exp=str(exp.date()),
                iv=iv, chain_vol=chain_vol, chain_oi=chain_oi,
                dollar_vol=dollar_vol, atr_pct=float((h["High"]-h["Low"]).rolling(14).mean().iloc[-1]/px))


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




def save_book(positions: pd.DataFrame, todays: pd.DataFrame, scan_table: Optional[pd.DataFrame]=None):
    with pd.ExcelWriter(EXCEL_PATH, engine="xlsxwriter") as w:
        positions.to_excel(w, index=False, sheet_name="Positions")
        todays.to_excel(w, index=False, sheet_name="TodaysPicks")
        if scan_table is not None and not scan_table.empty:
            scan_table.to_excel(w, index=False, sheet_name="ScanSnapshot")



def _parse_strike_legs(legs: str) -> Tuple[float, float, bool]:
    """回傳(short_k, long_k, is_put)"""
    try:
        k_short = float(legs.split()[3].rstrip("PC"))
        is_put  = legs.split()[3].endswith("P")
        k_long  = float(legs.split()[-1].rstrip("PC"))
        return k_short, k_long, is_put
    except Exception:
        return np.nan, np.nan, True

def mark_to_market(row: pd.Series) -> Tuple[float,float]:
    # 與你現有版本相同（這段保留）；若取不到 mid 就回 (nan, nan)
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

def _today_utc_date():
    return _now_utc().date()

def _dte_from_exp(exp_str: str) -> int:
    try:
        d = datetime.strptime(exp_str,"%Y-%m-%d").date()
        return (d - _today_utc_date()).days
    except Exception:
        return 9999

def _apply_slippage_credit(credit: float) -> float:
    return credit * (1.0 - SLIPPAGE_PCT)

def _apply_slippage_debit(debit: float) -> float:
    return debit * (1.0 + SLIPPAGE_PCT)

def decide_exit(row: pd.Series) -> Tuple[bool, Optional[float], Optional[str]]:
    """根據規則判斷是否平倉；回 (should_close, exit_debit, reason)"""
    if str(row.get("Status","")) != "OPEN":
        return (False, None, None)

    entry = float(row.get("EntryCredit", np.nan))
    cur   = float(row.get("SpreadNow",  np.nan))
    dte   = _dte_from_exp(str(row.get("Exp","")))

    if not np.isnan(cur):
        # 目標獲利（拿到 X% 權利金）：買回成本 <= (1 - X%) * Entry
        if cur <= entry * (1.0 - TAKE_PROFIT_PCT):
            return (True, _apply_slippage_debit(max(cur, 0.01)), "TP")

        # 停損（2x 權利金）
        if cur >= entry * STOP_LOSS_MULT:
            return (True, _apply_slippage_debit(cur), "SL")

    # 時間出場：剩餘 DTE <= EXIT_AT_DTE_LE
    if dte <= EXIT_AT_DTE_LE and not np.isnan(cur):
        return (True, _apply_slippage_debit(cur), "DTE")

    # 到期處理（dte <= 0）：盡量用 MTM，若拿不到就用內在價估
    if dte <= 0:
        kS, kL, is_put = _parse_strike_legs(str(row["Legs"]))
        try:
            S = float(yf_history(row["Ticker"], period="1d", interval="1d")["Close"].iloc[-1])
        except Exception:
            S = float(row.get("Spot_Entry", np.nan))
        width = abs(kL - kS) * 100.0
        intrinsic = 0.0
        if is_put:
            intrinsic = max(0.0, (kS - S))*100.0 - max(0.0, (kL - S))*100.0
        else:
            intrinsic = max(0.0, (S - kS))*100.0 - max(0.0, (S - kL))*100.0
        intrinsic = min(max(intrinsic, 0.0), width)
        return (True, _apply_slippage_debit(intrinsic), "EXP")

    return (False, None, None)

def append_trade(trades_df: pd.DataFrame, when_str: str, trade_id: int, side: str,
                 row_like: dict, price: float, note: str="") -> pd.DataFrame:
    new = {
        "Datetime": when_str, "TradeID": trade_id, "Side": side,
        "Ticker": row_like["Ticker"], "Strategy": row_like["Strategy"],
        "Legs": row_like["Legs"], "Exp": row_like["Exp"],
        "Price": round(price,2), "Qty": int(row_like.get("Qty",1)), "Note": note
    }
    return pd.concat([trades_df, pd.DataFrame([new])], ignore_index=True)

def recompute_daily_nav(positions_df: pd.DataFrame) -> Tuple[float, float, float, int]:
    """回 (realized_sum, open_pnl_sum, nav, open_cnt)"""
    opens = positions_df[positions_df["Status"]=="OPEN"]
    closed = positions_df[positions_df["Status"]=="CLOSED"]
    realized = float(pd.to_numeric(closed.get("RealizedPnL", 0.0), errors="coerce").fillna(0.0).sum())
    open_pnl = float(pd.to_numeric(opens.get("PnL$", 0.0), errors="coerce").fillna(0.0).sum())
    nav = NAV0 + realized + open_pnl
    return realized, open_pnl, nav, len(opens)

def _ensure_position_columns(df: pd.DataFrame) -> pd.DataFrame:
    need_cols = ["ExitDate","ExitDebit","RealizedPnL","CloseReason"]
    for c in need_cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

def load_positions() -> pd.DataFrame:
    if os.path.exists(EXCEL_PATH):
        try:
            df = pd.read_excel(EXCEL_PATH, sheet_name="Positions")
            return _ensure_position_columns(df)
        except Exception:
            pass
    cols=["TradeID","Date","Ticker","Strategy","Legs","Exp","DTE_Orig",
          "EntryCredit","MaxLoss","Qty","Status","Thesis",
          "Spot_Entry","Delta_Sh","Vega_$","Sector","SpreadNow","PnL$",
          "ExitDate","ExitDebit","RealizedPnL","CloseReason"]
    return pd.DataFrame(columns=cols)

def load_sheets_all():
    """把舊檔所有 sheet 讀進來，沒有就給空表。"""
    sheets = {}
    if os.path.exists(EXCEL_PATH):
        try:
            sheets = pd.read_excel(EXCEL_PATH, sheet_name=None)
        except Exception:
            pass
    sheets["Positions"]    = _ensure_position_columns(sheets.get("Positions", load_positions()))
    sheets["TodaysPicks"]  = sheets.get("TodaysPicks", pd.DataFrame())
    sheets["ScanSnapshot"] = sheets.get("ScanSnapshot", pd.DataFrame())
    # 交易日誌（每一筆進／出場一列）
    if "Trades" not in sheets:
        sheets["Trades"] = pd.DataFrame(columns=[
            "Datetime","TradeID","Side","Ticker","Strategy","Legs","Exp","Price","Qty","Note"
        ])
    # NAV/績效日線
    if "DailyNAV" not in sheets:
        sheets["DailyNAV"] = pd.DataFrame(columns=["Date","RealizedPnLToDate","OpenPnL","NAV","OpenPositions"])
    return sheets

def save_all_sheets(sheets: Dict[str, pd.DataFrame]):
    with pd.ExcelWriter(EXCEL_PATH, engine="xlsxwriter") as w:
        for name, df in sheets.items():
            df.to_excel(w, index=False, sheet_name=name)



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

    # 先把最新 Excel 從 GCS 拉下來到 /tmp
    try:
        gcs_download_latest()
    except Exception as e:
        print("GCS download skipped/failed:", e)

    


    # 載入所有 sheet
    sheets = load_sheets_all()
    book = sheets["Positions"].copy()
    trades = sheets["Trades"].copy()
    daily = sheets["DailyNAV"].copy()

    # A) Scanner
    tickers, scan_snapshot = scan_liquid_high_iv(TOP_N)
    print(f"Universe picks (top {TOP_N}):", tickers)

    # B) MTM（先更新所有 OPEN 的 SpreadNow、PnL$）
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

    # C) 先嘗試平倉（TP/SL/DTE/到期）
    if not book.empty:
        changed_idx = []
        for idx, r in book.iterrows():
            if str(r.get("Status","")) != "OPEN":
                continue
            should_close, exit_debit, reason = decide_exit(r)
            if should_close and (exit_debit is not None) and np.isfinite(exit_debit):
                when = _now_utc().strftime("%Y-%m-%d %H:%M")
                trade_id = int(r["TradeID"])
                qty = int(r.get("Qty",1))
                entry_credit = float(r["EntryCredit"])     # 這個值本來就已經扣過滑價
                realized = (entry_credit - float(exit_debit)) * qty
                book.loc[idx, "ExitDate"] = when
                book.loc[idx, "ExitDebit"] = round(float(exit_debit),2)
                book.loc[idx, "RealizedPnL"] = round(realized,2)
                book.loc[idx, "Status"] = "CLOSED"
                book.loc[idx, "CloseReason"] = reason
                trades = append_trade(trades, when, trade_id, "CLOSE", r, float(exit_debit), note=reason)
                changed_idx.append(idx)

        if changed_idx:
            print(f"Closed {len(changed_idx)} positions:", changed_idx)

    # D) 重新計算平倉後的 MTM（避免平倉後還殘留 PnL）
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

    # E) 避免重複腿
    open_legs = set(book.loc[book["Status"]=="OPEN", "Legs"]) if not book.empty else set()

    # 既有 OPEN 籃子曝險
    def existing_open_exposure(book_df: pd.DataFrame) -> Tuple[float, float]:
        if book_df.empty: return 0.0, 0.0
        opens = book_df[book_df["Status"]=="OPEN"].copy()
        if opens.empty: return 0.0, 0.0
        tickers_ = opens["Ticker"].unique().tolist()
        S_map: Dict[str, float] = {}
        for t in tickers_:
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

    # F) 只對 Scanner 的 tickers 建候選、過濾、排名、選出今天要開的新倉
    candidates: List[Candidate] = []
    for t in tickers:
        try:
            candidates += build_spreads_for_ticker(t)
        except Exception:
            continue
    candidates = [c for c in candidates if passes_hard_filters(c)]
    ranked = rank_candidates(candidates)

    todays = []
    sector_counts: Dict[str,int] = {}
    selected: List[Candidate] = []

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

    # G) Paper BUY：把今日選到的單寫進 Positions + Trades（含滑價）
    if not todays_df.empty:
        start_id = 1 if book.empty else int(pd.to_numeric(book["TradeID"], errors="coerce").fillna(0).max()) + 1
        rows=[]
        for i, r in todays_df.iterrows():
            entry_credit_eff = _apply_slippage_credit(float(r["Credit($)"]))
            when = _now_utc().strftime("%Y-%m-%d %H:%M")
            row_new = {
                "TradeID": start_id + i,
                "Date": when,
                "Ticker": r["Ticker"], "Strategy": r["Strategy"], "Legs": r["Legs"],
                "Exp": r["Legs"].split()[2], "DTE_Orig": r["DTE"],
                "EntryCredit": entry_credit_eff, "MaxLoss": r["MaxLoss($)"],
                "Qty": 1, "Status": "OPEN", "Thesis": r["Thesis"],
                "Spot_Entry": float(yf_history(r["Ticker"], period="1d", interval="1d")["Close"].iloc[-1]),
                "Delta_Sh": r["Delta_Sh"], "Vega_$": r["Vega_$"],
                "Sector": r["Sector"], "SpreadNow": np.nan, "PnL$": np.nan,
                "ExitDate": np.nan, "ExitDebit": np.nan, "RealizedPnL": np.nan, "CloseReason": np.nan
            }
            rows.append(row_new)
            # 交易日誌記一筆「OPEN」
            trades = append_trade(trades, when, row_new["TradeID"], "OPEN", row_new, entry_credit_eff, note="ENTRY")

        book = pd.concat([book, pd.DataFrame(rows)], ignore_index=True)

    # H) NAV 曲線：當日快照
    realized, open_pnl, nav, open_cnt = recompute_daily_nav(book)
    daily = pd.concat([daily, pd.DataFrame([{
        "Date": _now_utc().strftime("%Y-%m-%d"),
        "RealizedPnLToDate": round(realized,2),
        "OpenPnL": round(open_pnl,2),
        "NAV": round(nav,2),
        "OpenPositions": open_cnt
    }])], ignore_index=True).drop_duplicates(subset=["Date"], keep="last")

    # I) 存檔
    sheets["Positions"]    = book
    sheets["TodaysPicks"]  = todays_df
    sheets["ScanSnapshot"] = scan_snapshot
    sheets["Trades"]       = trades
    sheets["DailyNAV"]     = daily
    

    # I) 存檔（本地）
    save_all_sheets(sheets)

    # I+) 上傳 GCS（latest 會覆蓋，archive 不要覆蓋）
    try:
        gcs_upload_workbook(sheets)
        print(f"Uploaded to gs://{GCS_BUCKET}/{GCS_LATEST} (+ archived)")
    except Exception as e:
        print("GCS upload failed (kept local copy):", e)

    # J) Metrics
    elapsed = max(1e-6, time.time() - t0)
    rps = HTTP_CALLS / elapsed
    print(f"\nCandidates: {len(candidates)} | Selected today: {len(todays_df)}")
    print(f"HTTP calls (approx): {HTTP_CALLS} | 429s caught: {HTTP_429S} | avg RPS: {rps:.3f} | sleep_accum: {SLEEP_SEC_ACCUM:.1f}s")
    print(f"Excel saved -> {EXCEL_PATH}")
    print("Skip reasons:", SKIP)

    return book, todays_df, scan_snapshot



# --- Jupyter: 呼叫 run_once() 即可 ---
# run_once()
