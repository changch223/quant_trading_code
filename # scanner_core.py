# scanner_core.py
# 封裝成 run_once() 供本地/雲端重複呼叫
# 需求：pandas, numpy, yfinance, xlsxwriter

import os, math, json, warnings
import numpy as np
import pandas as pd
import yfinance as yf
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict
from datetime import datetime, timezone

warnings.filterwarnings("ignore", category=FutureWarning)

# ================== 全域參數 ==================
NAV_DEFAULT             = 10_000.0      # 啟動資金（本函式參數可覆蓋）
MAX_LOSS_PER_TRADE_PCT  = 0.005         # 每筆 <= 0.5% NAV
MIN_POP                 = 0.70          # 勝率門檻
MIN_CR_ML               = 0.33          # credit / max_loss
MIN_CREDIT_DOLLARS      = 60.0          # 最低權利金
RISK_FREE               = 0.045         # 年化無風險利率
MIN_DTE, MAX_DTE        = 25, 45
TOP_N                   = 15            # 每日掃描保留前 15 名
MIN_PRICE               = 10.0
MIN_DOLLAR_VOL          = 5e7           # 20日均額 >= $50m
MIN_CHAIN_VOL           = 2_000         # 當期權到期的總量 >= 2000
MIN_CHAIN_OI            = 5_000         # 當期權到期的 OI >= 5000

PUT_DELTA_TARGETS       = [0.20, 0.25, 0.30, 0.35]
CALL_DELTA_TARGETS      = [0.30, 0.35, 0.40, 0.45]

# ================== 常用數學（BSM） ==================
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
    return norm_cdf(_d2) if _d2 is not None else None

def pop_short_call(S, K, iv, T):
    _d2 = d2(S, K, RISK_FREE, iv, T)
    return norm_cdf(-_d2) if _d2 is not None else None

# ================== 掃描 Universe ==================
def get_universe():
    # S&P500（可能失敗就 fallback）
    try:
        tbls = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        syms = sorted(tbls[0]["Symbol"].unique().tolist())
        return [s.replace(".", "-") for s in syms]  # BRK.B -> BRK-B
    except Exception:
        return ["AAPL","MSFT","NVDA","AMD","TSLA","META","GOOGL","AMZN","NFLX",
                "XOM","JPM","BAC","SPY","QQQ","TQQQ","AVGO","COST","WMT","PEP","UNH"]

def near_dte_exp(ticker, min_dte=MIN_DTE, max_dte=MAX_DTE) -> Optional[pd.Timestamp]:
    tk = yf.Ticker(ticker)
    try: exps = tk.options
    except: return None
    if not exps: return None
    today = datetime.now(timezone.utc).date()
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
    tk = yf.Ticker(t)
    h = tk.history(period="3mo", interval="1d")
    if h.empty: return None
    px = float(h["Close"].iloc[-1])
    if not np.isfinite(px) or px < MIN_PRICE: return None

    dollar_vol = float((h["Close"] * h["Volume"]).rolling(20).mean().iloc[-1])
    if not np.isfinite(dollar_vol) or dollar_vol < MIN_DOLLAR_VOL:
        return None

    exp = near_dte_exp(t, 21, 60)
    if exp is None: return None
    try:
        ch = tk.option_chain(exp.strftime("%Y-%m-%d"))
        calls, puts = ch.calls, ch.puts
    except Exception:
        return None
    if calls.empty or puts.empty: return None

    # 近 ATM 的中位 IV
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

    atr_pct = float((h["High"] - h["Low"]).rolling(14).mean().iloc[-1] / px)

    return dict(ticker=t, price=px, exp=str(exp.date()),
                iv=iv, chain_vol=chain_vol, chain_oi=chain_oi,
                dollar_vol=dollar_vol, atr_pct=atr_pct)

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
    df["score"] = 0.45*df["iv_z"] + 0.25*df["atr_pct_z"] + 0.30*((df["dollar_vol_z"]+df["chain_vol_z"]+df["chain_oi_z"])/3)
    picks = df.sort_values("score", ascending=False).head(top_n)
    return picks["ticker"].tolist(), picks

# ================== 小工具 ==================
def get_sector(ticker) -> str:
    try:
        info = yf.Ticker(ticker).get_info()
        return (info.get("sector") or "Unknown")
    except Exception:
        return "Unknown"

def momentum_volume_features(ticker) -> Tuple[float, float]:
    try:
        h = yf.Ticker(ticker).history(period="6mo", interval="1d")
        if len(h) < 40: return 0.0, 0.0
        ret20 = h["Close"].pct_change(20).iloc[-1]
        mom_series = h["Close"].pct_change(20).dropna()
        momentum_z = (ret20 - mom_series.mean()) / (mom_series.std() + 1e-9)
        vol = h["Volume"].astype(float)
        volume_z = (vol.iloc[-1] - vol.rolling(20).mean().iloc[-1]) / (vol.rolling(20).std().iloc[-1] + 1e-9)
        return float(momentum_z), float(volume_z)
    except Exception:
        return 0.0, 0.0

def _theo_price(opt_type, S, K, iv, T, r=RISK_FREE):
    _d1 = d1(S, K, r, iv, T); _d2 = d2(S, K, r, iv, T)
    if _d1 is None or _d2 is None: return np.nan
    if opt_type == "call":
        return S*norm_cdf(_d1) - K*math.exp(-r*T)*norm_cdf(_d2)
    else:
        return K*math.exp(-r*T)*norm_cdf(-_d2) - S*norm_cdf(-_d1)

def _prepare_chain(df: pd.DataFrame, opt_type: str, S: float, T: float) -> pd.DataFrame:
    df = df.copy()
    # 填 IV（0->NaN->中位數->0.5）
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

    if opt_type == "put":
        df["_delta"] = df.apply(lambda r: greeks_put(S, float(r["strike"]), RISK_FREE, float(r["impliedVolatility"]), T)[0], axis=1)
    else:
        df["_delta"] = df.apply(lambda r: greeks_call(S, float(r["strike"]), RISK_FREE, float(r["impliedVolatility"]), T)[0], axis=1)
    return df

def _detect_step(strikes: pd.Series) -> float:
    s = np.sort(strikes.unique())
    diffs = np.diff(s); diffs = diffs[diffs>0]
    return float(np.min(diffs)) if len(diffs) else 1.0

# ================== 候選建構 ==================
@dataclass
class Candidate:
    ticker: str
    sector: str
    strategy: str
    legs: str
    exp: str
    dte: int
    pop: float
    credit: float
    max_loss: float
    net_delta_shares: float
    net_vega_dollars: float
    momentum_z: float
    flow_z: float
    S: float

def build_spreads_for_ticker(ticker) -> List[Candidate]:
    out=[]
    tk = yf.Ticker(ticker)
    exp = near_dte_exp(ticker, MIN_DTE, MAX_DTE)
    if exp is None: return out
    dte = (exp.date() - datetime.now(timezone.utc).date()).days
    T   = dte / 365.0

    h = tk.history(period="6mo", interval="1d")
    if h.empty: return out
    S = float(h["Close"].iloc[-1])
    momentum_z, flow_z = momentum_volume_features(ticker)

    try:
        chain = tk.option_chain(exp.strftime("%Y-%m-%d"))
        calls, puts = chain.calls.copy(), chain.puts.copy()
    except Exception:
        return out
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
                    if max_loss <= 0: continue
                    if (credit / max_loss) < MIN_CR_ML or credit < MIN_CREDIT_DOLLARS: 
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
