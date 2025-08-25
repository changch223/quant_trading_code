# 每日 09:00（紐約時間）自動執行的期權信用價差掃描系統 — 技術說明（中文版）

> 目的：讓合伙人快速理解**系統如何選股、如何建議買進/賣出（開/平倉）**、使用了哪些**因子與條件**、以及**輸出 Excel 的格式與如何閱讀**。文末附上**優化投資策略**與**工程改進**建議。

---

## TL;DR（一句話版本）

每天 09:00 ET（紐約）觸發一次 `/run`：
系統從 **S\&P500 宇宙**挑出**流動性佳、隱波高**的 15 檔，對每檔在 **25–45 DTE** 的到期日上，用 **Black-Scholes** 估算 POP、Delta、Vega，依 **利潤/風險比、最低權利金、部位/曝險限制** 等硬條件做篩選，為每檔生成 **賣出價外 Put/Call 信用價差** 候選，排名後**最多選 5 筆**開倉；既有部位依 **50% 收回權利金止盈、2x 權利金停損、到期前 7 天出場** 規則自動平倉。結果寫到 GCS 的 Excel（含 Positions/TodaysPicks/ScanSnapshot/Trades/DailyNAV）。

---

## 1. 系統架構與排程

### 1.1 元件一覽

* `scanner_core.py`：**核心策略**與**風控/交易簿**管理（本檔負責 95% 的邏輯）。
* `app.py`：Flask API（`/run`、`/healthz`），由 **Cloud Scheduler** 以 HTTP 觸發。
* `Dockerfile`：Cloud Run 容器。
* `requirements.txt`：相依套件（pandas、yfinance、google-cloud-storage…）。

### 1.2 每日排程（紐約 09:00）

* 建議用 **Cloud Scheduler** → **Cloud Run**（HTTP GET `/run`）。
* **注意**：09:00 ET 尚未開盤（9:30 ET 開盤），此時 yfinance 期權報價多為**前日收盤**或**延遲**，見【9. 交易時段風險】的建議。

### 1.3 GCS / 檔案路徑

* `scanner_core.py` 內建：

  * `GCS_LATEST = sim_trades/latest.xlsx`（**最新檔**，覆蓋）
  * `GCS_ARCHIVE_PREFIX = sim_trades/archive/`（**歸檔**，時間戳唯一）
* `app.py` 另存一份（預設 `XLSX_KEY=sim_trades.xlsx`），**內容只有 3 個分頁**。
  ▶ **建議統一**：只保留 `scanner_core.py` 的 `gcs_upload_workbook()` 完整版，以免兩份檔案混淆（詳見【11. 工程改進】）。

---

## 2. 宇宙與初步流動性/隱波掃描（ScanSnapshot）

### 2.1 股票宇宙

* 預設 `UNIVERSE="sp500"`：嘗試抓維基 S\&P500 成分；失敗時有備援清單。

### 2.2 初步資料與硬性流動性條件

對每檔標的建立 **快照**（`option_liq_iv_snapshot`）：

1. **價格**：近日收盤 `Close`，須 ≥ `MIN_PRICE=10`。
2. **平均成交額**：近 20 日 `Close*Volume` 平均，須 ≥ `MIN_DOLLAR_VOL=5e7`（≥ 5,000 萬美元）。
3. **到期日**：近似於 `MIN_DTE=25` \~ `MAX_DTE=45` 天（允許 21–60 的寬容挑選邏輯）。
4. **期權鏈**：該到期日的 calls/puts 必須取得。
5. **IV 估算**：以**貼近 ATM 的 3 檔**（從 puts 與 calls 合併）取 `impliedVolatility` 的 **中位數**作為該標的 IV。
6. **整體流動性**（該到期日加總）：

   * 成交量 `chain_vol` ≥ `MIN_CHAIN_VOL=2000`
   * 未平倉量 `chain_oi` ≥ `MIN_CHAIN_OI=5000`
7. **波動代理**：14 日平均的 `(High-Low)/Close` 當作 `atr_pct`。

> 不符合者記入 `SKIP` 計數（hist/price/dollar\_vol/exp/chain/iv\_nan/liq）以利診斷。

### 2.3 打分與 Top N

* 對 `iv`, `atr_pct`, `dollar_vol`, `chain_vol`, `chain_oi` 做 **z-score**，計分：

  ```
  score = 0.45*iv_z 
        + 0.25*atr_pct_z 
        + 0.30*平均( dollar_vol_z, chain_vol_z, chain_oi_z )
  ```
* 取 **TOP\_N=15** 檔為「候選股票清單」；此表即 **ScanSnapshot** 分頁。

---

## 3. 為每檔股票構建信用價差（Put/Call）

### 3.1 共同設定

* 目標到期：上述範圍內的單一到期日（每檔各自挑一個）。
* 使用 **Black-Scholes** 計算：

  * `POP`（probability of profit proxy）：

    * Put：`P[S_T > Kshort] = Φ(d2)`
    * Call：`P[S_T < Kshort] = Φ(-d2)`
  * `Delta / Vega`（短腿、長腿皆算，再相減得淨值）。
* **權利金（中間價）**：`(bid+ask)/2`；若缺值 → `lastPrice`；再缺 → **理論價**（B-S）。
* **IV 缺值處理**：0→NaN→以中位數補，仍無 → 0.5 fallback。

### 3.2 價差結構與搜尋空間

* **價差寬度**：最小為**該到期日的最小跳動檔距**（strike step），最大為 `min(5, 2*step)`。
* **目標 Delta**：

  * Put（賣出價外 Put 信用價差）：`PUT_DELTA_TARGETS = [0.20, 0.25, 0.30, 0.35]`（以 |Δ| 接近）
  * Call（賣出價外 Call 信用價差）：`CALL_DELTA_TARGETS = [0.30, 0.35, 0.40, 0.45]`（以 Δ 接近）
* 若找不到合適 Delta 的短腿，退而求其次取 **最接近 ATM 的 3 檔**嘗試。

### 3.3 風險/收益硬條件（單筆候選）

對每個（短腿、長腿）組合，計算：

* **Credit**（每價差總權利金，乘 100）
* **MaxLoss = 寬度\*100 − Credit**
* **濾條**：

  * `MaxLoss <= MAX_LOSS_PER_TRADE = 0.5% * NAV`（例：`NAV=100,000` → 每筆最多 500 美元）
  * `Credit / MaxLoss >= MIN_CR_ML = 0.33`
  * `Credit >= MIN_CREDIT_DOLLARS = 60`
  * `POP >= MIN_POP = 0.70`
* **評分**：`score = POP * Credit`，每檔標的分別挑 **Put 最佳**與 **Call 最佳** *各一*（若存在）。

### 3.4 牛熊方向邏輯

* 以 **20 日報酬** `m` 判斷先建哪一邊：

  * 若 `m >= 0`：**先 Put** 再 Call（偏向賣 Put）。
  * 否則：**先 Call** 再 Put。

---

## 4. 多標的整體排名與當日選股（最多 5 筆）

### 4.1 候選排名分數

```
rank_score = POP * Credit 
           * (1 + 0.03 * max(0, momentum_z) + 0.02 * max(0, flow_z))
```

* `momentum_z`：近 6 個月資料估 20 日報酬的 z-score。
* `flow_z`：當日成交量相對 20 日均值的 z-score（量能異動）。

### 4.2 結構性限制

逐一納入排名後的候選，直到**最多 5 筆**或無法再納入：

* **去重**：避免與既有 OPEN 部位 **同一支 Legs**（完全重覆）。
* **曝險限制**（`exposure_ok`）：

  * 將 **Δ（股數）× 現價** 加總 ⇒ `delta_dollars`；
    允許 **`delta_dollars / NAV ∈ [-0.30, +0.30]`**。
  * **Vega/NAV** 不得過度偏空 ⇒ **`vega_ratio >= -0.05`**（可偏多，不設上限）。
* **產業分散**：同一產業**最多 2 筆**。

> ✅ 通過者寫入 **TodaysPicks**；同時計算入場時的**滑價**（見下）。

---

## 5. 下單（紙上交易）與滑價模型

* **入場滑價**：賣出 credit 乘以 `(1 - SLIPPAGE_PCT)`；預設 `SLIPPAGE_PCT=0.05`（少收 5% 權利金）。
* **下單數量**：`Qty = 1`（可擴充為風險配重）。
* 建倉紀錄寫入：

  * **Positions**（主交易簿）
  * **Trades**（交易日誌，記一筆 `OPEN`）

---

## 6. 既有部位的每日標記與平倉規則

### 6.1 每日標記（MTM）

* 以同到期日同 strikes 的短/長腿**中間價**計算當前價差 `SpreadNow`（取不到則 NaN）。
* `PnL$ = (EntryCredit - SpreadNow) * Qty`。

### 6.2 平倉規則（`decide_exit`）

僅對 `Status=="OPEN"` 的部位：

1. **止盈 TP**：`SpreadNow <= EntryCredit * (1 - TAKE_PROFIT_PCT)`；預設 `TAKE_PROFIT_PCT=0.50`（吃到 50% 權利金）。
2. **停損 SL**：`SpreadNow >= EntryCredit * STOP_LOSS_MULT`；預設 `STOP_LOSS_MULT=2.0`（虧損達 2 倍入場權利金）。
3. **時間出場 DTE**：`剩餘天數 <= EXIT_AT_DTE_LE=7`。
4. **到期日處理**：若當天到期，先嘗試 MTM；取不到價就用**內在價**估值。

> **出場滑價**：買回 `debit * (1 + SLIPPAGE_PCT)`（多付 5%），以模擬吃價成交。
> 平倉後寫入 **ExitDate/ExitDebit/RealizedPnL/CloseReason**，並在 **Trades** 記一筆 `CLOSE`。

---

## 7. 風控：NAV、每筆最大風險、曝險邊界

* `NAV`：淨值基準（預設 `100,000`）。

  * 每筆**最大虧損**：`MAX_LOSS_PER_TRADE = 0.5% * NAV`。
    例：若你把 `NAV` 設為 `100_000_000`，則單筆允許最大虧損為 **\$500,000**。
    ▶ **建議**：無論實際資金多寡，`每筆風險`維持 **0.25%–0.75% NAV** 區間較穩健，可依策略勝率/夏普調整。
* **全帳戶曝險**：`|Delta_dollars/NAV| ≤ 0.30`；`Vega/NAV ≥ -0.05`。

---

## 8. Excel 輸出與閱讀指南（最重要）

系統最終把所有表格寫入同一個 Excel（本地 `/tmp/sim_trades.xlsx`，並上傳到 GCS：**latest + archive**）。

### 8.1 主要分頁

1. **Positions**（主交易簿）
   \| 欄位 | 說明 |
   \|---|---|
   \| TradeID | 交易流水號（整數遞增） |
   \| Date | 建倉時間（UTC） |
   \| Ticker | 標的代號 |
   \| Strategy | `Short Put Credit Spread` / `Short Call Credit Spread` |
   \| Legs | 例如：`Short AAPL 2025-10-17 180P / Long 175P` |
   \| Exp | 到期日（YYYY-MM-DD） |
   \| DTE\_Orig | 建倉時距到期天數 |
   \| EntryCredit | 入場權利金（已扣 5% 滑價），單位：USD/價差 |
   \| MaxLoss | 該價差最大虧損 |
   \| Qty | 數量（目前=1） |
   \| Status | `OPEN` / `CLOSED` |
   \| Thesis | 簡述（例如「IV高、動能穩健…」） |
   \| Spot\_Entry | 入場時標的現價 |
   \| Delta\_Sh | 淨 Delta（股數） |
   \| Vega\_\$ | 淨 Vega（每 1.00 vol 變化的美元變動） |
   \| Sector | 產業 |
   \| SpreadNow | 當前中間價（短腿−長腿）×100 |
   \| PnL\$ | 含未實現損益（USD） |
   \| ExitDate | 平倉時間（UTC） |
   \| ExitDebit | 出場成本（含 5% 滑價） |
   \| RealizedPnL | 已實現損益（USD） |
   \| CloseReason | `TP`/`SL`/`DTE`/`EXP` |

> **閱讀重點**：
>
> * **風險**：看 `MaxLoss` 與 `Qty`；
> * **進度**：`SpreadNow` 相對 `EntryCredit` 的比率（離 TP/SL 多遠）；
> * **部位健康**：`DTE`（可由 `Exp` 推回）、`PnL$`、`Delta_Sh`/`Vega_$` 合計占 NAV 的比例。

2. **TodaysPicks**（當日新建倉建議清單／實際已下單）
   \| 欄位 | 說明 |
   \|---|---|
   \| Ticker / Strategy / Legs | 同上 |
   \| POP | 買方不觸發區間機率（B-S） |
   \| Credit(\$) | 權利金（未扣滑價的理論入場） |
   \| MaxLoss(\$) | 最大虧損 |
   \| DTE | 到期天數 |
   \| Delta\_Sh / Vega\_\$ | 淨 Greeks |
   \| Thesis / Sector | 簡述與產業 |

> **閱讀重點**：當日選入的 1–5 檔，**POP ≥ 0.7**、**Credit/MaxLoss ≥ 0.33**、**每筆 MaxLoss ≤ 0.5% NAV**、**產業≤2 檔**。

3. **ScanSnapshot**（宇宙掃描結果 Top 15）
   \| 欄位 | 說明 |
   \|---|---|
   \| ticker / price / exp | 標的、現價、擬用到期 |
   \| iv / atr\_pct | ATM IV 估計、ATR% |
   \| chain\_vol / chain\_oi | 期權鏈總成交量/未平倉量 |
   \| dollar\_vol | 20 日 \$ 成交額 |
   \| \*\_z | 各欄 z-score |
   \| score | 綜合打分（用來挑 Top 15） |

> **閱讀重點**：觀察當日被挑入候選的市場面貌（是高 IV 的哪些產業？）。

4. **Trades**（交易日誌）
   \| 欄位 | 說明 |
   \|---|---|
   \| Datetime | UTC 時間 |
   \| TradeID | 對應 Positions |
   \| Side | `OPEN` / `CLOSE` |
   \| Ticker / Strategy / Legs / Exp | 如上 |
   \| Price | OPEN=入場 credit（已扣 5%），CLOSE=出場 debit（含 5%） |
   \| Qty / Note | 數量；原因（ENTRY/TP/SL/DTE/EXP） |

> **閱讀重點**：逐筆審計；對齊**實單執行**時也可對接此格式。

5. **DailyNAV**（績效曲線快照，逐日唯一一筆）
   \| 欄位 | 說明 |
   \|---|---|
   \| Date | YYYY-MM-DD |
   \| RealizedPnLToDate | 截至當日的累計已實現損益 |
   \| OpenPnL | 當日所有 OPEN 的未實現損益合計 |
   \| NAV | `NAV0 + Realized + OpenPnL` |
   \| OpenPositions | 未平倉筆數 |

> **閱讀重點**：用來畫資產曲線與回撤；追蹤**倉位數**變化。

---

## 9. 交易時段與數據品質風險（重要）

* **09:00 ET** 尚未開盤，yfinance 期權鏈常是**前日收盤或延遲**。
  **建議**：

  1. 若以日內執行為主，調整排程到 **09:40–10:30 ET**（開盤後報價較穩）。
  2. 或明確定位為 **隔日 EOD 決策**（以昨日收盤資料運作），並在簡報/報告中註明。

---

## 10. 主要參數（可調矩陣）

| 參數                   |         目前值 | 影響                                   |
| -------------------- | ----------: | ------------------------------------ |
| `NAV`                |     100,000 | 基準淨值；影響 `MAX_LOSS_PER_TRADE` 與曝險比率分母 |
| `MAX_LOSS_PER_TRADE` | 0.5% \* NAV | 單筆風險上限                               |
| `MIN_POP`            |        0.70 | 機率門檻                                 |
| `MIN_CR_ML`          |        0.33 | Credit / MaxLoss 下限                  |
| `MIN_CREDIT_DOLLARS` |          60 | 最低權利金                                |
| `MIN_DTE, MAX_DTE`   |      25, 45 | 目標到期區間                               |
| `TOP_N`              |          15 | 初選股票數                                |
| `PUT_DELTA_TARGETS`  |   0.20–0.35 | Put 短腿 Delta 目標                      |
| `CALL_DELTA_TARGETS` |   0.30–0.45 | Call 短腿 Delta 目標                     |
| `TAKE_PROFIT_PCT`    |        0.50 | 止盈比例                                 |
| `STOP_LOSS_MULT`     |         2.0 | 停損倍數                                 |
| `EXIT_AT_DTE_LE`     |           7 | 到期前強制出場                              |
| `SLIPPAGE_PCT`       |        0.05 | 進出場滑價                                |
| `MIN_PRICE`          |          10 | 避免仙股                                 |
| `MIN_DOLLAR_VOL`     |         5e7 | 流動性（\$）門檻                            |
| `MIN_CHAIN_VOL`      |       2,000 | 期權鏈量門檻                               |
| `MIN_CHAIN_OI`       |       5,000 | 期權鏈 OI 門檻                            |

> 若你把 `NAV` 提高到 `100_000_000`，請同步評估：
>
> * `MAX_LOSS_PER_TRADE` 是否仍用 0.5%（單筆 50 萬美金上限）；
> * 是否需要**提高最低權利金**（例如從 \$60 → \$600），確保 **Credit/滑價/交易成本** 仍相對合理；
> * 曝險邊界（Delta、Vega 比率）可維持**比例不變**。

---

## 11. 工程改進建議（落地易、風險低）

1. **單一真相來源（Single Source of Truth）**

   * 目前 `app.py` 會另寫一份只含三分頁的 Excel；`scanner_core.py` 又寫完整工作簿到 `latest.xlsx + archive`。
     **改法**：移除 `app.py` 的另存，或改成直接重用 `scanner_core.gcs_upload_workbook()`；只維護 **一份** GCS 路徑與格式。

2. **時區與排程**

   * Cloud Scheduler 設定**時區為 America/New\_York**；若改 09:40–10:30 執行，可避開開盤瞬間跳動與前日延遲。

3. **觀測性**

   * 把 `Skip reasons`、HTTP 次數、429 次數、睡眠秒數等**指標**輸出到 **Cloud Logging**（或 BigQuery），做日誌面板。

4. **重試/快取**

   * 已有本地快取（/tmp）。如部署多實例，建議以 **GCS 或 Memorystore** 做共享快取，降低 Wikipedia/yfinance 壓力。

---

## 12. 策略優化方向（投研層面）

> 以下為能讓**期望值更穩**、**勝率與盈虧比**更均衡的方向，建議分階段 A/B 驗證。

### 12.1 排名與特徵提升

* **IV Rank / IV Percentile（1 年）**：以相對位階取代單日 IV 絕對值 → 減少橫向標的差異對比分歧。
* **Earnings / 股利** 過濾：到期日含財報前後 ±7 天的標的排除或降權。
* **Skew/Smile**：用短腿附近的 **Put-Call IV 差** 辨識偏態，決定偏向賣 Put 或 Call。
* **技術距離**：短腿距離 `k` 至近期高/低點或 `N*ATR` 的 buffer（例如 ≥ 1.5×ATR）。
* **Term Structure**：若 IV 曲線倒掛，考慮放寬 DTE 或避開該到期群。
* **Probability of Touch** / **Expected Shortfall**：輔助 POP，對 tail risk 友善。

### 12.2 風控與倉位

* **最小權利金**隨標的價位、點差寬度或 **Bid-Ask** 寬度動態調整（例如 Bid-Ask / Mid ≤ 12%）。
* **同一標的**當日/同到期只保留**一筆**，避免重疊風險。
* **相關性/產業/主題**曝險：以**相關矩陣**限制同方向累積。
* **分級止盈**：回收 35%/50%/65% 分段退場，增強勝率尖端的保護。

### 12.3 執行品質

* **時間窗下單**：09:40–10:30 或 15:15–15:45（收盤前），減少價差跳動。
* **委託策略**：以 **Mid − ε** 的限價出清，逐步抬升（參考 NBBO 中位）；實測比固定 5% 滑價更佳。
* **資料源**：yfinance 適合研發，但實盤建議升級成 Tradier/Polygon/OPRA feed。

### 12.4 回測與監控

* **將 DailyNAV/Trades 匯入 notebook** 做 **月度/季度**績效：勝率、盈虧比、期望值、回撤、卡瑟比、每月 NAV 稳定度。
* **分 bucket 分析**：以 `POP/IVR/DTE/Delta` 量化哪些區間最賺/風險最大，反向調參。

---

## 13. 快速檢查清單（交付與日常運維）

* [ ] Cloud Scheduler（America/New\_York）對 `/run` 成功觸發。
* [ ] GCS：`latest.xlsx` 更新、`archive/` 新增檔案。
* [ ] Excel 五分頁齊全：`Positions / TodaysPicks / ScanSnapshot / Trades / DailyNAV`。
* [ ] `Skip reasons` 不異常（若大量 `iv_nan/chain`，多半是資料源波動或過早抓取）。
* [ ] `DailyNAV` 當日只有一筆紀錄（避免重複運行覆蓋）。

---

## 14. 常見問答

* **Q：今天為何沒開任何倉？**
  A：可能因為候選都沒通過硬條件（POP、Credit/MaxLoss、MaxLoss 上限、流動性）或**曝險限制**導致排除。

* **Q：Legs 欄位怎麼讀？**
  A：`Short <TICKER> <YYYY-MM-DD> <Kshort>P/C / Long <Klong>P/C`，短腿在前，例：`Short MSFT 2025-10-17 415C / Long 420C`。

* **Q：為何我的 PnL\$ 為 NaN？**
  A：該到期日/行權價在 yfinance 取不到 Bid/Ask 或 lastPrice；屬資料源限制。下次更新或接近盤中通常恢復。

---

## 15. 環境變數（部署）

```bash
# Cloud Run / 容器
GCS_BUCKET=你的-bucket
GCS_LATEST=sim_trades/latest.xlsx
GCS_ARCHIVE_PREFIX=sim_trades/archive/
EXCEL_PATH=/tmp/sim_trades.xlsx
PORT=8080
```

---

### 附註：`NAV` 合理性說明

* `NAV` 不代表真實資金部位（除非你對齊），而是**風控比例**的基準。
* 若要以 1 億美金為 NAV 進行機構級模擬：

  * 把 `MIN_CREDIT_DOLLARS` 相應上調（例如 ≥ \$600～\$1,000），確保 **交易成本/滑價**佔比合理。
  * 增設**每日最多新倉筆數**與**每標的/每產業上限金額**，防止集中風險。

---
