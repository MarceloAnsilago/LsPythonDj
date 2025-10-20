# longshort/services/metrics.py
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Dict, Any

# Modelos
from acoes.models import Asset
from cotacoes.models import QuoteDaily

# Util: half-life para OU discreto via regressão Δs_t = α + ρ s_{t-1} + ε
def _half_life(spread: pd.Series) -> float | None:
    s = spread.dropna()
    if len(s) < 3:
        return None
    ds = s.diff().dropna()
    s_lag = s.shift(1).dropna()
    ds = ds.loc[s_lag.index]
    if len(ds) < 3:
        return None
    # OLS simples: ds = a + rho * s_{t-1}
    X = np.vstack([np.ones(len(s_lag)), s_lag.values]).T
    y = ds.values
    try:
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        rho = beta[1]
        # evitar log de <=0
        if 1 + rho <= 0:
            return None
        hl = -np.log(2) / np.log(1 + rho)
        if np.isfinite(hl) and hl > 0:
            return float(hl)
        return None
    except Exception:
        return None

def _last_corr(returns_a: pd.Series, returns_b: pd.Series, lookback: int) -> float | None:
    a = returns_a.dropna().tail(lookback)
    b = returns_b.dropna().tail(lookback)
    idx = a.index.intersection(b.index)
    if len(idx) < max(10, int(0.6 * lookback)):  # evitar amostra muito curta
        return None
    c = np.corrcoef(a.loc[idx].values, b.loc[idx].values)
    val = c[0, 1]
    return float(val) if np.isfinite(val) else None

def compute_pair_window_metrics(*, pair, window: int) -> Dict[str, Any]:
    """
    Calcula métricas para um par (pair.left, pair.right) na janela 'window' (em dias úteis da sua base).
    Retorna dict com:
      adf_pvalue, beta, zscore, half_life, corr30, corr60, n_samples
    """
    left = pair.left
    right = pair.right

    # Carrega últimos 'window' candles **alinhados por data** (inner join)
    # Estratégia: puxa um pouco mais e faz o alinhamento em pandas.
    ql = (QuoteDaily.objects
          .filter(asset=left)
          .values("date", "close")
          .order_by("-date")[:window*2])
    qr = (QuoteDaily.objects
          .filter(asset=right)
          .values("date", "close")
          .order_by("-date")[:window*2])

    df_l = pd.DataFrame(list(ql)).rename(columns={"close": "close_l"})
    df_r = pd.DataFrame(list(qr)).rename(columns={"close": "close_r"})
    if df_l.empty or df_r.empty:
        return {"n_samples": 0}

    df = pd.merge(df_l, df_r, on="date", how="inner").sort_values("date").tail(window)
    n = len(df)
    if n < 60:
        return {"n_samples": int(n)}

    # Preços em log
    px_l = np.log(df["close_l"].astype(float))
    px_r = np.log(df["close_r"].astype(float))

    # Estima beta via OLS: y = a + beta * x  (y=left, x=right)
    X = np.vstack([np.ones(n), px_r.values]).T
    y = px_l.values
    beta_hat = np.linalg.lstsq(X, y, rcond=None)[0][1]

    spread = px_l - beta_hat * px_r
    spread_z = (spread - spread.mean()) / (spread.std(ddof=1) if spread.std(ddof=1) != 0 else 1)

    # ADF no spread (precisa statsmodels)
    try:
        from statsmodels.tsa.stattools import adfuller
        adf_res = adfuller(spread.values, maxlag=1, regression="c", autolag="AIC")
        adf_pvalue = float(adf_res[1])
    except Exception:
        adf_pvalue = None

    # Half-life
    hl = _half_life(spread)

    # Correlações de retornos (log-retornos)
    ret_l = px_l.diff()
    ret_r = px_r.diff()
    corr30 = _last_corr(ret_l, ret_r, 30)
    corr60 = _last_corr(ret_l, ret_r, 60)

    return {
        "adf_pvalue": adf_pvalue,
        "beta": float(beta_hat),
        "zscore": float(spread_z.iloc[-1]) if np.isfinite(spread_z.iloc[-1]) else None,
        "half_life": hl,
        "corr30": corr30,
        "corr60": corr60,
        "n_samples": int(n),
    }


def get_zscore_series(pair, window: int) -> list[tuple[pd.Timestamp, float]]:
    """
    Retorna uma série (date, z) do Z-score do spread (log L - beta * log R)
    calculado na janela informada. O beta é estimado uma única vez via OLS
    no período e o Z-score é padronizado usando média e desvio do spread no período.
    """
    left = pair.left
    right = pair.right

    # Carrega candles a mais para garantir alinhamento e depois corta na janela
    ql = (QuoteDaily.objects
          .filter(asset=left)
          .values("date", "close")
          .order_by("-date")[:window*2])
    qr = (QuoteDaily.objects
          .filter(asset=right)
          .values("date", "close")
          .order_by("-date")[:window*2])

    df_l = pd.DataFrame(list(ql)).rename(columns={"close": "close_l"})
    df_r = pd.DataFrame(list(qr)).rename(columns={"close": "close_r"})
    if df_l.empty or df_r.empty:
        return []

    df = (
        pd.merge(df_l, df_r, on="date", how="inner")
          .sort_values("date")
          .tail(window)
          .reset_index(drop=True)
    )
    n = len(df)
    if n < 10:  # limite mínimo para algo apresentável
        return []

    # Preços em log
    px_l = np.log(df["close_l"].astype(float))
    px_r = np.log(df["close_r"].astype(float))

    # Beta via OLS: y = a + beta * x
    X = np.vstack([np.ones(n), px_r.values]).T
    y = px_l.values
    beta_hat = np.linalg.lstsq(X, y, rcond=None)[0][1]

    # Spread e Z-score padronizado no período todo
    spread = px_l - beta_hat * px_r
    std = spread.std(ddof=1)
    if std == 0 or not np.isfinite(std):
        return []
    spread_z = (spread - spread.mean()) / std

    # Retorna lista (date, z)
    dates = pd.to_datetime(df["date"])
    series = [(d.to_pydatetime() if hasattr(d, "to_pydatetime") else d, float(z))
              for d, z in zip(dates, spread_z)]
    return series


def get_normalized_price_series(pair, window: int) -> list[tuple[pd.Timestamp, float, float]]:
    """
    Retorna uma série (date, norm_left, norm_right) com os preços normalizados
    (base 100) para o par na janela informada.
    """
    left = pair.left
    right = pair.right

    ql = (QuoteDaily.objects
          .filter(asset=left)
          .values("date", "close")
          .order_by("-date")[:window * 2])
    qr = (QuoteDaily.objects
          .filter(asset=right)
          .values("date", "close")
          .order_by("-date")[:window * 2])

    df_l = pd.DataFrame(list(ql)).rename(columns={"close": "close_l"})
    df_r = pd.DataFrame(list(qr)).rename(columns={"close": "close_r"})
    if df_l.empty or df_r.empty:
        return []

    df = (
        pd.merge(df_l, df_r, on="date", how="inner")
          .sort_values("date")
          .tail(window)
          .reset_index(drop=True)
    )
    if df.empty or len(df) < 2:
        return []

    base_l = float(df["close_l"].iloc[0])
    base_r = float(df["close_r"].iloc[0])
    if base_l == 0 or base_r == 0:
        return []

    df["norm_left"] = (df["close_l"].astype(float) / base_l) * 100.0
    df["norm_right"] = (df["close_r"].astype(float) / base_r) * 100.0

    dates = pd.to_datetime(df["date"])
    series = [
        (
            d.to_pydatetime() if hasattr(d, "to_pydatetime") else d,
            float(nl),
            float(nr),
        )
        for d, nl, nr in zip(dates, df["norm_left"], df["norm_right"])
    ]
    return series
