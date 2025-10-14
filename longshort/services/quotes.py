# longshort/services/quotes.py
from __future__ import annotations

import io
from typing import Iterable, Optional, Callable

import pandas as pd
import requests
import yfinance as yf
from django.db.models import Max

from cotacoes.models import QuoteDaily, MissingQuoteLog

# -----------------------
# Progresso (callback)
# -----------------------
# assinatura: (ticker, idx1, total, status, rows_inserted)
ProgressCB = Optional[Callable[[str, int, int, str, int], None]]


def fetch_stooq_df(ticker: str) -> Optional[pd.DataFrame]:
    """
    Retorna DataFrame diário do Stooq para ticker B3 (ex: 'PETR4') ou None.
    """
    try:
        t = f"{ticker.lower()}.sa"
        url = f"https://stooq.com/q/d/l/?s={t}&i=d"
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and "Date,Open,High,Low,Close,Volume" in r.text:
            df = pd.read_csv(io.StringIO(r.text), parse_dates=["Date"])
            df.set_index("Date", inplace=True)
            return df
    except Exception as e:
        print(f"[stooq] erro {ticker}: {e}")
    return None


def _yf_symbol(ticker_b3: str) -> str:
    # permite customização futura, hoje só acrescenta .SA
    return f"{ticker_b3}.SA"


def bulk_update_quotes(
    assets: Iterable,
    period: str = "2y",
    interval: str = "1d",
    progress_cb: ProgressCB = None,
) -> tuple[int, int]:
    """
    Atualiza cotações por ATIVO, com fallback:
      1) Stooq -> insere apenas datas > última gravada
      2) Se nada inserido, tenta Yahoo Finance
      3) Só loga MissingQuote quando NENHUMA fonte trouxe dado algum
      4) Em caso de 'up_to_date' (sem novas datas), NÃO loga
    """
    assets = list(assets)
    total_assets = len(assets)

    if progress_cb:
        progress_cb("start", 0, total_assets, "starting", 0)

    bulk_objs: list[QuoteDaily] = []
    total_rows = 0
    assets_with_inserts = 0

    for idx, asset in enumerate(assets, start=1):
        ticker = getattr(asset, "ticker", "").strip().upper()
        if not ticker:
            # pular ativos inválidos
            if progress_cb:
                progress_cb("", idx, total_assets, "skip_invalid", 0)
            continue

        if progress_cb:
            progress_cb(ticker, idx, total_assets, "processing", 0)

        # última data gravada
        last_dt = QuoteDaily.objects.filter(asset=asset).aggregate(Max("date"))["date__max"]

        inserted_for_asset = 0
        had_any_source_data = False  # alguma fonte retornou dataframe não vazio?

        # ---- 1) STQOOQ ----
        try:
            df_stq = fetch_stooq_df(ticker)
            if df_stq is not None and not df_stq.empty:
                had_any_source_data = True
                s = df_stq["Close"].copy()
                s.index = pd.to_datetime(s.index).date
                if last_dt:
                    s = s[s.index > last_dt]
                if not s.empty:
                    for dt, px in s.items():
                        if pd.isna(px):
                            continue
                        try:
                            bulk_objs.append(QuoteDaily(asset=asset, date=dt, close=float(px)))
                            inserted_for_asset += 1
                        except Exception:
                            # ignora falha individual
                            pass
        except Exception as e:
            print(f"[stooq] exceção {ticker}: {e}")

        # ---- 2) YAHOO (apenas se nada inserido ainda) ----
        if inserted_for_asset == 0:
            try:
                df_yf = yf.download(
                    tickers=_yf_symbol(ticker),
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                )
                if isinstance(df_yf, pd.DataFrame) and not df_yf.empty and "Close" in df_yf.columns:
                    had_any_source_data = True
                    s = df_yf["Close"].copy()
                    s.index = pd.to_datetime(s.index).date
                    if last_dt:
                        s = s[s.index > last_dt]
                    if not s.empty:
                        for dt, px in s.items():
                            if pd.isna(px):
                                continue
                            try:
                                bulk_objs.append(QuoteDaily(asset=asset, date=dt, close=float(px)))
                                inserted_for_asset += 1
                            except Exception:
                                pass
            except Exception as e:
                print(f"[yfinance] erro {ticker}: {e}")

        # ---- 3) Contabiliza / Progresso / Logs ----
        if inserted_for_asset > 0:
            total_rows += inserted_for_asset
            assets_with_inserts += 1
            if progress_cb:
                progress_cb(ticker, idx, total_assets, "ok", inserted_for_asset)
        else:
            # sem inserção
            if had_any_source_data:
                # havia dados, mas todos já estavam gravados -> up_to_date
                if progress_cb:
                    progress_cb(ticker, idx, total_assets, "up_to_date", 0)
            else:
                # nenhuma fonte trouxe dado algum -> logar
                try:
                    MissingQuoteLog.objects.create(
                        asset=asset,
                        reason="no_data",
                        detail=f"Nenhum dado retornado pelo Stooq nem Yahoo para {ticker}",
                    )
                except Exception:
                    pass
                if progress_cb:
                    progress_cb(ticker, idx, total_assets, "no_data", 0)

    # ---- 4) Persistência em lote ----
    if bulk_objs:
        QuoteDaily.objects.bulk_create(bulk_objs, ignore_conflicts=True, batch_size=1000)

    if progress_cb:
        progress_cb("done", total_assets, total_assets, "done", total_rows)

    return assets_with_inserts, total_rows
