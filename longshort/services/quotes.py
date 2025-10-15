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


# ============================================================
# 🔵 Helpers de fonte de dados
# ============================================================
def _yf_symbol(ticker_b3: str) -> str:
    """
    Retorna o símbolo correto para o Yahoo Finance,
    garantindo apenas um sufixo '.SA'.
    """
    t = (ticker_b3 or "").strip().upper()
    return t if t.endswith(".SA") else f"{t}.SA"


def _yf_close_series(df: Optional[pd.DataFrame]) -> Optional[pd.Series]:
    """
    Normaliza DataFrame do yfinance e devolve uma Series com 'Close' (ou 'Adj Close'):
      - Trata MultiIndex em que o 1º nível são campos ('Close','Open',...) e o 2º nível é o ticker
      - Ou achata para colunas simples e procura 'Close'/'Adj Close'
      - Converte o índice para date (datetime.date)
    """
    if df is None or not isinstance(df, pd.DataFrame) or getattr(df, "empty", True):
        return None

    if isinstance(df.columns, pd.MultiIndex):
        # Caso clássico do yfinance: 1º nível = ('Close','Open',...), 2º nível = tickers
        level0 = df.columns.get_level_values(0)
        # Prioridade: 'Close', depois 'Adj Close'
        if "Close" in set(level0):
            sub = df["Close"]
        elif "Adj Close" in set(level0):
            sub = df["Adj Close"]
        else:
            # fallback: achata pegando último nível (ticker), e depois tenta 'Close'
            flat = df.copy()
            flat.columns = flat.columns.get_level_values(-1)
            if "Close" in flat.columns:
                sub = flat["Close"]
            elif "Adj Close" in flat.columns:
                sub = flat["Adj Close"]
            else:
                return None

        # sub pode ser DataFrame (vários tickers) ou Series (um ticker só)
        if isinstance(sub, pd.DataFrame):
            # pega a primeira coluna (único ticker no seu caso)
            if sub.shape[1] == 0:
                return None
            s = sub.iloc[:, 0].dropna()
        else:
            s = sub.dropna()

        if s.empty:
            return None

        s.index = pd.to_datetime(s.index).date
        return s

    # Colunas simples (não-MultiIndex)
    cols = list(df.columns)
    col = "Close" if "Close" in cols else ("Adj Close" if "Adj Close" in cols else None)
    if col is None:
        return None

    s = df[col].dropna().copy()
    if s.empty:
        return None
    s.index = pd.to_datetime(s.index).date
    return s


def fetch_stooq_df(ticker: str) -> Optional[pd.DataFrame]:
    """
    Retorna DataFrame diário do Stooq para ticker B3 (ex: 'PETR4') ou None.
    Nota: Stooq pode ficar lento/indisponível. Timeout curto para não travar.
    """
    try:
        t = f"{ticker.lower()}.sa"
        url = f"https://stooq.com/q/d/l/?s={t}&i=d"
        r = requests.get(url, timeout=4)  # timeout curto para não travar shell/servidor
        if r.status_code == 200 and "Date,Open,High,Low,Close,Volume" in r.text:
            df = pd.read_csv(io.StringIO(r.text), parse_dates=["Date"])
            df.set_index("Date", inplace=True)
            return df
    except Exception as e:
        print(f"[stooq] erro {ticker}: {e}")
    return None


# ============================================================
# 🟢 Atualização diária (Yahoo principal, Stooq opcional)
# ============================================================
def bulk_update_quotes(
    assets: Iterable,
    period: str = "2y",
    interval: str = "1d",
    progress_cb: ProgressCB = None,
    use_stooq: bool = False,  # desligado por padrão para não travar
) -> tuple[int, int]:
    """
    Atualiza cotações por ATIVO:
      1) Yahoo Finance (principal)
      2) (Opcional) Stooq como fallback se nada inserido e use_stooq=True
      3) Só loga MissingQuote quando NENHUMA fonte trouxe dado algum
      4) Em caso de 'up_to_date' (sem novas datas), NÃO loga

    Retorna: (n_ativos_com_insercao, n_linhas_inseridas)
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
            if progress_cb:
                progress_cb("", idx, total_assets, "skip_invalid", 0)
            continue

        if progress_cb:
            progress_cb(ticker, idx, total_assets, "processing", 0)

        # última data gravada para filtrar incrementalmente
        last_dt = QuoteDaily.objects.filter(asset=asset).aggregate(Max("date"))["date__max"]

        inserted_for_asset = 0
        had_any_source_data = False

        # ---- 1) YAHOO (principal) ----
        try:
            df_yf = yf.download(
                tickers=_yf_symbol(ticker),
                period=period,
                interval=interval,
                auto_adjust=False,   # mantém compat com seu pipeline
                progress=False,
                threads=False,
                group_by="column",   # ajuda a padronizar colunas
            )
            s_close = _yf_close_series(df_yf)
            if s_close is not None:
                had_any_source_data = True
                s = s_close
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
                            # ignora erro pontual na construção do objeto
                            pass
        except Exception as e:
            print(f"[yfinance] erro {ticker}: {e}")

        # ---- 2) STQOOQ (fallback opcional) ----
        if inserted_for_asset == 0 and use_stooq:
            try:
                df_stq = fetch_stooq_df(ticker)
                if isinstance(df_stq, pd.DataFrame) and not getattr(df_stq, "empty", True):
                    had_any_source_data = True
                    s = df_stq["Close"].dropna().copy()
                    s.index = pd.to_datetime(s.index).date
                    if last_dt:
                        s = s[s.index > last_dt]
                    if not s.empty:
                        for dt, px in s.items():
                            try:
                                bulk_objs.append(QuoteDaily(asset=asset, date=dt, close=float(px)))
                                inserted_for_asset += 1
                            except Exception:
                                pass
            except Exception as e:
                print(f"[stooq] exceção {ticker}: {e}")

        # ---- 3) Contabiliza / Progresso / Logs ----
        if inserted_for_asset > 0:
            total_rows += inserted_for_asset
            assets_with_inserts += 1
            if progress_cb:
                progress_cb(ticker, idx, total_assets, "ok", inserted_for_asset)
        else:
            if had_any_source_data:
                # havia dados, mas todos já estavam gravados -> up_to_date
                if progress_cb:
                    progress_cb(ticker, idx, total_assets, "up_to_date", 0)
            else:
                # nenhuma fonte trouxe dado algum -> logar (não bloqueia)
                try:
                    MissingQuoteLog.objects.create(
                        asset=asset,
                        reason="no_data",
                        detail=f"Nenhum dado retornado pelo Yahoo/Stooq para {ticker}",
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


# ============================================================
# 🟣 Preço em "tempo real" (5m, ~15 min de delay típico no Yahoo)
# ============================================================
from cotacoes.models import QuoteLive

def fetch_latest_price(ticker: str) -> Optional[float]:
    """
    Retorna o último preço (quase em tempo real) do Yahoo Finance.
    Intervalo de 5m, atraso típico de ~15 minutos.
    """
    try:
        sym = _yf_symbol(ticker)
        df = yf.download(
            tickers=sym,
            period="1d",
            interval="5m",
            progress=False,
            threads=False,
        )
        s_close = _yf_close_series(df)
        if s_close is not None and not s_close.empty:
            return float(s_close.iloc[-1])
    except Exception as e:
        print(f"[live] erro {ticker}: {e}")
    return None


def update_live_quotes(assets: Iterable, progress_cb: ProgressCB = None) -> tuple[int, int]:
    """
    Atualiza (ou cria) cotações em tempo real (tabela QuoteLive).
    """
    assets = list(assets)
    total = len(assets)
    updated = 0

    for idx, asset in enumerate(assets, start=1):
        ticker = getattr(asset, "ticker", "").strip().upper()
        if not ticker:
            continue

        if progress_cb:
            progress_cb(ticker, idx, total, "processing_live", 0)

        px = fetch_latest_price(ticker)
        if px is None:
            if progress_cb:
                progress_cb(ticker, idx, total, "no_data", 0)
            continue

        QuoteLive.objects.update_or_create(asset=asset, defaults={"price": px})
        updated += 1

        if progress_cb:
            progress_cb(ticker, idx, total, "ok", 1)

    if progress_cb:
        progress_cb("done", total, total, "done", updated)

    return updated, total


# ============================================================
# 🧪 Utilitário opcional (teste rápido de um ativo)
# ============================================================
def update_single_asset(ticker_b3: str, period: str = "2y", interval: str = "1d") -> tuple[int, int]:
    """
    Atualiza um único ticker (string) sem precisar montar queryset.
    Útil para depuração pontual no shell.
    """
    from acoes.models import Asset
    asset = Asset.objects.filter(ticker=ticker_b3.upper()).first()
    if not asset:
        raise ValueError(f"Ativo {ticker_b3} não encontrado")

    def _p(t, i, tot, st, rows):  # progress minimalista
        print(f"[{i}/{tot}] {t} -> {st} ({rows})")

    return bulk_update_quotes([asset], period=period, interval=interval, progress_cb=_p, use_stooq=False)
