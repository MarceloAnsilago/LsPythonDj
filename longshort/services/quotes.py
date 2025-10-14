from cotacoes.models import QuoteDaily, MissingQuoteLog

def fetch_stooq(ticker):
    import requests
    import pandas as pd
    import io

    ticker_stooq = f"{ticker.lower()}.sa"
    url = f"https://stooq.com/q/d/l/?s={ticker_stooq}&i=d"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and "Date,Open,High,Low,Close,Volume" in r.text:
            df = pd.read_csv(io.StringIO(r.text), parse_dates=["Date"])
            df.set_index("Date", inplace=True)
            return df
        else:
            return None
    except Exception as e:
        print(f"Erro Stooq {ticker}: {e}")
        return None

def bulk_update_quotes(assets, period="2y", interval="1d"):
    import yfinance as yf
    import pandas as pd
    from django.db.models import Max

    total_rows = 0
    assets_with_data = 0
    bulk_objs = []

    for asset in assets:
        series = None

        # 1. Tenta Stooq
        df = fetch_stooq(asset.ticker)
        if df is not None and not df.empty and "Close" in df.columns:
            series = df["Close"]
            # Converter o índice para date puro
            series.index = pd.to_datetime(series.index).date
            last = QuoteDaily.objects.filter(asset=asset).aggregate(Max("date"))["date__max"]
            if last:
                series = series[series.index > last]

        # 2. Fallback para yfinance se não conseguiu pelo Stooq
        if series is None or series.empty:
            try:
                df_yf = yf.download(
                    tickers=asset.ticker + ".SA",
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    progress=False
                )
                if not df_yf.empty:
                    # Corrige: pode vir DataFrame multi-coluna se múltiplos tickers
                    # Aqui garantimos Series só de um ativo!
                    if "Close" in df_yf.columns:
                        s = df_yf["Close"]
                        # Caso seja DataFrame multi-coluna (quando chama vários tickers)
                        if isinstance(s, pd.DataFrame):
                            # Tenta pegar pelo ticker
                            tck = asset.ticker + ".SA"
                            if tck in s.columns:
                                series = s[tck]
                            else:
                                # Pega primeira coluna
                                series = s.iloc[:, 0]
                        else:
                            series = s
                        # Index para date puro
                        series.index = pd.to_datetime(series.index).date
                        last = QuoteDaily.objects.filter(asset=asset).aggregate(Max("date"))["date__max"]
                        if last:
                            series = series[series.index > last]
            except Exception as e:
                print(f"Erro Yahoo Finance para {asset.ticker}: {e}")
                series = None

        # 3. Salva ou loga ausência
        if series is not None and not series.empty:
            asset_inserted = False
            for dt, price in series.items():
                try:
                    price_float = float(price)
                    if pd.isna(price_float):
                        continue
                    bulk_objs.append(QuoteDaily(asset=asset, date=dt, close=price_float))
                    total_rows += 1
                    asset_inserted = True
                except Exception as e:
                    print(f"[bulk_update_quotes] Erro convertendo preço para {asset.ticker} em {dt}: {price} ({e})")
                    continue
            if asset_inserted:
                assets_with_data += 1
        else:
            MissingQuoteLog.objects.create(
                asset=asset,
                reason="no_data",
                detail=f"Nenhum dado para {asset.ticker} no Stooq nem Yahoo"
            )

    # 4. Bulk insert
    if bulk_objs:
        QuoteDaily.objects.bulk_create(bulk_objs, ignore_conflicts=True, batch_size=1000)

    return assets_with_data, total_rows
