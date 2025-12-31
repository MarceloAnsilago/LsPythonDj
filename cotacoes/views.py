from __future__ import annotations

from datetime import date, timedelta
from urllib.parse import quote_plus
import pandas as pd

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.cache import cache
from django.http import HttpRequest, JsonResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse_lazy
from django.utils import timezone
from django.views.decorators.http import require_http_methods, require_GET, require_POST
from django.views.generic import ListView, TemplateView

from acoes.models import Asset
from .models import QuoteDaily, MissingQuoteLog

from longshort.services.quotes import (
    bulk_update_quotes,
    scan_all_assets_and_fix,
    find_missing_dates_for_asset,
    try_fetch_single_date,
    _date_to_unix,  # helper p/ montar link do Yahoo
)

@require_http_methods(["GET"])


def _build_pivot_context(request: HttpRequest, max_rows: int = 90):
    tickers_query = (request.GET.get("tickers") or "").strip()
    tickers_filter = [
        token.strip().upper()
        for token in tickers_query.split(",")
        if token.strip()
    ]

    qs = QuoteDaily.objects.select_related("asset").order_by("-date")
    if not qs.exists():
        return {"cols": [], "rows": [], "tickers_query": tickers_query}
    df = pd.DataFrame(list(qs.values("date", "asset__ticker", "close")))
    if df.empty:
        return {"cols": [], "rows": [], "tickers_query": tickers_query}
    df_pivot = (
        df.pivot(index="date", columns="asset__ticker", values="close")
          .sort_index(ascending=False)
          .round(2)
    )
    effective_max = 200 if tickers_filter else max_rows
    if effective_max:
        df_pivot = df_pivot.head(effective_max)
    cols = list(df_pivot.columns)
    if tickers_filter:
        filtered = [ticker for ticker in tickers_filter if ticker in cols]
        cols = filtered
    rows = []
    for dt, row in df_pivot.iterrows():
        rows.append(
            {
                "date": dt,
                "values": [("" if pd.isna(row[c]) else float(row[c])) for c in cols],
            }
        )
    return {"cols": cols, "rows": rows, "tickers_query": tickers_query}



class QuotesHomeView(LoginRequiredMixin, TemplateView):
    template_name = "cotacoes/quote_list.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        tickers_query = (self.request.GET.get("tickers") or "").strip()
        tickers_filter = [
            token.strip().upper()
            for token in tickers_query.split(",")
            if token.strip()
        ]

        ctx["last_quotes"] = (
            QuoteDaily.objects.select_related("asset")
            .order_by("-date")[:30]
        )
        ctx["logs"] = MissingQuoteLog.objects.order_by("-created_at")[:20]

        qs = QuoteDaily.objects.select_related("asset").order_by("date", "asset__ticker")
        df = pd.DataFrame(list(qs.values("date", "asset__ticker", "close")))
        if df.empty:
            ctx["pivot_cols"] = []
            ctx["pivot_rows"] = []
            return ctx

        df["date"] = pd.to_datetime(df["date"])
        max_rows = 200 if tickers_filter else 60
        pivot = (
            df.pivot(index="date", columns="asset__ticker", values="close")
              .sort_index(ascending=False)
              .head(max_rows)
              .round(2)
        )
        cols = list(pivot.columns)
        if tickers_filter:
            cols = [col for col in cols if col in tickers_filter]
        rows = []
        for idx, row in pivot.iterrows():
            rows.append({
                "date": idx,
                "values": [None if pd.isna(row[c]) else float(row[c]) for c in cols],
            })
        ctx["pivot_cols"] = cols
        ctx["pivot_rows"] = rows
        ctx["tickers_query"] = tickers_query
        return ctx


class QuoteDailyListView(LoginRequiredMixin, ListView):
    model = QuoteDaily
    template_name = "cotacoes/quote_table.html"
    context_object_name = "quotes"
    paginate_by = 100


@login_required
def update_quotes(request: HttpRequest):
    assets = Asset.objects.filter(is_active=True).order_by("id")
    n_assets, n_rows = bulk_update_quotes(assets, period="2y", interval="1d")
    messages.success(request, f"Cotações atualizadas: {n_assets} ativos, {n_rows} linhas inseridas.")
    return redirect(reverse_lazy("cotacoes:home"))

def quotes_pivot(request: HttpRequest):
    pivot_ctx = _build_pivot_context(request, max_rows=None)
    return render(
        request,
        "cotacoes/quote_pivot.html",
        {
            "cols": pivot_ctx["cols"],
            "data": pivot_ctx["rows"],
            "tickers_query": pivot_ctx.get("tickers_query", ""),
        },
    )



@login_required
@require_POST
def clear_logs(request: HttpRequest):
    deleted = MissingQuoteLog.objects.filter(resolved_bool=False).delete()[0]
    messages.success(request, f"Logs limpos: {deleted} removidos.")
    return redirect("cotacoes:home")


PROGRESS_KEY = "quotes_progress_user_{uid}"

def _progress_set(user_id: int, **kwargs):
    key = PROGRESS_KEY.format(uid=user_id)
    payload = {"ts": timezone.now().isoformat(), **kwargs}
    cache.set(key, payload, timeout=60*10)

def _progress_get(user_id: int):
    key = PROGRESS_KEY.format(uid=user_id)
    return cache.get(key) or {}

@require_GET
@login_required
def quotes_progress(request: HttpRequest):
    return JsonResponse(_progress_get(request.user.id))

@login_required
@require_POST
def update_quotes_ajax(request: HttpRequest):
    assets = Asset.objects.filter(is_active=True).order_by("id")

    def progress_cb(sym: str, idx: int, total: int, status: str, rows: int):
        _progress_set(request.user.id, ticker=sym, index=idx, total=total, status=status, rows=rows)

    _progress_set(request.user.id, ticker="", index=0, total=assets.count(), status="starting", rows=0)
    n_assets, n_rows = bulk_update_quotes(assets, period="2y", interval="1d", progress_cb=progress_cb)
    messages.success(request, f"Cotações atualizadas: {n_assets} ativos, {n_rows} linhas inseridas.")
    _progress_set(request.user.id, ticker="", index=n_assets, total=assets.count(), status="done", rows=n_rows)
    return JsonResponse({"ok": True, "assets": n_assets, "rows": n_rows})


@login_required
def update_live_quotes_view(request: HttpRequest):
    """
    View que atualiza os preços ao vivo (intervalo de 5 minutos via Yahoo Finance)
    e salva na tabela cotacoes_quotelive.
    """
    from longshort.services.quotes import update_live_quotes

    assets = Asset.objects.filter(is_active=True).order_by("id")
    n_updated, n_total = update_live_quotes(assets)

    messages.success(request, f"Cotações ao vivo atualizadas: {n_updated}/{n_total} ativos.")
    return redirect("cotacoes:home")



def faltantes(request):
    return redirect("cotacoes:faltantes_home")

@require_http_methods(["GET"])
def faltantes_home(request):
    """
    Mostra a página e um botão 'Escanear e corrigir'.
    Se já houver resultados em sessão (última execução), renderiza-os.
    """
    ctx = {
        "current": "faltantes",
        "results": request.session.pop("faltantes_results", None),
    }
    return render(request, "cotacoes/faltantes.html", ctx)

@require_http_methods(["POST"])
def faltantes_scan(request):
    use_stooq = bool(request.POST.get("use_stooq"))
    # exemplo limitando a janela a 18 meses (opcional):
    results = scan_all_assets_and_fix(use_stooq=use_stooq, since_months=18)

    n_fixed = sum(r["fixed"] for r in results)
    n_remaining = sum(len(r["remaining"]) for r in results)
    messages.info(request, f"Scanner concluído: {n_fixed} preenchido(s), {n_remaining} restante(s).")

    request.session["faltantes_results"] = results
    return redirect("cotacoes:faltantes_home")


from django.shortcuts import render, redirect, get_object_or_404
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from acoes.models import Asset
from longshort.services.quotes import (
    find_missing_dates_for_asset,
    try_fetch_single_date,
)



@require_http_methods(["GET"])
def faltantes_detail(request, ticker: str):
    asset = get_object_or_404(Asset, ticker=ticker.upper())
    # reescaneia só esse ativo pra pegar a lista atualizada
    missing = find_missing_dates_for_asset(asset)
    # monta linhas com link pro Yahoo e ação de tentar baixar
    google_query = quote_plus(f"{ticker.upper()} SA")
    google_url = f"https://www.google.com/search?q={google_query}"
    rows = []
    for d in missing:
        period1 = _date_to_unix(d)  # usa helper do services (ou recrie aqui)
        period2 = _date_to_unix(d + timedelta(days=1))
        yahoo_url = f"https://finance.yahoo.com/quote/{ticker.upper()}.SA/history?period1={period1}&period2={period2}"
        rows.append(
            {
                "date": d,
                "date_iso": d.isoformat(),
                "yahoo_url": yahoo_url,
                "google_url": google_url,
            }
        )
    ctx = {
        "current": "faltantes",
        "ticker": ticker.upper(),
        "rows": rows,
    }
    return render(request, "cotacoes/faltantes_detail.html", ctx)

@require_http_methods(["POST"])
def faltantes_fetch_one(request, ticker: str, dt: str):
    asset = get_object_or_404(Asset, ticker=ticker.upper())
    try:
        d = date.fromisoformat(dt)
    except Exception:
        messages.error(request, f"Data inválida: {dt}")
        return redirect("cotacoes:faltantes_detail", ticker=ticker)

    ok = try_fetch_single_date(asset, d, use_stooq=True)
    if ok:
        messages.success(request, f"{ticker} {d} inserido com sucesso.")
    else:
        messages.warning(request, f"{ticker} {d}: não há dado nas fontes.")
    return redirect("cotacoes:faltantes_detail", ticker=ticker)

@require_http_methods(["POST"])
def faltantes_insert_one(request, ticker: str):
    asset = get_object_or_404(Asset, ticker=ticker.upper())
    dt = request.POST.get("date")
    px = request.POST.get("price")
    try:
        d = date.fromisoformat(dt)
        price = float(px)
        QuoteDaily.objects.create(asset=asset, date=d, close=price)
        messages.success(request, f"Inserido manualmente: {ticker} {d} = {price:.2f}.")
    except Exception as e:
        messages.error(request, f"Falha ao inserir: {e}")
    return redirect("cotacoes:faltantes_detail", ticker=ticker)
