# cotacoes/views.py
from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.shortcuts import redirect, render
from django.urls import reverse_lazy
from django.views.generic import TemplateView, ListView

from acoes.models import Asset
from .models import QuoteDaily, MissingQuoteLog
from longshort.services.quotes import bulk_update_quotes
from .models import MissingQuoteLog
import pandas as pd

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
from django.shortcuts import redirect

from django.http import JsonResponse, HttpRequest
from django.core.cache import cache
from django.views.decorators.http import require_GET, require_POST
from django.utils import timezone


def _build_pivot_context(max_rows: int = 90):
    """
    Monta o dataframe pivoteado (linhas = datas, colunas = tickers) e
    transforma em estruturas simples pro template.
    """
    qs = QuoteDaily.objects.select_related("asset").order_by("-date")
    if not qs.exists():
        return {"cols": [], "rows": []}

    df = pd.DataFrame(list(qs.values("date", "asset__ticker", "close")))
    if df.empty:
        return {"cols": [], "rows": []}

    # pivot: datas desc, colunas por ticker
    df_pivot = (
        df.pivot(index="date", columns="asset__ticker", values="close")
          .sort_index(ascending=False)
          .round(2)
    )

    # limita o número de linhas mostradas na Home (pra não pesar)
    if max_rows:
        df_pivot = df_pivot.head(max_rows)

    cols = list(df_pivot.columns)  # ordem alfabética por padrão do pandas
    rows = []
    for dt, row in df_pivot.iterrows():
        rows.append({
            "date": dt,  # datetime.date
            "values": [("" if pd.isna(row[c]) else float(row[c])) for c in cols]
        })

    return {"cols": cols, "rows": rows}


class QuotesHomeView(LoginRequiredMixin, TemplateView):
    template_name = "cotacoes/quote_list.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        # últimos registros (lista simples)
        ctx["last_quotes"] = (
            QuoteDaily.objects.select_related("asset")
            .order_by("-date")[:30]
        )

        # logs
        ctx["logs"] = MissingQuoteLog.objects.order_by("-created_at")[:20]

        # --------- PIVOT PARA A HOME ----------
        qs = QuoteDaily.objects.select_related("asset").order_by("date", "asset__ticker")
        df = pd.DataFrame(list(qs.values("date", "asset__ticker", "close")))
        if df.empty:
            ctx["pivot_cols"] = []
            ctx["pivot_rows"] = []
            return ctx

        df["date"] = pd.to_datetime(df["date"])
        pivot = (
            df.pivot(index="date", columns="asset__ticker", values="close")
              .sort_index(ascending=False)
        )

        # limite opcional (ex.: últimos 60 dias/linhas)
        pivot = pivot.head(60)

        # prepara para template
        pivot = pivot.round(2)
        cols = list(pivot.columns)
        rows = []
        for idx, row in pivot.iterrows():
            rows.append({
                "date": idx,  # DateTime -> o template formata
                "values": [None if pd.isna(row[c]) else float(row[c]) for c in cols]
            })

        ctx["pivot_cols"] = cols
        ctx["pivot_rows"] = rows
        # --------------------------------------

        return ctx

class QuoteDailyListView(LoginRequiredMixin, ListView):
    model = QuoteDaily
    template_name = "cotacoes/quote_table.html"
    context_object_name = "quotes"
    paginate_by = 100

def update_quotes(request):
    assets = Asset.objects.filter(is_active=True).order_by("id")
    n_assets, n_rows = bulk_update_quotes(assets, period="2y", interval="1d")
    messages.success(
        request,
        f"Cotações atualizadas: {n_assets} ativos, {n_rows} linhas inseridas."
    )
    return redirect(reverse_lazy("cotacoes:home"))


# (opcional) página dedicada com o pivot em tela cheia
def quotes_pivot(request):
    pivot_ctx = _build_pivot_context(max_rows=None)  # sem limite aqui
    return render(
        request,
        "cotacoes/quote_pivot.html",
        {"cols": pivot_ctx["cols"], "data": pivot_ctx["rows"]}
    )

@login_required
@require_POST
def clear_logs(request):
    # apaga apenas logs pendentes; troque para .all() se quiser tudo
    deleted = MissingQuoteLog.objects.filter(resolved_bool=False).delete()[0]
    messages.success(request, f"Logs limpos: {deleted} removidos.")
    return redirect("cotacoes:home")


PROGRESS_KEY = "quotes_progress_user_{uid}"

def _progress_set(user_id: int, **kwargs):
    key = PROGRESS_KEY.format(uid=user_id)
    payload = {"ts": timezone.now().isoformat(), **kwargs}
    cache.set(key, payload, timeout=60*10)  # 10 min

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
        _progress_set(
            request.user.id,
            ticker=sym,
            index=idx,
            total=total,
            status=status,
            rows=rows,
        )

    # sinaliza início
    _progress_set(request.user.id, ticker="", index=0, total=assets.count(),
                  status="starting", rows=0)

    n_assets, n_rows = bulk_update_quotes(
        assets, period="2y", interval="1d", progress_cb=progress_cb
    )
    messages.success(
        request, f"Cotações atualizadas: {n_assets} ativos, {n_rows} linhas inseridas."
    )
    # conclusão
    _progress_set(request.user.id, ticker="", index=n_assets, total=assets.count(),
                  status="done", rows=n_rows)
    return JsonResponse({"ok": True, "assets": n_assets, "rows": n_rows})
