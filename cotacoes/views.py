# cotacoes/views.py
from __future__ import annotations

import pandas as pd
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.cache import cache
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse_lazy
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST
from django.views.generic import ListView, TemplateView

from acoes.models import Asset
from .models import QuoteDaily, MissingQuoteLog
from longshort.services.quotes import bulk_update_quotes


def _build_pivot_context(max_rows: int = 90):
    qs = QuoteDaily.objects.select_related("asset").order_by("-date")
    if not qs.exists():
        return {"cols": [], "rows": []}

    df = pd.DataFrame(list(qs.values("date", "asset__ticker", "close")))
    if df.empty:
        return {"cols": [], "rows": []}

    df_pivot = (
        df.pivot(index="date", columns="asset__ticker", values="close")
          .sort_index(ascending=False)
          .round(2)
    )
    if max_rows:
        df_pivot = df_pivot.head(max_rows)

    cols = list(df_pivot.columns)
    rows = []
    for dt, row in df_pivot.iterrows():
        rows.append({
            "date": dt,
            "values": [("" if pd.isna(row[c]) else float(row[c])) for c in cols],
        })
    return {"cols": cols, "rows": rows}


class QuotesHomeView(LoginRequiredMixin, TemplateView):
    template_name = "cotacoes/quote_list.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

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
        pivot = (
            df.pivot(index="date", columns="asset__ticker", values="close")
              .sort_index(ascending=False)
              .head(60)
              .round(2)
        )
        cols = list(pivot.columns)
        rows = []
        for idx, row in pivot.iterrows():
            rows.append({
                "date": idx,
                "values": [None if pd.isna(row[c]) else float(row[c]) for c in cols],
            })
        ctx["pivot_cols"] = cols
        ctx["pivot_rows"] = rows
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
    pivot_ctx = _build_pivot_context(max_rows=None)
    return render(request, "cotacoes/quote_pivot.html", {"cols": pivot_ctx["cols"], "data": pivot_ctx["rows"]})


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
