# cotacoes/views.py
from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.shortcuts import redirect, render
from django.urls import reverse_lazy
from django.views.generic import TemplateView

from acoes.models import Asset
from .models import QuoteDaily, MissingQuoteLog
from longshort.services.quotes import bulk_update_quotes

import pandas as pd


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

        # Logs continuam na lateral
        ctx["logs"] = MissingQuoteLog.objects.order_by("-created_at")[:20]

        # 👉 agora a Home usa a tabela dinâmica
        pivot_ctx = _build_pivot_context(max_rows=90)   # ajuste se quiser mais/menos linhas
        ctx["pivot_cols"] = pivot_ctx["cols"]
        ctx["pivot_rows"] = pivot_ctx["rows"]

        return ctx


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
