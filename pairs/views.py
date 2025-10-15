from __future__ import annotations
from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpRequest, HttpResponse, HttpResponseBadRequest
from django.views.decorators.http import require_GET, require_POST
from django.contrib import messages
from longshort.services.metrics import get_zscore_series, compute_pair_window_metrics

from .models import Pair
import json
from .services.scan import (
    scan_pair_windows,
    DEFAULT_WINDOWS,
    build_pairs_base,
    BASE_WINDOW,  # use o BASE_WINDOW do service
)

@require_POST
def refresh_pairs_base(request: HttpRequest) -> HttpResponse:
    result = build_pairs_base(window=BASE_WINDOW, limit_assets=40)
    ok = len(result.get("approved_ids", []))
    errs = len(result.get("errors", []))
    messages.info(request, f"Base {BASE_WINDOW}d: {ok} pares aprovados. Erros: {errs}.")
    return redirect("pairs:home")

@require_GET
def pairs_home(request: HttpRequest) -> HttpResponse:
    qs = Pair.objects.all().order_by("id")
    # mostra só quem tem cache 'base' ok (ajuste se quiser ver todos)
    pairs = [p for p in qs if (p.scan_cache_json or {}).get("base", {}).get("status") == "ok"]
    context = {
        "pairs": pairs,
        "BASE_WINDOW": BASE_WINDOW,
        "DEFAULT_WINDOWS": DEFAULT_WINDOWS,
        "current": "pares",
    }
    return render(request, "pairs/pairs_home.html", context)

@require_GET
def scan_windows(request: HttpRequest, pair_id: int) -> HttpResponse:
    pair = get_object_or_404(Pair, pk=pair_id)
    try:
        result = scan_pair_windows(pair, DEFAULT_WINDOWS)
    except Exception as e:
        return HttpResponse(f"<div class='p-3 text-danger'>Erro no scan do par #{pair_id}: {e}</div>", status=500)

    return render(request, "pairs/_scan_table.html", {
        "pair": pair,
        "rows": result["rows"],
        "best": result["best"],
    })

@require_GET
def choose_window(request: HttpRequest, pair_id: int, window: int) -> HttpResponse:
    pair = get_object_or_404(Pair, pk=pair_id)
    pair.chosen_window = window
    pair.save(update_fields=["chosen_window"])
    messages.success(request, f"Janela {window} dias definida para o par #{pair.id}.")
    return redirect("pairs:home")

@require_GET
def zscore_chart(request: HttpRequest, pair_id: int, window: int) -> HttpResponse:
    import json
    pair = get_object_or_404(Pair, pk=pair_id)

    # Série do Z-score
    series = get_zscore_series(pair, window)
    labels = [d.strftime("%Y-%m-%d") for d, _ in series] if series else []
    values = [z for _, z in series] if series else []

    # Métricas para mostrar no título
    m = compute_pair_window_metrics(pair=pair, window=window)
    adf_pct = None
    if m.get("adf_pvalue") is not None:
        adf_pct = (1.0 - float(m["adf_pvalue"])) * 100.0

    context = {
        "pair": pair,
        "window": window,
        "labels_json": json.dumps(labels),
        "values_json": json.dumps(values),
        "metrics": {
            "beta": m.get("beta"),
            "zscore": m.get("zscore"),
            "half_life": m.get("half_life"),
            "adf_pct": adf_pct,
            "corr30": m.get("corr30"),
            "corr60": m.get("corr60"),
            "n_samples": m.get("n_samples"),
        },
    }
    return render(request, "pairs/_zscore_chart.html", context)
