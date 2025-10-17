from __future__ import annotations

from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpRequest, HttpResponse
from django.views.decorators.http import require_GET, require_POST
from django.contrib import messages
from django.utils.safestring import mark_safe

from django.core.cache import cache
import threading
import uuid
import traceback
from typing import Any

from longshort.services.metrics import get_zscore_series, compute_pair_window_metrics
from .models import Pair
from .services.scan import (
    scan_pair_windows,
    DEFAULT_WINDOWS,
    build_pairs_base,
    BASE_WINDOW,
    hunt_pairs_until_found,
)

CACHE_TTL = 60 * 30  # 30 min


# -------- Base / Grid A --------

@require_POST
def refresh_pairs_base(request: HttpRequest) -> HttpResponse:
    """
    Recalcula a base (Grid A) usando BASE_WINDOW e limita o universo (se quiser).
    Blindado para não quebrar caso o service retorne algo inesperado.
    """
    try:
        result = build_pairs_base(window=BASE_WINDOW, limit_assets=40)
        if not isinstance(result, dict):
            raise ValueError("build_pairs_base retornou tipo inesperado (None?).")
        ok = len(result.get("approved_ids") or [])
        errs = len(result.get("errors") or [])
        messages.info(request, f"Base {BASE_WINDOW}d: {ok} pares aprovados. Erros: {errs}.")
    except Exception as e:
        tb = traceback.format_exc()
        messages.error(
            request,
            mark_safe(f"Erro ao recalcular base: <code>{e}</code><br><small>{tb.splitlines()[-1]}</small>")
        )
    return redirect("pairs:home")


@require_GET
def pairs_home(request: HttpRequest) -> HttpResponse:
    qs = Pair.objects.all().order_by("id")
    # Mostra só quem tem cache 'base' ok; ajuste se quiser ver todos
    pairs = [p for p in qs if (p.scan_cache_json or {}).get("base", {}).get("status") == "ok"]
    context = {
        "pairs": pairs,
        "BASE_WINDOW": BASE_WINDOW,
        "DEFAULT_WINDOWS": DEFAULT_WINDOWS,
        "SCAN_MIN": min(DEFAULT_WINDOWS) if DEFAULT_WINDOWS else None,
        "SCAN_MAX": max(DEFAULT_WINDOWS) if DEFAULT_WINDOWS else None,
        "current": "pares",
    }
    return render(request, "pairs/pairs_home.html", context)


# -------- Scanner / Grid B --------

@require_GET
def scan_windows(request: HttpRequest, pair_id: int) -> HttpResponse:
    pair = get_object_or_404(Pair, pk=pair_id)
    try:
        result = scan_pair_windows(pair, DEFAULT_WINDOWS)
    except Exception as e:
        return HttpResponse(
            f"<div class='p-3 text-danger'>Erro no scan do par #{pair_id}: {e}</div>",
            status=500,
        )

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
        "labels_json": __import__("json").dumps(labels),
        "values_json": __import__("json").dumps(values),
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


# -------- Caça (status ao vivo) --------

@require_POST
def hunt_start(request: HttpRequest) -> HttpResponse:
    job_id = uuid.uuid4().hex
    cache.set(f"hunt:{job_id}", {"state": "starting"}, CACHE_TTL)

    def progress_cb(ev: dict[str, Any]) -> None:
        cache.set(f"hunt:{job_id}", {"state": "running", **ev}, CACHE_TTL)

    def runner():
        try:
            res = hunt_pairs_until_found(source="assets", limit_assets=40, progress_cb=progress_cb)
            cache.set(f"hunt:{job_id}", {"state": "done", "result": res}, CACHE_TTL)
        except Exception as e:
            cache.set(f"hunt:{job_id}", {"state": "error", "error": str(e)}, CACHE_TTL)

    threading.Thread(target=runner, daemon=True).start()
    return render(request, "pairs/_hunt_status.html", {"job_id": job_id})



@require_GET
def hunt_status(request: HttpRequest, job_id: str) -> HttpResponse:
    data = cache.get(f"hunt:{job_id}") or {"state": "unknown"}
    return render(request, "pairs/_hunt_status.html", {"job_id": job_id, "data": data})


# pairs/views.py (adicione depois das views da Caça)

@require_POST
def refresh_start(request: HttpRequest) -> HttpResponse:
    """
    Inicia recálculo da base em thread e devolve snippet com polling HTMX,
    com progresso (i/total).
    """
    job_id = uuid.uuid4().hex
    cache.set(f"refresh:{job_id}", {"state": "starting"}, CACHE_TTL)

    def progress_cb(ev: dict[str, Any]) -> None:
        # esperado: {"phase":"iter","i":int,"total":int,"left":str,"right":str,"window":int}
        cache.set(
            f"refresh:{job_id}",
            {"state": "running", **ev},
            CACHE_TTL,
        )

    def runner():
        try:
            res = build_pairs_base(
                window=BASE_WINDOW,
                limit_assets=40,
                progress_cb=progress_cb,
            )
            ok = len((res or {}).get("approved_ids") or [])
            errs = len((res or {}).get("errors") or [])
            cache.set(f"refresh:{job_id}", {"state": "done", "ok": ok, "errs": errs}, CACHE_TTL)
        except Exception as e:
            cache.set(f"refresh:{job_id}", {"state": "error", "error": str(e)}, CACHE_TTL)

    threading.Thread(target=runner, daemon=True).start()
    return render(request, "pairs/_refresh_status.html", {"job_id": job_id})


@require_GET
def refresh_status(request: HttpRequest, job_id: str) -> HttpResponse:
    data = cache.get(f"refresh:{job_id}") or {"state": "unknown"}
    return render(request, "pairs/_refresh_status.html", {"job_id": job_id, "data": data})
