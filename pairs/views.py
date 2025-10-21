from __future__ import annotations

from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpRequest, HttpResponse
from django.views.decorators.http import require_GET, require_POST
from django.contrib import messages
from django.utils.safestring import mark_safe

from django.core.cache import cache
import json
import re
import threading
import uuid
import traceback
import math
from typing import Any
CACHE_TTL = 60 * 30  # 30 min

from django.http import Http404
from django.utils.timezone import now

from longshort.services.metrics import (
    compute_pair_window_metrics,
    get_normalized_price_series,
    get_zscore_series,
)
from .models import Pair
from .services.scan import (
    scan_pair_windows,
    DEFAULT_WINDOWS,
    build_pairs_base,
    BASE_WINDOW,
    hunt_pairs_until_found,
)

from acoes.models import Asset
# from operacoes.models import Operacao  # (deixe import comentado até existir)


# -------- Base / Grid A --------

@require_POST
def refresh_pairs_base(request: HttpRequest) -> HttpResponse:
    """
    Recalcula a base (Grid A) usando BASE_WINDOW e limita o universo (se quiser).
    Blindado para não quebrar caso o service retorne algo inesperado.
    """
    try:
        result = build_pairs_base(window=BASE_WINDOW)
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
            res = hunt_pairs_until_found(source="assets", progress_cb=progress_cb)
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


DEFAULT_WINDOWS = [120, 140, 150, 160, 170, 180]

def _format_float(value: Any, decimals: int) -> str:
    if value is None:
        return "--"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return "--"


def _format_int(value: Any) -> str:
    if value is None:
        return "--"
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return "--"


def _build_metrics_display(metrics: dict[str, Any] | None) -> list[dict[str, Any]]:
    metrics = metrics or {}
    return [
        {"label": "Z-score", "value": _format_float(metrics.get("zscore"), 2), "raw": metrics.get("zscore")},
        {"label": "Beta", "value": _format_float(metrics.get("beta"), 3), "raw": metrics.get("beta")},
        {"label": "Half-life (dias)", "value": _format_float(metrics.get("half_life"), 1), "raw": metrics.get("half_life")},
        {"label": "ADF p-valor", "value": _format_float(metrics.get("adf_pvalue"), 4), "raw": metrics.get("adf_pvalue")},
        {"label": "Correlacao 30d", "value": _format_float(metrics.get("corr30"), 2), "raw": metrics.get("corr30")},
        {"label": "Correlacao 60d", "value": _format_float(metrics.get("corr60"), 2), "raw": metrics.get("corr60")},
        {"label": "Amostras", "value": _format_int(metrics.get("n_samples")), "raw": metrics.get("n_samples")},
    ]


def _resolve_context(request):
    """
    Retorna (pair, window, source) onde source ∈ {"pair","ad-hoc","op"}.
    - ?pair=<id>
    - ?left=<ticker>&right=<ticker>
    - ?op=<id> (futuro)
    """
    window = int(request.GET.get("window") or 0) or None
    if "pair" in request.GET:
        p = get_object_or_404(Pair, pk=int(request.GET["pair"]))
        if not window:
            window = p.chosen_window or p.base_window or 180
        return p, window, "pair"

    if "left" in request.GET and "right" in request.GET:
        l_t = request.GET["left"].strip().upper()
        r_t = request.GET["right"].strip().upper()
        left = get_object_or_404(Asset, ticker=l_t)
        right = get_object_or_404(Asset, ticker=r_t)
        # tenta achar Pair existente; se não houver, cria objeto em memória (não salva)
        try:
            p = Pair.objects.get(left=left, right=right)
        except Pair.DoesNotExist:
            p = Pair(left=left, right=right, base_window=180, chosen_window=window or 180)
        if not window:
            window = p.chosen_window or p.base_window or 180
        return p, window, "ad-hoc"

    if "op" in request.GET:
        # op = get_object_or_404(Operacao, pk=int(request.GET["op"]))
        # p = op.pair
        raise Http404("Integração com Operações virá na Fase 2.")

    raise Http404("Informe ?pair=<id> ou ?left=&right=.")

# pairs/views.py


def analysis_entry(request):
    try:
        pair, window, source = _resolve_context(request)
        ctx = {"pair": pair, "window": window, "windows": DEFAULT_WINDOWS,
               "source": source, "current": "analise"}
        return render(request, "pairs/analysis.html", ctx)
    except Http404:
        # sem params: abre landing com formulário
        return render(request, "pairs/analysis_landing.html",
                      {"windows": DEFAULT_WINDOWS, "current": "analise"})

def analysis_metrics(request):
    pair, window, _ = _resolve_context(request)
    metrics = compute_pair_window_metrics(pair=pair, window=window)
    metrics_display = _build_metrics_display(metrics)
    return render(
        request,
        "pairs/_analysis_metrics.html",
        {
            "metrics": metrics,
            "metrics_display": metrics_display,
            "pair": pair,
            "pair_label": f"{pair.left.ticker} x {pair.right.ticker}",
            "window": window,
        },
    )


def analysis_zseries(request):
    pair, window, _ = _resolve_context(request)
    metrics = compute_pair_window_metrics(pair=pair, window=window)
    metrics_display = _build_metrics_display(metrics)
    series = get_zscore_series(pair, window)
    normalized_series = get_normalized_price_series(pair=pair, window=window)

    labels: list[str] = []
    values: list[float] = []
    for dt_value, z_value in series:
        if hasattr(dt_value, "strftime"):
            labels.append(dt_value.strftime("%Y-%m-%d"))
        else:
            labels.append(str(dt_value))
        try:
            values.append(float(z_value))
        except (TypeError, ValueError):
            values.append(0.0)

    normalized_labels: list[str] = []
    normalized_left: list[float] = []
    normalized_right: list[float] = []
    for dt_value, left_val, right_val in normalized_series:
        if hasattr(dt_value, "strftime"):
            normalized_labels.append(dt_value.strftime("%Y-%m-%d"))
        else:
            normalized_labels.append(str(dt_value))
        try:
            normalized_left.append(float(left_val))
        except (TypeError, ValueError):
            normalized_left.append(0.0)
        try:
            normalized_right.append(float(right_val))
        except (TypeError, ValueError):
            normalized_right.append(0.0)

    dispersion_points: list[dict[str, float]] = []
    for left_val, right_val in zip(normalized_left, normalized_right):
        try:
            x_val = float(left_val)
            y_val = float(right_val)
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(x_val) and math.isfinite(y_val)):
            continue
        dispersion_points.append({"x": x_val, "y": y_val})

    pair_label = f"{pair.left.ticker} x {pair.right.ticker}"
    slug_source = str(pair.pk or f"{pair.left.ticker}-{pair.right.ticker}")
    chart_id = re.sub(r"[^a-zA-Z0-9_-]", "", f"{slug_source}-{window}") or "analysis-chart"

    context = {
        "pair": pair,
        "pair_label": pair_label,
        "left_label": pair.left.ticker,
        "right_label": pair.right.ticker,
        "window": window,
        "metrics": metrics,
        "metrics_display": metrics_display,
        "labels_json": json.dumps(labels),
        "values_json": json.dumps(values),
        "normalized_labels_json": json.dumps(normalized_labels),
        "normalized_left_json": json.dumps(normalized_left),
        "normalized_right_json": json.dumps(normalized_right),
        "dispersion_points_json": json.dumps(dispersion_points),
        "chart_id": chart_id,
        "data_points": len(values),
        "normalized_points": len(normalized_labels),
        "dispersion_points": len(dispersion_points),
        "generated_at": now(),
    }
    return render(request, "pairs/_analysis_panel.html", context)
