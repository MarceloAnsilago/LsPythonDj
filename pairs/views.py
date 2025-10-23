from __future__ import annotations

import json
import math
import re
import threading
import traceback
import uuid
from typing import Any

from django.contrib import messages
from django.core.cache import cache
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils.safestring import mark_safe
from django.utils.timezone import now
from django.views.decorators.http import require_GET, require_POST

from acoes.models import Asset
from longshort.services.metrics import (
    compute_pair_window_metrics,
    get_moving_beta_series,
    get_normalized_price_series,
    get_zscore_series,
)
from .constants import DEFAULT_BASE_WINDOW, DEFAULT_BETA_WINDOW, DEFAULT_WINDOWS
from .models import Pair, UserMetricsConfig
from .services.scan import build_pairs_base, hunt_pairs_until_found, scan_pair_windows

CACHE_TTL = 60 * 30  # 30 minutes


def _get_user_metrics_config(user) -> UserMetricsConfig | None:
    if getattr(user, "is_authenticated", False):
        config, _ = UserMetricsConfig.objects.get_or_create(
            user=user,
            defaults=UserMetricsConfig.default_kwargs(),
        )
        return config
    return None


def _user_windows(config: UserMetricsConfig | None) -> list[int]:
    return config.windows_list() if config else list(DEFAULT_WINDOWS)


def _user_base_window(config: UserMetricsConfig | None) -> int:
    return config.base_window if config else DEFAULT_BASE_WINDOW


def _user_beta_window(config: UserMetricsConfig | None) -> int:
    return config.beta_window if config else DEFAULT_BETA_WINDOW


# -------- Base / Grid A --------


@require_POST
def refresh_pairs_base(request: HttpRequest) -> HttpResponse:
    """
    Recalcula a base (Grid A), respeitando a configuracao do usuario quando disponivel.
    """
    config = _get_user_metrics_config(request.user)
    base_window = _user_base_window(config)

    try:
        result = build_pairs_base(
            window=base_window,
            metrics_config=config,
        )
        if not isinstance(result, dict):
            raise ValueError("build_pairs_base retornou tipo inesperado.")
        approved = len(result.get("approved_ids") or [])
        errors = len(result.get("errors") or [])
        messages.info(
            request,
            f"Base {base_window}d: {approved} pares aprovados. Erros: {errors}.",
        )
    except Exception as exc:  # pragma: no cover - defensive
        tb = traceback.format_exc()
        messages.error(
            request,
            mark_safe(
                f"Erro ao recalcular base: <code>{exc}</code>"
                f"<br><small>{tb.splitlines()[-1]}</small>"
            ),
        )
    return redirect("pairs:home")


@require_GET
def pairs_home(request: HttpRequest) -> HttpResponse:
    config = _get_user_metrics_config(request.user)
    windows = _user_windows(config)
    base_window = _user_base_window(config)

    qs = Pair.objects.all().order_by("id")
    pairs = [p for p in qs if (p.scan_cache_json or {}).get("base", {}).get("status") == "ok"]

    context = {
        "pairs": pairs,
        "BASE_WINDOW": base_window,
        "DEFAULT_WINDOWS": windows,
        "SCAN_MIN": min(windows) if windows else None,
        "SCAN_MAX": max(windows) if windows else None,
        "current": "pares",
    }
    return render(request, "pairs/pairs_home.html", context)


# -------- Scanner / Grid B --------


@require_GET
def scan_windows(request: HttpRequest, pair_id: int) -> HttpResponse:
    config = _get_user_metrics_config(request.user)
    pair = get_object_or_404(Pair, pk=pair_id)

    try:
        result = scan_pair_windows(
            pair,
            windows=_user_windows(config),
            metrics_config=config,
        )
    except Exception as exc:  # pragma: no cover - defensive
        return HttpResponse(
            f"<div class='p-3 text-danger'>Erro no scan do par #{pair_id}: {exc}</div>",
            status=500,
        )

    return render(
        request,
        "pairs/_scan_table.html",
        {
            "pair": pair,
            "rows": result["rows"],
            "best": result["best"],
            "thresholds": result.get("thresholds"),
        },
    )


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

    series = get_zscore_series(pair, window)
    labels = [d.strftime("%Y-%m-%d") for d, _ in series] if series else []
    values = [z for _, z in series] if series else []

    metrics = compute_pair_window_metrics(pair=pair, window=window)
    adf_pct = None
    if metrics.get("adf_pvalue") is not None:
        adf_pct = (1.0 - float(metrics["adf_pvalue"])) * 100.0

    context = {
        "pair": pair,
        "window": window,
        "labels_json": json.dumps(labels),
        "values_json": json.dumps(values),
        "metrics": {
            "beta": metrics.get("beta"),
            "zscore": metrics.get("zscore"),
            "half_life": metrics.get("half_life"),
            "adf_pct": adf_pct,
            "corr30": metrics.get("corr30"),
            "corr60": metrics.get("corr60"),
            "n_samples": metrics.get("n_samples"),
        },
    }
    return render(request, "pairs/_zscore_chart.html", context)


# -------- Hunt / Background status --------


@require_POST
def hunt_start(request: HttpRequest) -> HttpResponse:
    config = _get_user_metrics_config(request.user)

    job_id = uuid.uuid4().hex
    cache.set(f"hunt:{job_id}", {"state": "starting"}, CACHE_TTL)

    def progress_cb(ev: dict[str, Any]) -> None:
        cache.set(f"hunt:{job_id}", {"state": "running", **ev}, CACHE_TTL)

    def runner() -> None:
        try:
            result = hunt_pairs_until_found(
                windows_desc=None,
                source="assets",
                progress_cb=progress_cb,
                metrics_config=config,
            )
            cache.set(f"hunt:{job_id}", {"state": "done", "result": result}, CACHE_TTL)
        except Exception as exc:  # pragma: no cover - defensive
            cache.set(f"hunt:{job_id}", {"state": "error", "error": str(exc)}, CACHE_TTL)

    threading.Thread(target=runner, daemon=True).start()
    return render(request, "pairs/_hunt_status.html", {"job_id": job_id})


@require_GET
def hunt_status(request: HttpRequest, job_id: str) -> HttpResponse:
    data = cache.get(f"hunt:{job_id}") or {"state": "unknown"}
    return render(request, "pairs/_hunt_status.html", {"job_id": job_id, "data": data})


# -------- Refresh base (background) --------


@require_POST
def refresh_start(request: HttpRequest) -> HttpResponse:
    """
    Inicia o recalculo da base em background e envia status via HTMX.
    """
    config = _get_user_metrics_config(request.user)
    base_window = _user_base_window(config)

    job_id = uuid.uuid4().hex
    cache.set(f"refresh:{job_id}", {"state": "starting"}, CACHE_TTL)

    def progress_cb(event: dict[str, Any]) -> None:
        cache.set(
            f"refresh:{job_id}",
            {"state": "running", **event},
            CACHE_TTL,
        )

    def runner() -> None:
        try:
            result = build_pairs_base(
                window=base_window,
                progress_cb=progress_cb,
                metrics_config=config,
            )
            approved = len((result or {}).get("approved_ids") or [])
            errors = len((result or {}).get("errors") or [])
            cache.set(
                f"refresh:{job_id}",
                {"state": "done", "ok": approved, "errs": errors},
                CACHE_TTL,
            )
        except Exception as exc:  # pragma: no cover - defensive
            cache.set(f"refresh:{job_id}", {"state": "error", "error": str(exc)}, CACHE_TTL)

    threading.Thread(target=runner, daemon=True).start()
    return render(
        request,
        "pairs/_refresh_status.html",
        {"job_id": job_id, "BASE_WINDOW": base_window},
    )


@require_GET
def refresh_status(request: HttpRequest, job_id: str) -> HttpResponse:
    data = cache.get(f"refresh:{job_id}") or {"state": "unknown"}
    config = _get_user_metrics_config(request.user)
    base_window = _user_base_window(config)
    return render(
        request,
        "pairs/_refresh_status.html",
        {"job_id": job_id, "data": data, "BASE_WINDOW": base_window},
    )


# -------- Helpers for metrics display --------


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


def _resolve_context(request: HttpRequest, config: UserMetricsConfig | None) -> tuple[Pair, int, str]:
    """
    Retorna (pair, window, source) onde source e um dos {"pair","ad-hoc"}.
    """
    window_param = request.GET.get("window")
    window = int(window_param) if window_param else None
    base_default = _user_base_window(config)

    if "pair" in request.GET:
        pair = get_object_or_404(Pair, pk=int(request.GET["pair"]))
        if not window:
            window = pair.chosen_window or pair.base_window or base_default
        return pair, int(window), "pair"

    if "left" in request.GET and "right" in request.GET:
        left_ticker = request.GET["left"].strip().upper()
        right_ticker = request.GET["right"].strip().upper()
        left = get_object_or_404(Asset, ticker=left_ticker)
        right = get_object_or_404(Asset, ticker=right_ticker)
        try:
            pair = Pair.objects.get(left=left, right=right)
        except Pair.DoesNotExist:
            pair = Pair(left=left, right=right, base_window=base_default, chosen_window=window or base_default)
        if not window:
            window = p.chosen_window or p.base_window or 180
        return p, window, "ad-hoc"

    if "op" in request.GET:
        raise Http404("Integracao com Operacoes estara disponivel na fase 2.")

    raise Http404("Informe ?pair=<id> ou ?left=&right=.")


# -------- Analysis views --------


def analysis_entry(request: HttpRequest) -> HttpResponse:
    config = _get_user_metrics_config(request.user)
    windows = _user_windows(config)
    try:
        pair, window, source = _resolve_context(request, config)
        context = {
            "pair": pair,
            "window": window,
            "windows": windows,
            "source": source,
            "current": "analise",
        }
        return render(request, "pairs/analysis.html", context)
    except Http404:
        return render(
            request,
            "pairs/analysis_landing.html",
            {"windows": windows, "current": "analise"},
        )


def analysis_metrics(request: HttpRequest) -> HttpResponse:
    config = _get_user_metrics_config(request.user)
    pair, window, _ = _resolve_context(request, config)
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


def analysis_zseries(request: HttpRequest) -> HttpResponse:
    config = _get_user_metrics_config(request.user)
    pair, window, _ = _resolve_context(request, config)
    beta_window = _user_beta_window(config)

    metrics = compute_pair_window_metrics(pair=pair, window=window)
    metrics_display = _build_metrics_display(metrics)
    series = get_zscore_series(pair, window)
    normalized_series = get_normalized_price_series(pair=pair, window=window)
    moving_beta_series = get_moving_beta_series(pair=pair, window=window, beta_window=beta_window)

    labels: list[str] = []
    values: list[float] = []
    for dt_value, z_value in series:
        labels.append(dt_value.strftime("%Y-%m-%d") if hasattr(dt_value, "strftime") else str(dt_value))
        try:
            values.append(float(z_value))
        except (TypeError, ValueError):
            values.append(0.0)

    normalized_labels: list[str] = []
    normalized_left: list[float] = []
    normalized_right: list[float] = []
    for dt_value, left_val, right_val in normalized_series:
        normalized_labels.append(dt_value.strftime("%Y-%m-%d") if hasattr(dt_value, "strftime") else str(dt_value))
        try:
            normalized_left.append(float(left_val))
        except (TypeError, ValueError):
            normalized_left.append(0.0)
        try:
            normalized_right.append(float(right_val))
        except (TypeError, ValueError):
            normalized_right.append(0.0)

    beta_labels: list[str] = []
    beta_values: list[float] = []
    for dt_value, beta_val in moving_beta_series:
        beta_labels.append(dt_value.strftime("%Y-%m-%d") if hasattr(dt_value, "strftime") else str(dt_value))
        try:
            beta_values.append(float(beta_val))
        except (TypeError, ValueError):
            beta_values.append(0.0)

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
        "beta_labels_json": json.dumps(beta_labels),
        "beta_values_json": json.dumps(beta_values),
        "dispersion_points_json": json.dumps(dispersion_points),
        "chart_id": chart_id,
        "data_points": len(values),
        "normalized_points": len(normalized_labels),
        "beta_points": len(beta_labels),
        "dispersion_points": len(dispersion_points),
        "beta_window": beta_window,
        "generated_at": now(),
    }
    return render(request, "pairs/_analysis_panel.html", context)

