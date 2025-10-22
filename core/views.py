from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.utils import timezone
from django.utils.formats import number_format

from acoes.models import Asset
from cotacoes.models import QuoteLive
from longshort.services.metrics import (
    compute_pair_window_metrics,
    calcular_proporcao_long_short,
)
from longshort.services.quotes import fetch_latest_price
from pairs.constants import DEFAULT_BASE_WINDOW, DEFAULT_WINDOWS
from pairs.forms import UserMetricsConfigForm
from pairs.models import Pair, UserMetricsConfig


def home(request):
    return render(
        request,
        "core/home.html",
        {
            "current": "home",
            "title": "Inicio - Operacoes em andamento",
        },
    )


def stub_page(request, page: str = "Pagina"):
    return render(
        request,
        "core/stub.html",
        {
            "current": page.lower(),
            "title": page,
        },
    )


@login_required
def operacoes(request):
    config_obj, _ = UserMetricsConfig.objects.get_or_create(
        user=request.user,
        defaults=UserMetricsConfig.default_kwargs(),
    )

    window_options = config_obj.windows_list() or list(DEFAULT_WINDOWS)
    default_window = config_obj.base_window or DEFAULT_BASE_WINDOW

    def _safe_window(value: str | None) -> int:
        if not value:
            return default_window
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default_window
        return parsed if parsed > 0 else default_window

    def _normalize_ticker(value: str | None) -> str:
        return (value or "").strip().upper()

    def _parse_decimal(raw: str | None) -> Decimal:
        if raw is None:
            raise InvalidOperation
        text = str(raw).strip()
        if not text:
            raise InvalidOperation
        text = text.replace("R$", "").replace("r$", "").replace(" ", "")
        if "," in text and "." in text:
            text = text.replace(".", "")
            text = text.replace(",", ".")
        elif "," in text:
            text = text.replace(",", ".")
        return Decimal(text)

    def _format_money(value: Decimal | float | None) -> str | None:
        if value is None:
            return None
        return f"R$ {number_format(value, 2)}"

    def _format_decimal_input(value: Decimal) -> str:
        return format(value.quantize(Decimal("0.01")), "f")

    def _get_asset(ticker: str) -> Asset | None:
        if not ticker:
            return None
        return Asset.objects.select_related("live_quote").filter(ticker=_normalize_ticker(ticker)).first()

    def _format_updated(dt_value):
        if not dt_value:
            return ""
        try:
            localized = timezone.localtime(dt_value)
        except Exception:
            localized = dt_value
        return localized.strftime("%d/%m %H:%M")

    def _build_trade_info(role: str, asset: Asset | None, ticker: str) -> dict[str, str | float | bool | None]:
        ticker_norm = _normalize_ticker(ticker)
        asset_obj = asset or _get_asset(ticker_norm)

        price = None
        updated_at = None
        price_source = None

        live_quote = getattr(asset_obj, "live_quote", None) if asset_obj else None
        if live_quote:
            price = getattr(live_quote, "price", None)
            updated_at = getattr(live_quote, "updated_at", None)
            if price is not None:
                price_source = "cache"

        yahoo_price = None
        yahoo_error = False
        if ticker_norm:
            try:
                yahoo_price = fetch_latest_price(ticker_norm)
            except Exception:
                yahoo_price = None
                yahoo_error = True

        if yahoo_price is not None:
            price = yahoo_price
            updated_at = timezone.now()
            price_source = "yahoo"
            if asset_obj:
                QuoteLive.objects.update_or_create(asset=asset_obj, defaults={"price": yahoo_price})

        info = {
            "role": role,
            "label": "Venda" if role == "sell" else "Compra",
            "ticker": ticker_norm,
            "name": getattr(asset_obj, "name", ""),
            "price": price,
            "price_label": f"R$ {number_format(price, 2)}" if price is not None else None,
            "source": price_source,
            "source_label": (
                "Yahoo (agora)" if price_source == "yahoo" else "Yahoo (ultima leitura)" if price_source == "cache" else ""
            ),
            "updated_label": _format_updated(updated_at),
            "error_label": "",
            "fetched_now": price_source == "yahoo",
        }

        if price is None and ticker_norm:
            if yahoo_error:
                info["error_label"] = "Nao foi possivel contatar o Yahoo Finance agora."
            else:
                info["error_label"] = "Yahoo nao retornou cotacao para este ticker."

        return info

    initial_window = _safe_window(request.GET.get("window"))
    source = (request.GET.get("source") or "").strip().lower()
    if source not in {"analysis", "manual"}:
        source = "analysis" if request.GET.get("pair") or request.GET.get("left") else "manual"

    initial_left = ""
    initial_right = ""
    pair_obj: Pair | None = None
    pair_param = request.GET.get("pair")

    if pair_param:
        try:
            pair_id = int(pair_param)
            pair_obj = Pair.objects.select_related("left", "right", "left__live_quote", "right__live_quote").get(pk=pair_id)
            initial_left = pair_obj.left.ticker
            initial_right = pair_obj.right.ticker
            window_guess = pair_obj.chosen_window or pair_obj.base_window
            if window_guess and initial_window == default_window:
                initial_window = int(window_guess)
            source = "analysis"
        except (Pair.DoesNotExist, ValueError):
            pair_obj = None
            messages.warning(request, "Par informado na analise nao foi encontrado.")

    if not pair_obj:
        left_param = (request.GET.get("left") or "").strip().upper()
        right_param = (request.GET.get("right") or "").strip().upper()
        if left_param or right_param:
            initial_left = left_param
            initial_right = right_param
            source = "analysis" if left_param and right_param else source

    initial_left = _normalize_ticker(initial_left)
    initial_right = _normalize_ticker(initial_right)

    if initial_window not in window_options:
        window_options = sorted(set(window_options + [initial_window]))

    left_asset = pair_obj.left if pair_obj else _get_asset(initial_left)
    right_asset = pair_obj.right if pair_obj else _get_asset(initial_right)

    metrics = None
    zscore_value: float | None = None
    if pair_obj and initial_window:
        try:
            metrics = compute_pair_window_metrics(pair=pair_obj, window=initial_window)
            raw_z = metrics.get("zscore") if isinstance(metrics, dict) else None
            if raw_z is not None:
                zscore_value = float(raw_z)
        except Exception:
            metrics = None
            messages.warning(request, "Nao foi possivel calcular o Z-score deste par no momento.")

    if zscore_value is not None and zscore_value < 0:
        sell_asset = right_asset
        sell_ticker = initial_right
        buy_asset = left_asset
        buy_ticker = initial_left
        orientation = "inverted"
    else:
        sell_asset = left_asset
        sell_ticker = initial_left
        buy_asset = right_asset
        buy_ticker = initial_right
        orientation = "default"

    sell_info = _build_trade_info("sell", sell_asset, sell_ticker)
    buy_info = _build_trade_info("buy", buy_asset, buy_ticker)

    summary_note = ""
    if zscore_value is not None and sell_info["ticker"] and buy_info["ticker"]:
        summary_note = (
            f"Z-score {zscore_value:.2f} sugere vender {sell_info['ticker']} e comprar {buy_info['ticker']}."
        )
    elif not sell_info["ticker"] and not buy_info["ticker"]:
        summary_note = "Informe os tickers para montar o plano da operacao."

    summary = {
        "zscore": zscore_value,
        "zscore_label": f"{zscore_value:.2f}" if zscore_value is not None else None,
        "orientation": orientation,
        "note": summary_note,
        "sell": sell_info,
        "buy": buy_info,
        "from_analysis": source == "analysis",
    }

    def _fmt_metric(value: float | int | None, digits: int = 2, fallback: str = "--") -> str:
        if value is None:
            return fallback
        try:
            return f"{float(value):.{digits}f}"
        except (TypeError, ValueError):
            return fallback

    pair_metrics_display: list[dict[str, str]] = []
    if isinstance(metrics, dict) and metrics:
        pair_metrics_display = [
            {"label": "Z-score", "value": _fmt_metric(metrics.get("zscore"), 2)},
            {"label": "Half-life", "value": _fmt_metric(metrics.get("half_life"), 2)},
            {"label": "ADF p-valor", "value": _fmt_metric(metrics.get("adf_pvalue"), 4)},
            {"label": "Beta", "value": _fmt_metric(metrics.get("beta"), 4)},
            {"label": "Correlacao 30", "value": _fmt_metric(metrics.get("corr30"), 4)},
            {"label": "Correlacao 60", "value": _fmt_metric(metrics.get("corr60"), 4)},
            {
                "label": "Amostra",
                "value": number_format(metrics.get("n_samples"), 0) if metrics.get("n_samples") is not None else "--",
            },
        ]

    summary["pair_metrics_payload"] = metrics if isinstance(metrics, dict) else None
    summary["pair_metrics_display"] = pair_metrics_display

    capital_param = request.GET.get("valor") or request.GET.get("capital")
    valuation: dict[str, object] = {
        "input_raw": capital_param or "",
        "input_display": capital_param or "",
        "error": None,
        "has_result": False,
        "lot_size": 100,
        "suggested_value": None,
        "suggested_label": None,
        "input_adjusted": False,
        "input_adjusted_message": "",
    }

    sell_price = summary["sell"]["price"]
    buy_price = summary["buy"]["price"]

    lot_size_dec = Decimal(valuation["lot_size"])
    suggested_capital: Decimal | None = None
    sell_price_dec: Decimal | None = None
    buy_price_dec: Decimal | None = None
    if sell_price is not None and buy_price is not None:
        sell_price_dec = Decimal(str(sell_price))
        buy_price_dec = Decimal(str(buy_price))
        suggested_capital = max(sell_price_dec, buy_price_dec) * lot_size_dec
        valuation["suggested_value"] = suggested_capital
        valuation["suggested_label"] = _format_money(suggested_capital)
        if not capital_param:
            valuation["input_display"] = _format_decimal_input(suggested_capital)
    elif not capital_param:
        valuation["input_display"] = ""

    capital_informado: Decimal | None = None
    capital_utilizado: Decimal | None = None

    if capital_param:
        try:
            capital_informado = _parse_decimal(capital_param)
        except InvalidOperation:
            valuation["error"] = "Informe um valor numerico valido."
        else:
            if capital_informado <= 0:
                valuation["error"] = "Informe um valor maior que zero."
            else:
                capital_utilizado = capital_informado
    elif suggested_capital is not None:
        capital_utilizado = suggested_capital

    if (
        valuation["error"] is None
        and capital_utilizado is not None
        and suggested_capital is not None
        and capital_utilizado < suggested_capital
    ):
        valuation["input_adjusted"] = True
        valuation["input_adjusted_message"] = (
            f"Valor informado ajustado para o minimo recomendado ({valuation['suggested_label']})."
        )
        capital_utilizado = suggested_capital

    if valuation["error"] is None and capital_utilizado is not None:
        valuation["input_display"] = _format_decimal_input(capital_utilizado)

    if (
        valuation["error"] is None
        and capital_utilizado is not None
        and (sell_price_dec is None or buy_price_dec is None)
    ):
        valuation["error"] = "Cotacoes indisponiveis para calcular os lotes."

    result = None
    if (
        valuation["error"] is None
        and capital_utilizado is not None
        and sell_price_dec is not None
        and buy_price_dec is not None
    ):
        result = calcular_proporcao_long_short(
            preco_short=float(sell_price_dec),
            preco_long=float(buy_price_dec),
            limite_venda=float(capital_utilizado),
            lote=int(valuation["lot_size"]),
            ticker_short=sell_info["ticker"],
            ticker_long=buy_info["ticker"],
            nome_short=sell_info["name"],
            nome_long=buy_info["name"],
            capital_informado=float(capital_informado) if capital_informado is not None else None,
        )
        if result is None:
            valuation["error"] = "Valor insuficiente para um lote de 100 acoes na ponta vendida."
        else:
            payload = result.to_payload()
            valuation.update(
                {
                    "has_result": True,
                    "result": result,
                    "lots": result.lotes_vendidos,
                    "shares": result.quantidade_vendida,
                    "shares_buy": result.quantidade_comprada,
                    "capital_value": result.capital_utilizado,
                    "capital_label": _format_money(result.capital_utilizado),
                    "capital_informado_label": _format_money(result.capital_informado)
                    if result.capital_informado is not None
                    else None,
                    "lot_notional_label": _format_money(result.preco_short * Decimal(result.lote)),
                    "sell_amount": result.valor_vendido,
                    "sell_label": _format_money(result.valor_vendido),
                    "buy_amount": result.valor_comprado,
                    "buy_label": _format_money(result.valor_comprado),
                    "net_amount": result.saldo,
                    "net_label": _format_money(abs(result.saldo)),
                    "net_direction": "recebe" if result.saldo >= 0 else "paga",
                    "minimum_label": _format_money(result.valor_minimo_para_operar),
                    "proporcao_label": f"{result.proporcao:.4f}",
                    "description": result.resumo,
                    "result_payload": payload,
                }
            )
            valuation["input_display"] = _format_decimal_input(result.capital_utilizado)
            if (
                result.capital_informado is not None
                and result.capital_informado != result.capital_utilizado
            ):
                valuation["input_adjusted"] = True
                valuation["input_adjusted_message"] = (
                    f"Valor informado { _format_money(result.capital_informado) } "
                    f"ajustado para { _format_money(result.capital_utilizado) }."
                )
            summary["trade_plan_description"] = result.resumo
            summary["trade_plan_metrics"] = payload
            summary["trade_plan"] = result

    context = {
        "current": "operacoes",
        "title": "Operacoes",
        "window_options": window_options,
        "initial": {
            "left": initial_left,
            "right": initial_right,
            "window": initial_window,
            "pair_id": pair_obj.pk if pair_obj else (pair_param or ""),
            "source": source,
        },
        "pair_obj": pair_obj,
        "prefilled": bool(initial_left and initial_right),
        "summary": summary,
        "valuation": valuation,
    }
    return render(request, "core/operacoes.html", context)


def faltantes(request):
    return redirect("cotacoes:faltantes_home")


@login_required
def config(request):
    config_obj, _ = UserMetricsConfig.objects.get_or_create(
        user=request.user,
        defaults=UserMetricsConfig.default_kwargs(),
    )

    if request.method == "POST":
        form = UserMetricsConfigForm(request.POST, instance=config_obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Configuracoes atualizadas com sucesso.")
            return redirect("core:config")
    else:
        form = UserMetricsConfigForm(instance=config_obj)

    sample_windows = ", ".join(str(w) for w in config_obj.windows_list())
    metrics_help = [
        {
            "title": "Janela base (Grid A)",
            "icon": "bi-bullseye",
            "description": (
                "Quantidade de pregoes usados no calculo inicial. "
                "Aprova ou reprova pares antes de aparecerem no scanner."
            ),
        },
        {
            "title": "Janelas do scanner",
            "icon": "bi-sliders",
            "description": (
                "Lista de janelas (dias) testadas no Grid B. "
                "Use diferentes horizontes para encontrar pares em ritmos variados."
            ),
        },
        {
            "title": "ADF minimo (%)",
            "icon": "bi-graph-up-arrow",
            "description": (
                "Filtro baseado no teste Augmented Dickey-Fuller. "
                "Representa (1 - p-valor). Valores maiores indicam serie mais estacionaria."
            ),
        },
        {
            "title": "Z-score minimo",
            "icon": "bi-activity",
            "description": (
                "Controla a magnitude minima do Z-score do spread padronizado. "
                "Evita oportunidades com desvio pequeno demais."
            ),
        },
        {
            "title": "Janela do beta movel",
            "icon": "bi-arrow-repeat",
            "description": (
                "Define quantos pregoes entram em cada bloco do grafico de beta movel. "
                "Janelas curtas deixam o beta mais sensivel."
            ),
        },
        {
            "title": "Half-life maximo",
            "icon": "bi-hourglass-split",
            "description": (
                "Limite superior para o tempo de meia vida do spread. "
                "Pares com half-life maior que o valor informado sao descartados."
            ),
        },
        {
            "title": "Beta",
            "icon": "bi-diagram-3",
            "description": (
                "Coeficiente da regressao log(left) x log(right). "
                "Mostra o quanto o ativo esquerdo deveria variar quando o direito move 1%."
            ),
        },
    ]

    return render(
        request,
        "core/config.html",
        {
            "form": form,
            "current": "config",
            "title": "Configuracoes de metricas",
            "sample_windows": sample_windows,
            "metrics_help": metrics_help,
        },
    )

