from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import math
from itertools import combinations

from django.utils import timezone

from pairs.models import Pair
from acoes.models import Asset

# Esperado: compute_pair_window_metrics(pair=<Pair>, window=<int>) -> dict
# Retorno mínimo:
# { "adf_pvalue": float, "beta": float, "zscore": float, "half_life": float,
#   "corr30": float, "corr60": float, "n_samples": int }
try:
    from longshort.services.metrics import compute_pair_window_metrics
except Exception:
    compute_pair_window_metrics = None  # será tratado com "erro"

# Thresholds (defaults)
ADF_MIN = 95.0                 # p <= 0.05
HALF_LIFE_MIN = 3
HALF_LIFE_MAX = 30
CORR_MIN = 0.5

# Janelas padrão
DEFAULT_WINDOWS = [160, 170, 180, 190, 200, 210, 220]
BASE_WINDOW = 220


@dataclass
class WindowRow:
    window: int
    adf_pct: Optional[float]
    adf_pvalue: Optional[float]
    beta: Optional[float]
    zscore: Optional[float]
    half_life: Optional[float]
    corr30: Optional[float]
    corr60: Optional[float]
    status: str
    message: str


def _qualifica(row: WindowRow) -> bool:
    if row.adf_pct is None or row.half_life is None or row.corr30 is None or row.corr60 is None:
        return False
    if row.adf_pct < ADF_MIN:
        return False
    if not (HALF_LIFE_MIN <= row.half_life <= HALF_LIFE_MAX):
        return False
    if min(row.corr30, row.corr60) < CORR_MIN:
        return False
    return True


def _tie_break(best: WindowRow | None, candidate: WindowRow | None) -> WindowRow | None:
    if candidate is None:
        return best
    if best is None:
        return candidate
    # maior ADF% → menor half-life → menor |beta|
    if (candidate.adf_pct or -1) != (best.adf_pct or -1):
        return candidate if (candidate.adf_pct or -1) > (best.adf_pct or -1) else best
    if (candidate.half_life or math.inf) != (best.half_life or math.inf):
        return candidate if (candidate.half_life or math.inf) < (best.half_life or math.inf) else best
    cb = abs(candidate.beta or 0)
    bb = abs(best.beta or 0)
    return candidate if cb < bb else best


def scan_pair_windows(pair: Pair, windows: List[int] = DEFAULT_WINDOWS) -> Dict[str, Any]:
    """Scanner por janelas para um Pair específico (Grid B)."""
    rows: List[WindowRow] = []
    best: Optional[WindowRow] = None

    for w in windows:
        status = "pendente"
        message = ""
        adf_pct = adf_pvalue = beta = zscore = half_life = corr30 = corr60 = None

        try:
            if compute_pair_window_metrics is None:
                raise RuntimeError("Função compute_pair_window_metrics não encontrada.")

            m = compute_pair_window_metrics(pair=pair, window=w)
            adf_pvalue = m.get("adf_pvalue")
            adf_pct = (1.0 - adf_pvalue) * 100.0 if (adf_pvalue is not None) else None
            beta = m.get("beta")
            zscore = m.get("zscore")
            half_life = m.get("half_life")
            corr30 = m.get("corr30")
            corr60 = m.get("corr60")

            if m.get("n_samples") is not None and m["n_samples"] < 60:
                status, message = "reprovado", "Amostra insuficiente (N<60)"
            elif adf_pct is None:
                status, message = "reprovado", "ADF% indisponível"
            elif adf_pct < ADF_MIN:
                status, message = "reprovado", "Sem evidência (ADF < 95%)"
            elif half_life is None or not (HALF_LIFE_MIN <= half_life <= HALF_LIFE_MAX):
                status, message = "reprovado", "Meia-vida fora da faixa"
            elif (corr30 is None) or (corr60 is None) or min(corr30, corr60) < CORR_MIN:
                status, message = "reprovado", "Correlação baixa (min corr30/60 < 0.5)"
            else:
                status, message = "ok", "OK"
        except Exception as e:
            status, message = "erro", f"erro: {e}"

        row = WindowRow(
            window=w, adf_pct=adf_pct, adf_pvalue=adf_pvalue, beta=beta,
            zscore=zscore, half_life=half_life, corr30=corr30, corr60=corr60,
            status=status, message=message,
        )
        rows.append(row)
        if status == "ok":
            best = _tie_break(best, row)

    # cache simples
    cache_rows = [
        {
            "window": r.window, "adf_pct": r.adf_pct, "adf_pvalue": r.adf_pvalue,
            "beta": r.beta, "zscore": r.zscore, "half_life": r.half_life,
            "corr30": r.corr30, "corr60": r.corr60, "status": r.status, "message": r.message
        } for r in rows
    ]

    sc = pair.scan_cache_json or {}
    sc["scan"] = {"rows": cache_rows, "best_window": best.window if best else None}
    pair.scan_cache_json = sc
    pair.scan_cached_at = timezone.now()
    pair.save(update_fields=["scan_cache_json", "scan_cached_at"])

    return {"rows": rows, "best": best}


# ----------------------------
# Scanner da janela-base (Grid A)
# ----------------------------

@dataclass
class BaseResult:
    pair_id: int
    left_ticker: str
    right_ticker: str
    adf_pct: Optional[float]
    half_life: Optional[float]
    corr30: Optional[float]
    corr60: Optional[float]
    beta: Optional[float]
    status: str  # "ok" | "reprovado" | "erro"
    message: str


def _compute_base_for_pair(pair: Pair, window: int = BASE_WINDOW) -> tuple[bool, Dict[str, Any], str]:
    """Calcula métricas base e decide aprovação."""
    if compute_pair_window_metrics is None:
        return False, {}, "Função compute_pair_window_metrics não encontrada."

    try:
        m = compute_pair_window_metrics(pair=pair, window=window)
        adf_pvalue = m.get("adf_pvalue")
        adf_pct = (1.0 - adf_pvalue) * 100.0 if (adf_pvalue is not None) else None
        beta = m.get("beta")
        zscore = m.get("zscore")
        half_life = m.get("half_life")
        corr30 = m.get("corr30")
        corr60 = m.get("corr60")
        n = m.get("n_samples")

        if n is not None and n < 60:
            return False, {}, "Amostra insuficiente (N<60)"
        if adf_pct is None:
            return False, {}, "ADF% indisponível"
        if adf_pct < ADF_MIN:
            return False, {}, "Sem evidência (ADF < 95%)"
        if half_life is None or not (HALF_LIFE_MIN <= half_life <= HALF_LIFE_MAX):
            return False, {}, "Meia-vida fora da faixa"
        if (corr30 is None) or (corr60 is None) or min(corr30, corr60) < CORR_MIN:
            return False, {}, "Correlação baixa (min corr30/60 < 0.5)"

        base = {
            "window": window,
            "adf_pvalue": adf_pvalue,
            "adf_pct": adf_pct,
            "beta": beta,
            "zscore": zscore,
            "half_life": half_life,
            "corr30": corr30,
            "corr60": corr60,
            "n_samples": n,
            "status": "ok",
        }
        return True, base, "OK"
    except Exception as e:
        return False, {}, f"erro: {e}"


def build_pairs_base(window: int = BASE_WINDOW,
                     limit_assets: int | None = None) -> Dict[str, Any]:
    """
    Varre Asset, testa combinações 2-a-2 na janela-base e **só persiste**
    os pares aprovados pelos thresholds.
    """
    # Universo de ativos
    try:
        qs = Asset.objects.filter(is_active=True)
        if not qs.exists():
            qs = Asset.objects.all()
    except Exception:
        qs = Asset.objects.all()

    if limit_assets:
        qs = qs.order_by("id")[:limit_assets]

    assets = list(qs)
    created = 0
    updated = 0
    approved_ids: List[int] = []
    errors: List[str] = []

    for left, right in combinations(assets, 2):
        pair = Pair.objects.filter(left=left, right=right).first()
        was_created = False
        just_created = False

        try:
            if pair is None:
                # cria temporariamente (se a métrica espera um Pair real)
                pair = Pair(left=left, right=right)
                pair.save()
                was_created = True
                just_created = True

            approved, base, msg = _compute_base_for_pair(pair, window=window)

            if approved:
                sc = pair.scan_cache_json or {}
                sc["base"] = base
                pair.scan_cache_json = sc
                pair.scan_cached_at = timezone.now()
                pair.save(update_fields=["scan_cache_json", "scan_cached_at"])

                approved_ids.append(pair.id)
                if was_created:
                    created += 1
                else:
                    updated += 1
            else:
                if was_created and just_created:
                    # não deixar rastro de par reprovado recém-criado
                    pair.delete()
                else:
                    sc = pair.scan_cache_json or {}
                    sc["base"] = {"window": window, "status": "reprovado", "message": msg}
                    pair.scan_cache_json = sc
                    pair.scan_cached_at = timezone.now()
                    pair.save(update_fields=["scan_cache_json", "scan_cached_at"])

        except Exception as e:
            if was_created and just_created and getattr(pair, "pk", None):
                try:
                    pair.delete()
                except Exception:
                    pass
            errors.append(f"Pair {getattr(left,'ticker','?')}-{getattr(right,'ticker','?')}: {e}")

    return {"created": created, "updated": updated, "approved_ids": approved_ids, "errors": errors}


def hunt_pairs_until_found(
    windows_desc: List[int] | None = None,
    *,
    source: str = "assets",        # "assets" (todas combinações) ou "existing_pairs"
    limit_assets: int | None = None
) -> Dict[str, Any]:
    """
    Diminui a janela (ex.: [220,210,200,...,120]) e, para cada janela, tenta aprovar pares.
    - source="assets": usa build_pairs_base(window=...), varrendo combinações de Asset.
    - source="existing_pairs": apenas tenta os Pair já existentes (sem criar novos).
    Para ao encontrar pelo menos 1 aprovado. Retorna o resumo da janela vencedora.

    return:
      {
        "found": bool,
        "window": int | None,
        "approved_ids": [int],
        "errors": [str],
        "scanned_windows": [int]
      }
    """
    if windows_desc is None:
        windows_desc = [220, 210, 200, 190, 180, 170, 160, 150, 140, 130, 120]

    scanned = []
    all_errors: List[str] = []

    if source == "assets":
        # usa o pipeline já pronto
        for w in windows_desc:
            scanned.append(w)
            res = build_pairs_base(window=w, limit_assets=limit_assets)
            if res.get("errors"):
                all_errors.extend(res["errors"])
            approved = res.get("approved_ids", [])
            if approved:
                return {
                    "found": True, "window": w,
                    "approved_ids": approved,
                    "errors": all_errors,
                    "scanned_windows": scanned
                }
        return {"found": False, "window": None, "approved_ids": [], "errors": all_errors, "scanned_windows": scanned}

    elif source == "existing_pairs":
        # tenta somente pares já existentes, sem criar novos
        qs = Pair.objects.all().order_by("id")
        ids_aprovados: List[int] = []
        for w in windows_desc:
            scanned.append(w)
            ids_aprovados.clear()
            for pair in qs:
                try:
                    ok, base, msg = _compute_base_for_pair(pair, window=w)
                    sc = pair.scan_cache_json or {}
                    if ok:
                        sc["base"] = base
                        pair.scan_cache_json = sc
                        pair.scan_cached_at = timezone.now()
                        pair.save(update_fields=["scan_cache_json", "scan_cached_at"])
                        ids_aprovados.append(pair.id)
                    else:
                        # marca reprovação informativa (não remove o par)
                        sc["base"] = {"window": w, "status": "reprovado", "message": msg}
                        pair.scan_cache_json = sc
                        pair.scan_cached_at = timezone.now()
                        pair.save(update_fields=["scan_cache_json", "scan_cached_at"])
                except Exception as e:
                    all_errors.append(f"Pair {pair.id}: {e}")
            if ids_aprovados:
                return {
                    "found": True, "window": w,
                    "approved_ids": list(ids_aprovados),
                    "errors": all_errors,
                    "scanned_windows": scanned
                }
        return {"found": False, "window": None, "approved_ids": [], "errors": all_errors, "scanned_windows": scanned}

    else:
        return {"found": False, "window": None, "approved_ids": [], "errors": [f"source inválido: {source}"], "scanned_windows": scanned}
