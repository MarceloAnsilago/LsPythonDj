from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from django.conf import settings
from django.db import models
from django.utils import timezone

from acoes.models import Asset
from pairs.models import Pair


class Operation(models.Model):
    """
    Representa uma operacao (boleta) de long & short iniciada pelo usuario.
    Mantemos os ativos originais (left/right) e as pontas efetivamente vendida/comprada,
    pois a recomendacao pode inverter dependendo do Z-score.
    """

    STATUS_OPEN = "open"
    STATUS_CLOSED = "closed"
    STATUS_CHOICES: Sequence[tuple[str, str]] = (
        (STATUS_OPEN, "Aberta"),
        (STATUS_CLOSED, "Encerrada"),
    )

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="operations",
    )
    pair = models.ForeignKey(
        Pair,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="operations",
    )
    left_asset = models.ForeignKey(
        Asset,
        on_delete=models.CASCADE,
        related_name="operations_left",
    )
    right_asset = models.ForeignKey(
        Asset,
        on_delete=models.CASCADE,
        related_name="operations_right",
    )
    sell_asset = models.ForeignKey(
        Asset,
        on_delete=models.CASCADE,
        related_name="operations_sell",
    )
    buy_asset = models.ForeignKey(
        Asset,
        on_delete=models.CASCADE,
        related_name="operations_buy",
    )

    window = models.PositiveIntegerField(default=220)
    orientation = models.CharField(max_length=16, default="default")
    source = models.CharField(max_length=32, default="manual")

    sell_quantity = models.PositiveIntegerField()
    buy_quantity = models.PositiveIntegerField(default=0)
    lot_size = models.PositiveIntegerField(default=100)
    lot_multiplier = models.PositiveIntegerField(default=1)

    sell_price = models.DecimalField(max_digits=12, decimal_places=6)
    buy_price = models.DecimalField(max_digits=12, decimal_places=6)
    sell_value = models.DecimalField(max_digits=18, decimal_places=2)
    buy_value = models.DecimalField(max_digits=18, decimal_places=2)
    net_value = models.DecimalField(max_digits=18, decimal_places=2)
    capital_allocated = models.DecimalField(max_digits=18, decimal_places=2)

    entry_zscore = models.FloatField(null=True, blank=True)
    trade_plan = models.JSONField(null=True, blank=True)
    pair_metrics = models.JSONField(null=True, blank=True)

    is_real = models.BooleanField(default=False)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_OPEN)
    operation_date = models.DateField(default=timezone.now)
    opened_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-opened_at"]
        indexes = [
            models.Index(fields=["user", "status"]),
            models.Index(fields=["status", "operation_date"]),
        ]

    def __str__(self) -> str:
        return f"{self.sell_asset.ticker} x {self.buy_asset.ticker} ({self.get_status_display()})"

    @dataclass(frozen=True)
    class MetricSummary:
        zscore: float | None
        half_life: float | None
        adf_pvalue: float | None
        beta: float | None
        corr30: float | None
        corr60: float | None
        n_samples: int | None

        @classmethod
        def from_mapping(cls, data: Mapping[str, Any] | None) -> "Operation.MetricSummary":
            if not data:
                return cls(None, None, None, None, None, None, None)
            return cls(
                float(data["zscore"]) if data.get("zscore") is not None else None,
                float(data["half_life"]) if data.get("half_life") is not None else None,
                float(data["adf_pvalue"]) if data.get("adf_pvalue") is not None else None,
                float(data["beta"]) if data.get("beta") is not None else None,
                float(data["corr30"]) if data.get("corr30") is not None else None,
                float(data["corr60"]) if data.get("corr60") is not None else None,
                int(data["n_samples"]) if data.get("n_samples") is not None else None,
            )

    def current_lot_multiplier(self) -> int:
        if self.lot_size <= 0:
            return 0
        return max(1, self.sell_quantity // self.lot_size)

    def formatted_pair(self) -> str:
        return f"{self.sell_asset.ticker} / {self.buy_asset.ticker}"

    def as_trade_dict(self) -> dict[str, Any]:
        return {
            "sell": {
                "asset": self.sell_asset.ticker,
                "quantity": self.sell_quantity,
                "price": float(self.sell_price),
                "value": float(self.sell_value),
            },
            "buy": {
                "asset": self.buy_asset.ticker,
                "quantity": self.buy_quantity,
                "price": float(self.buy_price),
                "value": float(self.buy_value),
            },
            "net": float(self.net_value),
            "capital_allocated": float(self.capital_allocated),
            "window": self.window,
            "orientation": self.orientation,
        }

    def update_entry_zscore(self) -> None:
        metrics = self.metric_snapshots.filter(snapshot_type=OperationMetricSnapshot.TYPE_OPEN).order_by("reference_date").first()
        if metrics:
            self.entry_zscore = metrics.zscore
            self.save(update_fields=["entry_zscore", "updated_at"])


class OperationMetricSnapshot(models.Model):
    """
    Snapshot de metricas (z-score, beta, etc.) associado a uma operacao.
    Usado para guardar o estado no dia da entrada e, futuramente, historicos.
    """

    TYPE_OPEN = "open"
    TYPE_CURRENT = "current"
    TYPE_CHOICES: Sequence[tuple[str, str]] = (
        (TYPE_OPEN, "Entrada"),
        (TYPE_CURRENT, "Atual"),
    )

    operation = models.ForeignKey(
        Operation,
        on_delete=models.CASCADE,
        related_name="metric_snapshots",
    )
    snapshot_type = models.CharField(max_length=16, choices=TYPE_CHOICES, default=TYPE_OPEN)
    reference_date = models.DateField(default=timezone.now)

    zscore = models.FloatField(null=True, blank=True)
    half_life = models.FloatField(null=True, blank=True)
    adf_pvalue = models.FloatField(null=True, blank=True)
    beta = models.FloatField(null=True, blank=True)
    corr30 = models.FloatField(null=True, blank=True)
    corr60 = models.FloatField(null=True, blank=True)
    n_samples = models.IntegerField(null=True, blank=True)

    payload = models.JSONField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["operation", "snapshot_type"]),
            models.Index(fields=["snapshot_type", "reference_date"]),
        ]

    def __str__(self) -> str:
        return f"{self.operation_id} {self.snapshot_type} ({self.reference_date})"

    def apply_payload(self, data: Mapping[str, Any] | None) -> None:
        """
        Preenche campos númericos com base no dict recebido.
        """
        if not data:
            self.payload = {}
            return
        self.payload = dict(data)
        self.zscore = _safe_float(data.get("zscore"))
        self.half_life = _safe_float(data.get("half_life"))
        self.adf_pvalue = _safe_float(data.get("adf_pvalue"))
        self.beta = _safe_float(data.get("beta"))
        self.corr30 = _safe_float(data.get("corr30"))
        self.corr60 = _safe_float(data.get("corr60"))
        self.n_samples = _safe_int(data.get("n_samples"))


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
