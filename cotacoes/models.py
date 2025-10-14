from __future__ import annotations
from django.conf import settings
from django.db import models

class QuoteDaily(models.Model):
    asset = models.ForeignKey("acoes.Asset", on_delete=models.CASCADE, related_name="quotes")
    date  = models.DateField()
    close = models.FloatField()

    class Meta:
        unique_together = (("asset", "date"),)
        indexes = [
            models.Index(fields=["asset", "date"]),
        ]
        ordering = ["-date"]

    def __str__(self):
        return f"{self.asset.ticker} {self.date} = {self.close}"

class MissingQuoteLog(models.Model):
    asset = models.ForeignKey("acoes.Asset", on_delete=models.CASCADE, related_name="missing_logs")
    date = models.DateField(null=True, blank=True)  # opcional (pode logar por ativo/intervalo)
    reason = models.CharField(max_length=200)       # ex.: 'yf_error', 'no_data', 'invalid_ticker'
    detail = models.TextField(blank=True, default="")
    resolved_bool = models.BooleanField(default=False)
    resolved_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"[{self.asset.ticker}] {self.reason} {self.date or ''}"
