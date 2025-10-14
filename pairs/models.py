# pairs/models.py
from __future__ import annotations

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.apps import apps

User = settings.AUTH_USER_MODEL

class Pair(models.Model):
    owner = models.ForeignKey(User, on_delete=models.CASCADE, related_name="pairs")
    asset_a = models.ForeignKey("acoes.Asset", on_delete=models.CASCADE, related_name="as_a")
    asset_b = models.ForeignKey("acoes.Asset", on_delete=models.CASCADE, related_name="as_b")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["owner", "asset_a", "asset_b"],
                name="uniq_owner_pair_a_b",
            ),
        ]
        ordering = ["-created_at"]

    def clean(self):
        if self.asset_a_id == self.asset_b_id:
            raise ValidationError("asset_a e asset_b não podem ser o mesmo ativo.")

    def save(self, *args, **kwargs):
        # Canonizar A<B por id
        if self.asset_a_id and self.asset_b_id and self.asset_a_id > self.asset_b_id:
            self.asset_a_id, self.asset_b_id = self.asset_b_id, self.asset_a_id
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.asset_a.ticker} × {self.asset_b.ticker} ({self.owner})"

    def data_missing_info(self, min_days: int = 1) -> tuple[bool, str]:
        """Retorna (faltando, motivo). Se o app/tabla de cotações não existir, considera faltando."""
        try:
            QuoteDaily = apps.get_model("cotacoes", "QuoteDaily")
        except LookupError:
            return True, "App cotacoes/QuoteDaily inexistente"

        if QuoteDaily is None:
            return True, "App cotacoes/QuoteDaily indisponível"

        a_ok = QuoteDaily.objects.filter(asset=self.asset_a).count() >= min_days
        b_ok = QuoteDaily.objects.filter(asset=self.asset_b).count() >= min_days
        if not a_ok and not b_ok:
            return True, "Sem cotações para A e B"
        if not a_ok:
            return True, f"Sem cotações para {self.asset_a.ticker}"
        if not b_ok:
            return True, f"Sem cotações para {self.asset_b.ticker}"
        return False, ""

    @property
    def is_data_missing(self) -> bool:
        return self.data_missing_info()[0]
