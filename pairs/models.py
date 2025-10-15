from django.db import models
from acoes.models import Asset

class Pair(models.Model):
    left = models.ForeignKey(Asset, on_delete=models.CASCADE, related_name="pairs_left")
    right = models.ForeignKey(Asset, on_delete=models.CASCADE, related_name="pairs_right")
    base_window = models.IntegerField(default=220)
    chosen_window = models.IntegerField(null=True, blank=True)
    scan_cache_json = models.JSONField(null=True, blank=True)
    scan_cached_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.left.ticker} — {self.right.ticker}"