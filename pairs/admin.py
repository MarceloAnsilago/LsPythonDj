
# Register your models here.
from django.contrib import admin
from .models import Pair

@admin.register(Pair)
class PairAdmin(admin.ModelAdmin):
    list_display = ("owner", "pair_label", "is_active", "created_at", "faltando")
    list_filter = ("is_active", "created_at")
    search_fields = ("owner__username", "asset_a__ticker", "asset_b__ticker")

    def pair_label(self, obj: Pair):
        return f"{obj.asset_a.ticker} × {obj.asset_b.ticker}"

    def faltando(self, obj: Pair):
        missing, _ = obj.data_missing_info()
        return "⚠ faltando" if missing else "OK"
