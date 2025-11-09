from django.contrib import admin

from .models import Operation, OperationMetricSnapshot


@admin.register(Operation)
class OperationAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "user",
        "sell_asset",
        "buy_asset",
        "status",
        "operation_date",
        "entry_zscore",
    )
    list_filter = ("status", "is_real", "operation_date")
    search_fields = ("sell_asset__ticker", "buy_asset__ticker", "user__username")
    autocomplete_fields = ("user", "left_asset", "right_asset", "sell_asset", "buy_asset")


@admin.register(OperationMetricSnapshot)
class OperationMetricSnapshotAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "operation",
        "snapshot_type",
        "reference_date",
        "zscore",
        "beta",
    )
    list_filter = ("snapshot_type", "reference_date")
    autocomplete_fields = ("operation",)
