
# Register your models here.
from django.contrib import admin
from .models import QuoteDaily, MissingQuoteLog

@admin.register(QuoteDaily)
class QuoteDailyAdmin(admin.ModelAdmin):
    list_display = ("asset", "date", "close")
    list_filter = ("asset",)
    search_fields = ("asset__ticker",)
    ordering = ("-date",)

@admin.register(MissingQuoteLog)
class MissingQuoteLogAdmin(admin.ModelAdmin):
    list_display = ("asset", "date", "reason", "resolved_bool", "created_at")
    list_filter = ("reason", "resolved_bool", "created_at")
    search_fields = ("asset__ticker", "detail")