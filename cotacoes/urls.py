from django.urls import path
from .views import (
    QuotesHomeView, update_quotes, quotes_pivot,
    clear_logs, quotes_progress, update_quotes_ajax,
)

app_name = "cotacoes"

urlpatterns = [
    path("", QuotesHomeView.as_view(), name="home"),
    path("atualizar/", update_quotes, name="update"),  # se quiser manter o antigo
    path("ajax/atualizar/", update_quotes_ajax, name="update_ajax"),
    path("progresso/", quotes_progress, name="progress"),
    path("pivot/", quotes_pivot, name="pivot"),
    path("logs/limpar/", clear_logs, name="logs_clear"),
]