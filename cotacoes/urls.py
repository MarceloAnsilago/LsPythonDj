# cotacoes/urls.py
from django.urls import path
from .views import QuotesHomeView, update_quotes, quotes_pivot

app_name = "cotacoes"

urlpatterns = [
    path("",          QuotesHomeView.as_view(), name="home"),
    path("atualizar/", update_quotes,           name="update"),
    path("pivot/",     quotes_pivot,            name="pivot"),
]