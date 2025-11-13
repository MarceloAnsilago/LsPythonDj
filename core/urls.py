from django.contrib.auth.decorators import login_required
from django.urls import path

from . import views

app_name = "core"

urlpatterns = [
    path("", login_required(views.home), name="home"),
    path("data/", login_required(views.home_data), name="home_data"),
    path(
        "home/live/refresh/",
        login_required(views.refresh_live_quotes),
        name="refresh_live_quotes",
    ),
    path("pares/", login_required(views.stub_page), {"page": "Pares"}, name="pares"),
    path("analise/", login_required(views.stub_page), {"page": "Analise"}, name="analise"),
    path("operacoes/", views.operacoes, name="operacoes"),
    path(
        "operacoes/<int:pk>/encerrar/",
        login_required(views.operacao_encerrar),
        name="operacao_encerrar",
    ),
    path(
        "operacoes/<int:pk>/refresh/",
        login_required(views.operacao_refresh),
        name="operacao_refresh",
    ),
    path("encerradas/", login_required(views.stub_page), {"page": "Encerradas"}, name="encerradas"),
    path("config/", views.config, name="config"),
    path("faltantes/", views.faltantes, name="faltantes"),
]
