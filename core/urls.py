from django.contrib.auth.decorators import login_required
from django.urls import path

from . import views

app_name = "core"

urlpatterns = [
    path("", login_required(views.home), name="home"),
    path("pares/", login_required(views.stub_page), {"page": "Pares"}, name="pares"),
    path("analise/", login_required(views.stub_page), {"page": "Analise"}, name="analise"),
    path("operacoes/", views.operacoes, name="operacoes"),
    path("encerradas/", login_required(views.stub_page), {"page": "Encerradas"}, name="encerradas"),
    path("config/", views.config, name="config"),
    path("faltantes/", views.faltantes, name="faltantes"),
]
