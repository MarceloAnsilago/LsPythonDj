# core/urls.py
from django.urls import path
from django.contrib.auth.decorators import login_required
from . import views

app_name = "core"

urlpatterns = [
    path("", login_required(views.home), name="home"),
    path("pares/",      login_required(views.stub_page), {"page": "Pares"},       name="pares"),
  
    path("analise/",    login_required(views.stub_page), {"page": "Análise"},     name="analise"),
    path("operacoes/",  login_required(views.stub_page), {"page": "Operações"},   name="operacoes"),
    path("encerradas/", login_required(views.stub_page), {"page": "Encerradas"},  name="encerradas"),
    path("faltantes/",  login_required(views.stub_page), {"page": "Faltantes"},   name="faltantes"),
    path("config/",     login_required(views.stub_page), {"page": "Configurações"}, name="config"),
    path("faltantes/", views.faltantes, name="faltantes"),
]