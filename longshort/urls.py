from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path("admin/", admin.site.urls),

    path("acoes/", include("acoes.urls")),
    path("pairs/", include("pairs.urls")),
    path("cotacoes/", include("cotacoes.urls")),  # << AQUI ANTES DO CORE

    path("", include("core.urls")),               # << CORE POR ÚLTIMO
]