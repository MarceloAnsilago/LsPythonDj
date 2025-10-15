from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path("admin/", admin.site.urls),

    path("acoes/", include("acoes.urls")),
    path("cotacoes/", include("cotacoes.urls")),

    # 🔽 PARES ANTES DO CORE
    path("pares/", include(("pairs.urls", "pairs"), namespace="pairs")),


    # 🔽 CORE POR ÚLTIMO (catch-all da home e demais páginas gerais)
    path("", include("core.urls")),
]
