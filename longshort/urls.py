from django.contrib import admin
from django.urls import path, include
from django.http import JsonResponse


def healthcheck(request):
    return JsonResponse({"status": "ok"})


urlpatterns = [
    path("admin/", admin.site.urls),

    path("acoes/", include("acoes.urls")),
    path("cotacoes/", include("cotacoes.urls")),

    # 🔽 PARES ANTES DO CORE
    path("pares/", include(("pairs.urls", "pairs"), namespace="pairs")),

    # 🔽 LOGIN / CONTAS
    path("accounts/", include("accounts.urls")),  # 👈 ADICIONADO

    # ✅ HEALTH CHECK SEM LOGIN
    path("health/", healthcheck, name="healthcheck"),

    # 🔽 CORE POR ÚLTIMO (catch-all da home e demais páginas gerais)
    path("", include("core.urls")),
]
