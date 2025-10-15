from django.urls import path
from . import views

app_name = "pairs"

urlpatterns = [
    path("", views.pairs_home, name="home"),
    path("refresh/", views.refresh_pairs_base, name="refresh"),
    path("scan/<int:pair_id>/", views.scan_windows, name="scan_windows"),  # ← ESTE NOME
    path("choose/<int:pair_id>/<int:window>/", views.choose_window, name="choose"),
    path("zscore/<int:pair_id>/<int:window>/", views.zscore_chart, name="zscore"),
]