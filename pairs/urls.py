from django.urls import path
from . import views

app_name = "pairs"

urlpatterns = [
    path("", views.pairs_home, name="home"),
    path("refresh/", views.refresh_pairs_base, name="refresh"),
    path("scan/<int:pair_id>/", views.scan_windows, name="scan_windows"),  # ← ESTE NOME
    path("choose/<int:pair_id>/<int:window>/", views.choose_window, name="choose"),
    path("zscore/<int:pair_id>/<int:window>/", views.zscore_chart, name="zscore"),
    path("hunt/start/", views.hunt_start, name="hunt_start"),
    path("hunt/status/<str:job_id>/", views.hunt_status, name="hunt_status"),
    path("refresh/start/", views.refresh_start, name="refresh_start"),
    path("refresh/status/<str:job_id>/", views.refresh_status, name="refresh_status"),
]