from django.urls import path
from . import views

app_name = "pairs"

urlpatterns = [
    path("", views.PairListView.as_view(), name="list"),
    path("new/", views.PairCreateView.as_view(), name="create"),
    path("<int:pk>/edit/", views.PairUpdateView.as_view(), name="edit"),
    path("<int:pk>/delete/", views.PairDeleteView.as_view(), name="delete"),
    path("generate-from-favorites/", views.generate_from_favorites, name="generate_from_favorites"),
]
