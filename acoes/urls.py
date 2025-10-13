from django.urls import path
from .views import (
    AssetListView, AssetCreateView, AssetUpdateView, AssetDeleteView, ToggleFavoriteView
)
app_name = 'acoes'
urlpatterns = [
    path('', AssetListView.as_view(), name='lista'),
    path('novo/', AssetCreateView.as_view(), name='novo'),
    path('<int:pk>/editar/', AssetUpdateView.as_view(), name='editar'),
    path('<int:pk>/excluir/', AssetDeleteView.as_view(), name='excluir'),
    path('<int:pk>/favoritar/', ToggleFavoriteView.as_view(), name='favoritar'),
]