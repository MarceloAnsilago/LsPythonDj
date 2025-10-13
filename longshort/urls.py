from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('accounts/', include('django.contrib.auth.urls')),
    path('acoes/', include('acoes.urls', namespace='acoes')),  # app de ações
    path('', include('core.urls', namespace='core')),          # core depois
]