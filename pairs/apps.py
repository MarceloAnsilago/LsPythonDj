from django.apps import AppConfig

class PairsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "pairs"   # caminho do app
    label = "pairs"  # rótulo do app (garante consistência)