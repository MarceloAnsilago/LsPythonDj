
# core/views.py
from django.shortcuts import render
from django.shortcuts import redirect
def home(request):
    return render(request, "core/home.html", {
        "current": "home",
        "title": "Início — Operações em andamento"
    })

def stub_page(request, page="Página"):
    return render(request, "core/stub.html", {
        "current": page.lower(),
        "title": page
    })


def faltantes(request):
    # manda para a página real de faltantes dentro do app cotacoes
    return redirect("cotacoes:faltantes_home")