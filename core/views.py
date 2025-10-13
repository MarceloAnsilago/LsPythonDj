
# core/views.py
from django.shortcuts import render

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