from django.shortcuts import render

# Create your views here.from __future__ import annotations
from itertools import combinations
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db import IntegrityError, transaction
from django.shortcuts import redirect, render, get_object_or_404
from django.urls import reverse, reverse_lazy
from django.views.generic import ListView, CreateView, UpdateView, DeleteView

from .models import Pair
from .forms import PairForm
from acoes.models import Asset, UserAsset  # já existem desde a SP3

class PairListView(LoginRequiredMixin, ListView):
    model = Pair
    template_name = "pairs/pair_list.html"
    context_object_name = "pairs"

    def get_queryset(self):
        return Pair.objects.filter(owner=self.request.user).select_related("asset_a", "asset_b")

class PairCreateView(LoginRequiredMixin, CreateView):
    model = Pair
    form_class = PairForm
    template_name = "pairs/pair_form.html"
    success_url = reverse_lazy("pairs:list")

    def form_valid(self, form):
        form.instance.owner = self.request.user
        try:
            return super().form_valid(form)
        except IntegrityError:
            form.add_error(None, "Par já existe para este usuário.")
            return self.form_invalid(form)

class PairUpdateView(LoginRequiredMixin, UpdateView):
    model = Pair
    form_class = PairForm
    template_name = "pairs/pair_form.html"
    success_url = reverse_lazy("pairs:list")

    def get_queryset(self):
        return Pair.objects.filter(owner=self.request.user)

class PairDeleteView(LoginRequiredMixin, DeleteView):
    model = Pair
    template_name = "pairs/pair_confirm_delete.html"
    success_url = reverse_lazy("pairs:list")

    def get_queryset(self):
        return Pair.objects.filter(owner=self.request.user)

@login_required
def generate_from_favorites(request):
    """
    Gera todos os pares nC2 com base nos favoritos (UserAsset) do usuário.
    Respeita A<B, ignora existentes, e conta quantos foram criados.
    """
    fav_assets = Asset.objects.filter(userasset__user=request.user).distinct().order_by("id")
    created = 0
    with transaction.atomic():
        for a, b in combinations(fav_assets, 2):
            # garantir A<B por id
            aa, bb = (a, b) if a.id < b.id else (b, a)
            try:
                Pair.objects.create(owner=request.user, asset_a=aa, asset_b=bb)
                created += 1
            except IntegrityError:
                pass  # já existe
    if created:
        messages.success(request, f"{created} par(es) criado(s) a partir dos favoritos.")
    else:
        messages.info(request, "Nenhum novo par criado (talvez já existam).")
    return redirect("pairs:list")
