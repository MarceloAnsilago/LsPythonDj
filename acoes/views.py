from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.views import View
from django.urls import reverse_lazy
from django.db import transaction
from django.db.models import Q, Exists, OuterRef
from django.shortcuts import get_object_or_404, redirect
from django.http import HttpResponse
from django.template.loader import render_to_string
from django.core.exceptions import ObjectDoesNotExist

from .models import Asset, UserAsset
from .forms import AssetForm

class AssetListView(LoginRequiredMixin, ListView):
    model = Asset
    template_name = 'acoes/asset_list.html'
    paginate_by = 20
    def get_queryset(self):
        qs = Asset.objects.all().order_by('ticker')
        q = self.request.GET.get('q','').strip()
        fav_only = self.request.GET.get('fav') == '1'
        if q:
            qs = qs.filter(Q(ticker__icontains=q) | Q(name__icontains=q) | Q(ticker_yf__icontains=q))
        if fav_only:
            qs = qs.filter(favorites__user=self.request.user)
        fav_qs = UserAsset.objects.filter(user=self.request.user, asset=OuterRef('pk'))
        return qs.annotate(is_fav=Exists(fav_qs))

class AssetCreateView(CreateView):
    model = Asset
    form_class = AssetForm   # <-- use o form
    template_name = "acoes/asset_form.html"
    success_url = reverse_lazy("acoes:lista")

class AssetUpdateView(UpdateView):
    model = Asset
    form_class = AssetForm   # <-- use o form
    template_name = "acoes/asset_form.html"
    success_url = reverse_lazy("acoes:lista")

class AssetDeleteView(LoginRequiredMixin, DeleteView):
    model = Asset
    template_name = 'acoes/asset_confirm_delete.html'
    success_url = reverse_lazy('acoes:lista')

    def delete(self, request, *args, **kwargs):
        self.object = self.get_object()
        with transaction.atomic():
            self.object.quotes.all().delete()
            self.object.missing_logs.all().delete()
            try:
                self.object.live_quote.delete()
            except ObjectDoesNotExist:
                pass
            response = super().delete(request, *args, **kwargs)
        return response

class ToggleFavoriteView(LoginRequiredMixin, View):
    def post(self, request, pk):
        asset = get_object_or_404(Asset, pk=pk)
        ua, created = UserAsset.objects.get_or_create(user=request.user, asset=asset)
        is_fav = True
        if not created:
            ua.delete()
            is_fav = False
        if request.headers.get('HX-Request'):
            html = render_to_string('acoes/_fav_button.html', {'asset': asset, 'is_fav': is_fav}, request=request)
            return HttpResponse(html)
        return redirect('acoes:lista')
