# acoes/forms.py
from django import forms
from .models import Asset

class AssetForm(forms.ModelForm):
    class Meta:
        model = Asset
        fields = ["ticker", "ticker_yf", "name", "is_active"]  # <-- sem logo_prefix

    def clean_ticker(self):
        return (self.cleaned_data.get("ticker") or "").upper().strip()

    def clean_ticker_yf(self):
        ticker = (self.cleaned_data.get("ticker") or "").upper().strip()
        yf = (self.cleaned_data.get("ticker_yf") or ticker).upper().strip()
        if yf and "." not in yf:
            yf += ".SA"               # autopreenche .SA
        return yf