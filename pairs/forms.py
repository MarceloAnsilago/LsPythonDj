from django import forms
from .models import Pair

class PairForm(forms.ModelForm):
    class Meta:
        model = Pair
        fields = ["asset_a", "asset_b", "is_active"]
        widgets = {
            "is_active": forms.CheckboxInput(attrs={"class": "form-check-input"}),
        }

    def clean(self):
        cleaned = super().clean()
        a = cleaned.get("asset_a")
        b = cleaned.get("asset_b")
        if a and b and a.id == b.id:
            raise forms.ValidationError("Escolha dois ativos distintos.")
        return cleaned
