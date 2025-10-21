from __future__ import annotations

from django import forms

from .models import UserMetricsConfig


class UserMetricsConfigForm(forms.ModelForm):
    default_windows = forms.CharField(
        label="Janelas (dias)",
        help_text="Informe valores separados por virgula, ex.: 120,140,160.",
        widget=forms.TextInput(attrs={"placeholder": "80,90,100"}),
    )

    class Meta:
        model = UserMetricsConfig
        fields = [
            "base_window",
            "default_windows",
            "adf_min",
            "zscore_abs_min",
            "beta_window",
            "half_life_max",
        ]
        labels = {
            "base_window": "Janela base (Grid A)",
            "adf_min": "ADF minimo (%)",
            "zscore_abs_min": "Z-score minimo (valor absoluto)",
            "beta_window": "Janela movel do beta",
            "half_life_max": "Half-life maximo (dias)",
        }
        widgets = {
            "base_window": forms.NumberInput(attrs={"min": 10}),
            "adf_min": forms.NumberInput(attrs={"step": "0.1", "min": 0, "max": 100}),
            "zscore_abs_min": forms.NumberInput(attrs={"step": "0.1", "min": 0}),
            "beta_window": forms.NumberInput(attrs={"min": 1}),
            "half_life_max": forms.NumberInput(attrs={"step": "0.1", "min": 0}),
        }

    def clean_default_windows(self) -> str:
        raw = self.cleaned_data.get("default_windows", "")
        if not raw:
            raise forms.ValidationError("Informe ao menos uma janela.")

        values: list[int] = []
        for chunk in raw.replace(";", ",").split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                num = int(chunk)
            except ValueError as exc:
                raise forms.ValidationError("Use apenas numeros inteiros separados por virgula.") from exc
            if num <= 0:
                raise forms.ValidationError("As janelas devem ser maiores que zero.")
            if num not in values:
                values.append(num)

        if not values:
            raise forms.ValidationError("Informe ao menos uma janela.")
        return ",".join(str(v) for v in values)

    def clean_base_window(self) -> int:
        value = self.cleaned_data.get("base_window") or 0
        if value <= 0:
            raise forms.ValidationError("A janela base deve ser maior que zero.")
        return value

    def clean_adf_min(self) -> float:
        value = float(self.cleaned_data.get("adf_min") or 0)
        if not (0 < value <= 100):
            raise forms.ValidationError("O ADF minimo deve estar entre 0 e 100.")
        return value

    def clean_zscore_abs_min(self) -> float:
        value = float(self.cleaned_data.get("zscore_abs_min") or 0)
        if value <= 0:
            raise forms.ValidationError("O Z-score minimo deve ser maior que zero.")
        return value

    def clean_beta_window(self) -> int:
        value = self.cleaned_data.get("beta_window") or 0
        if value <= 0:
            raise forms.ValidationError("A janela movel deve ser maior que zero.")
        return value

    def clean_half_life_max(self) -> float:
        raw = self.cleaned_data.get("half_life_max")
        value = float(raw or 0)
        if value < 0:
            raise forms.ValidationError("O half-life deve ser zero (desativado) ou positivo.")
        return value
