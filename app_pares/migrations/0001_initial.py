from __future__ import annotations

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ("acoes", "0001_initial"),
        ("pairs", "0003_usermetricsconfig_half_life_max"),
    ]

    operations = [
        migrations.CreateModel(
            name="PriceHistory",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("date", models.DateField()),
                ("close", models.DecimalField(decimal_places=6, max_digits=18)),
                ("source", models.CharField(default="QuoteDaily", max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "asset",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="price_history",
                        to="acoes.asset",
                    ),
                ),
            ],
            options={
                "ordering": ["-date"],
                "unique_together": {("asset", "date")},
            },
        ),
        migrations.CreateModel(
            name="PairScanResult",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("run_date", models.DateField(db_index=True)),
                ("best_window", models.PositiveIntegerField(blank=True, null=True)),
                ("status", models.CharField(max_length=32)),
                ("message", models.CharField(blank=True, default="", max_length=200)),
                ("rows", models.JSONField(blank=True, null=True)),
                ("best_row", models.JSONField(blank=True, null=True)),
                ("thresholds", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "pair",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="scan_results",
                        to="pairs.pair",
                    ),
                ),
            ],
            options={
                "ordering": ["-run_date", "-pair_id"],
                "unique_together": {("pair", "run_date")},
            },
        ),
    ]
