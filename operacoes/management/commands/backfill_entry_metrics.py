from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from django.core.management.base import BaseCommand
from django.utils import timezone

from longshort.services.metrics import compute_pair_window_metrics
from operacoes.models import Operation, OperationMetricSnapshot


class Command(BaseCommand):
    help = "Cria/atualiza snapshots de métricas de ENTRADA para operações existentes."

    def add_arguments(self, parser):
        parser.add_argument("--only", type=int, help="ID da operação para processar apenas um registro")
        parser.add_argument(
            "--all",
            action="store_true",
            help="Processa todas as operações (abertas e encerradas). Por padrão considera apenas abertas.",
        )

    def handle(self, *args, **options):
        only_id: int | None = options.get("only")
        process_all: bool = bool(options.get("all"))

        qs = Operation.objects.all() if process_all else Operation.objects.filter(status=Operation.STATUS_OPEN)
        if only_id:
            qs = qs.filter(pk=only_id)

        processed = 0
        created = 0
        updated = 0
        skipped = 0

        for op in qs.select_related("pair", "left_asset", "right_asset"):
            # Obtém snapshot de ENTRADA existente
            snap = (
                OperationMetricSnapshot.objects.filter(operation=op, snapshot_type=OperationMetricSnapshot.TYPE_OPEN)
                .order_by("reference_date")
                .first()
            )

            # Já tem payload útil?
            if snap and isinstance(snap.payload, dict) and snap.payload.get("n_samples"):
                skipped += 1
                continue

            # Calcula métricas para o par, usando janela da operação
            try:
                pair_ref: Any = op.pair if op.pair else SimpleNamespace(left=op.left_asset, right=op.right_asset)
                payload = compute_pair_window_metrics(pair=pair_ref, window=op.window)
            except Exception:
                payload = None

            if not isinstance(payload, dict) or not payload:
                skipped += 1
                continue

            # Cria ou atualiza snapshot
            if not snap:
                snap = OperationMetricSnapshot(
                    operation=op,
                    snapshot_type=OperationMetricSnapshot.TYPE_OPEN,
                    reference_date=(op.opened_at.date() if op.opened_at else timezone.now().date()),
                )
                created += 1
            else:
                updated += 1

            snap.apply_payload(payload)
            snap.save()

            # Atualiza campos da operação para facilitar leituras futuras
            changed_fields: list[str] = []
            if op.entry_zscore is None and snap.zscore is not None:
                op.entry_zscore = snap.zscore
                changed_fields.append("entry_zscore")
            if op.pair_metrics != payload:
                op.pair_metrics = payload
                changed_fields.append("pair_metrics")
            if changed_fields:
                changed_fields.append("updated_at")
                op.save(update_fields=changed_fields)

            processed += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Backfill concluído. Processadas: {processed}, criadas: {created}, atualizadas: {updated}, puladas: {skipped}."
            )
        )

