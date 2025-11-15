from __future__ import annotations

from datetime import datetime, date

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from app_pares.services import rodar_scan_diario


class Command(BaseCommand):
    help = "Atualiza cotações e roda o scan diario de pares."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--data",
            dest="data",
            help="Data no formato YYYY-MM-DD. Padrão: dia atual.",
        )

    def handle(self, *args, **options) -> None:
        raw_date = options.get("data")
        target_date: date
        if raw_date:
            try:
                target_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
            except ValueError as exc:
                raise CommandError(f"Formato inválido de data: {exc}") from exc
        else:
            target_date = timezone.localdate()

        self.stdout.write(f"Rodando scan diario para {target_date}")
        summary = rodar_scan_diario(target_date)
        self.stdout.write(f"cotações: ativos={summary['quotes']['assets']} atualizados={summary['quotes']['updated_assets']}")
        self.stdout.write(f"price history: registros={summary['history']['records']} criados={summary['history']['created']} atualizados={summary['history']['updated']}")
        pairs = summary["pairs"]
        self.stdout.write(
            f"pares: processados={pairs['pairs_processed']} criados={pairs['created']} atualizados={pairs['updated']} erros={len(pairs['errors'])}"
        )
        if pairs["errors"]:
            for error in pairs["errors"]:
                self.stdout.write(f"- {error}")
