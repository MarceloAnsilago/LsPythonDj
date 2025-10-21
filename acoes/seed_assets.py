"""Utilities to sync the Asset table with a curated list."""
from __future__ import annotations

import argparse
import os
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Iterable, Sequence, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

if "DJANGO_SETTINGS_MODULE" not in os.environ:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "longshort.settings")

import django
from django.apps import apps

if not apps.ready:
    django.setup()

from django.db import transaction

from acoes.models import Asset
from cotacoes.models import MissingQuoteLog, QuoteDaily, QuoteLive


ASSETS: list[Tuple[str, str]] = [
    ("ABEV3", "AMBEV S/A"),
    ("ALOS3", "ALLOS"),
    ("ANIM3", "ANIMA"),
    ("ASAI3", "ASSAI"),
    ("AURE3", "AUREN"),
    ("AZZA3", "AZZAS 2154"),
    ("B3SA3", "B3"),
    ("BBAS3", "BRASIL"),
    ("BBSE3", "BBSEGURIDADE"),
    ("BBDC4", "BRADESCO"),
    ("BEEF3", "MINERVA"),
    ("BPAC11", "BTGP BANCO"),
    ("BRAP4", "BRADESPAR"),
    ("BRAV3", "BRAVA"),
    ("BRKM5", "BRASKEM"),
    ("CEAB3", "CEA MODAS"),
    ("CMIG4", "CEMIG"),
    ("CMIN3", "CSNMINERACAO"),
    ("COGN3", "COGNA ON"),
    ("CPFE3", "CPFL ENERGIA"),
    ("CPLE3", "COPEL"),
    ("CSAN3", "COSAN"),
    ("CSMG3", "COPASA"),
    ("CSNA3", "SID NACIONAL"),
    ("CURY3", "CURY S/A"),
    ("CXSE3", "CAIXA SEGURI"),
    ("CYRE3", "CYRELA REALT"),
    ("CVCB3", "CVC BRASIL"),
    ("DIRR3", "DIRECIONAL"),
    ("ECOR3", "ECORODOVIAS"),
    ("EGIE3", "ENGIE BRASIL"),
    ("ELET3", "ELETROBRAS"),
    ("EMBR3", "EMBRAER"),
    ("ENEV3", "ENEVA"),
    ("ENGI11", "ENERGISA"),
    ("EQTL3", "EQUATORIAL"),
    ("EZTC3", "EZTEC"),
    ("FLRY3", "FLEURY"),
    ("GGBR4", "GERDAU"),
    ("GGPS3", "GPS"),
    ("GMAT3", "GRUPO MATEUS"),
    ("GOAU4", "GERDAU MET"),
    ("HAPV3", "HAPVIDA"),
    ("HYPE3", "HYPERA"),
    ("IGTI11", "IGUATEMI S.A"),
    ("INTB3", "INTELBRAS"),
    ("IRBR3", "IRBBRASIL RE"),
    ("ISAE4", "ISA ENERGIA"),
    ("ITSA4", "ITAUSA"),
    ("ITUB4", "ITAUUNIBANCO"),
    ("KLBN11", "KLABIN S/A"),
    ("LREN3", "LOJAS RENNER"),
    ("LWSA3", "LWSA"),
    ("MGLU3", "MAGAZ LUIZA"),
    ("MBRF3", "MARFRIG"),
    ("MOVI3", "MOVIDA"),
    ("MOTV3", "MOTIVA SA"),
    ("MRVE3", "MRV"),
    ("MULT3", "MULTIPLAN"),
    ("NATU3", "NATURA"),
    ("PCAR3", "P.ACUCAR-CBD"),
    ("PETR4", "PETROBRAS"),
    ("PETZ3", "PETZ"),
    ("POMO4", "MARCOPOLO"),
    ("PRIO3", "PETRORIO"),
    ("PSSA3", "PORTO SEGURO"),
    ("RADL3", "RAIADROGASIL"),
    ("RAIL3", "RUMO S.A."),
    ("RAIZ4", "RAIZEN"),
    ("RAPT4", "RANDON PART"),
    ("RDOR3", "REDE D OR"),
    ("RECV3", "PETRORECSA"),
    ("RENT3", "LOCALIZA"),
    ("SANB11", "SANTANDER BR"),
    ("SAPR11", "SANEPAR"),
    ("SBSP3", "SABESP"),
    ("SLCE3", "SLC AGRICOLA"),
    ("SMFT3", "SMART FIT"),
    ("SMTO3", "SAO MARTINHO"),
    ("SRNA3", "SERENA"),
    ("SUZB3", "SUZANO S.A."),
    ("TAEE11", "TAESA"),
    ("TEND3", "TENDA"),
    ("TIMS3", "TIM"),
    ("TOTS3", "TOTVS"),
    ("UGPA3", "ULTRAPAR"),
    ("USIM5", "USIMINAS"),
    ("VALE3", "VALE"),
    ("VAMO3", "VAMOS"),
    ("VBBR3", "VIBRA"),
    ("VIVA3", "VIVARA S.A."),
    ("VIVT3", "TELEF BRASIL"),
    ("WEGE3", "WEG"),
    ("YDUQ3", "YDUQS PART"),
]


def _normalise_assets(assets: Iterable[Tuple[str, str]]) -> OrderedDict[str, str]:
    cleaned: OrderedDict[str, str] = OrderedDict()
    for raw_ticker, raw_name in assets:
        ticker = (raw_ticker or "").strip().upper()
        name = (raw_name or "").strip()
        if not ticker:
            continue
        cleaned[ticker] = name or ticker
    return cleaned


def _purge_related(asset_ids: Sequence[int]) -> dict[str, int]:
    if not asset_ids:
        return {"quotes": 0, "live": 0, "logs": 0}
    quotes = QuoteDaily.objects.filter(asset_id__in=asset_ids).delete()[0]
    live = QuoteLive.objects.filter(asset_id__in=asset_ids).delete()[0]
    logs = MissingQuoteLog.objects.filter(asset_id__in=asset_ids).delete()[0]
    return {"quotes": quotes, "live": live, "logs": logs}


@transaction.atomic
def run(
    assets: Iterable[Tuple[str, str]] = ASSETS,
    *,
    destructive: bool = True,
    update_names: bool = True,
    reactivate: bool = True,
    purge_quotes: bool = True,
    deactivate_removed: bool = True,
    dry_run: bool = False,
) -> None:
    """
    Sync the Asset table so it mirrors the provided list.

    destructive=True removes assets that are no longer listed.
    purge_quotes=True clears QuoteDaily/QuoteLive/MissingQuoteLog for removed assets.
    deactivate_removed=True marks removed assets as inactive when destructive=False.
    dry_run=True prints the intended actions without touching the database.
    """

    desired = _normalise_assets(assets)
    existing = {asset.ticker: asset for asset in Asset.objects.all()}

    to_create = [(ticker, desired[ticker]) for ticker in desired if ticker not in existing]
    to_keep = {ticker: existing[ticker] for ticker in desired if ticker in existing}
    to_delete_qs = Asset.objects.exclude(ticker__in=desired.keys())
    removed_ids = list(to_delete_qs.values_list("id", flat=True))

    if dry_run:
        print(f"[seed-assets] create={len(to_create)} keep={len(to_keep)} remove={len(removed_ids)}")
        if purge_quotes and removed_ids:
            print(f"[seed-assets] would purge quotes/logs for {len(removed_ids)} assets")
        return

    inserted = 0
    updated = 0
    reactivated = 0

    for ticker, name in to_create:
        Asset.objects.create(ticker=ticker, name=name, is_active=True)
        inserted += 1

    for ticker, asset in to_keep.items():
        desired_name = desired[ticker]
        has_changed = False
        if update_names and asset.name != desired_name:
            asset.name = desired_name
            has_changed = True
        if reactivate and not asset.is_active:
            asset.is_active = True
            has_changed = True
            reactivated += 1
        if has_changed:
            asset.save()
            updated += 1

    purged = {"quotes": 0, "live": 0, "logs": 0}
    if purge_quotes and removed_ids:
        purged = _purge_related(removed_ids)

    deleted_assets = 0
    deactivated_assets = 0
    if removed_ids:
        if destructive:
            deleted_assets = to_delete_qs.delete()[0]
        elif deactivate_removed:
            deactivated_assets = to_delete_qs.update(is_active=False)

    print(
        "[seed-assets] "
        f"created={inserted} updated={updated} reactivated={reactivated} "
        f"deleted={deleted_assets} deactivated={deactivated_assets} "
        f"purged_quotes={purged['quotes']} purged_live={purged['live']} purged_logs={purged['logs']}"
    )


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synchronize Asset table with curated list.")
    parser.add_argument("--dry-run", action="store_true", help="Only show what would change.")
    parser.add_argument(
        "--keep-old",
        action="store_true",
        help="Do not delete assets that are not in the curated list (they will be deactivated).",
    )
    parser.add_argument(
        "--skip-purge",
        action="store_true",
        help="Keep quotes/logs for assets that drop out of the curated list.",
    )
    parser.add_argument(
        "--no-update-names",
        action="store_true",
        help="Skip updating asset names for already existing tickers.",
    )
    parser.add_argument(
        "--no-reactivate",
        action="store_true",
        help="Do not force is_active=True for assets present in the curated list.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args(sys.argv[1:])
    run(
        destructive=not args.keep_old,
        purge_quotes=not args.skip_purge,
        update_names=not args.no_update_names,
        reactivate=not args.no_reactivate,
        dry_run=args.dry_run,
        deactivate_removed=True,
    )
