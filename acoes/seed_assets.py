# acoes/seed_assets.py
from acoes.models import Asset

# 30 ações mais líquidas da B3 (tickers + nomes simplificados)
ASSETS = [
    ("PETR4", "PETROBRAS PN"),
    ("VALE3", "VALE ON"),
    ("ITUB4", "ITAÚ UNIBANCO PN"),
    ("BBDC4", "BRADESCO PN"),
    ("ABEV3", "AMBEV ON"),
    ("BBAS3", "BANCO DO BRASIL ON"),
    ("ELET3", "ELETROBRAS ON"),
    ("ELET6", "ELETROBRAS PN"),
    ("CSNA3", "SID NACIONAL ON"),
    ("USIM5", "USIMINAS PN"),
    ("CMIG4", "CEMIG PN"),
    ("GGBR4", "GERDAU PN"),
    ("GOAU4", "METALÚRGICA GERDAU PN"),
    ("WEGE3", "WEG ON"),
    ("SUZB3", "SUZANO ON"),
    ("JBSS3", "JBS ON"),
    ("PRIO3", "PETRORIO ON"),
    ("RAIL3", "RUMO ON"),
    ("MULT3", "MULTIPLAN ON"),
    ("B3SA3", "B3 ON"),
    ("RENT3", "LOCALIZA ON"),
    ("ITSA4", "ITAUSA PN"),
    ("BRFS3", "BRF ON"),
    ("LREN3", "LOJAS RENNER ON"),
    ("HAPV3", "HAPVIDA ON"),
    ("RADL3", "RAIADROGASIL ON"),
    ("CYRE3", "CYRELA ON"),
    ("PETR3", "PETROBRAS ON"),
    ("NTCO3", "NATURA ON"),
    ("EGIE3", "ENGIE BRASIL ON"),
]

def run():
    for ticker, name in ASSETS:
        obj, created = Asset.objects.get_or_create(
            ticker=ticker,
            defaults={"name": name, "is_active": True},
        )
        print(f"{'✔️ Inserido' if created else '↩️ Já existia'}: {obj.ticker}")
