import subprocess
import sys

print("Gerando dump da base de dados atual...")

# Executa o dumpdata e captura a saida
p = subprocess.Popen(
    [sys.executable, "manage.py", "dumpdata",
     "--natural-foreign", "--natural-primary",
     "--exclude", "contenttypes",
     "--exclude", "auth.Permission",
     "--exclude", "admin.LogEntry",
     "--indent", "2"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)

out, err = p.communicate()

if p.returncode != 0:
    print("\n[ERRO] executando dumpdata:\n")
    print(err)
    sys.exit(1)

# Grava sem BOM
with open("dump_longshort.json", "w", encoding="utf-8", newline="\n") as f:
    f.write(out)

print("\n[OK] Dump criado (dump_longshort.json) com sucesso!")
