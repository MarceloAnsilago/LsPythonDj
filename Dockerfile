FROM python:3.11-slim

# Evita gerar .pyc e ativa stdout sem buffer
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Ajuste de timezone (opcional, mas bom para logs)
ENV TZ=America/Sao_Paulo

ARG DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres
ENV DATABASE_URL=${DATABASE_URL}

WORKDIR /app

# Dependencies do sistema para psycopg2 e build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    tzdata \
  && rm -rf /var/lib/apt/lists/*

# Atualiza pip
RUN pip install --no-cache-dir --upgrade pip

# Instala libs Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o projeto
COPY . .

# Coleta estáticos para servir via WhiteNoise em produção
RUN python manage.py collectstatic --noinput

# Cria usuário não-root (melhor segurança)
RUN useradd -m appuser
USER appuser

# Porta interna (deve coincidir com fly.toml)
ENV PORT=8080
EXPOSE 8080

# Comando final
CMD ["gunicorn", "longshort.wsgi:application", "--bind", "0.0.0.0:8080"]
