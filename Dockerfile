FROM python:3.10-slim

# ── Env básicos
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONPATH=/app/src/main

# ── Dependências de sistema (curl p/ fallback TLS; certs)
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── App
WORKDIR /app

# Instala Python deps primeiro para cache
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copia o código
COPY . .

# Pastas locais usadas pelo extractor/transform
RUN mkdir -p local_storage/zip local_storage/csv local_storage/curated

# ── Entrada padrão: extrator (CLI)
# Você pode passar args em `docker run ... --year_month 202401` etc.
ENTRYPOINT ["python", "-m", "cli.run_extract"]
CMD []
