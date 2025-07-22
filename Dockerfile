FROM python:3.10-slim

# Variáveis de ambiente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Diretório da aplicação
WORKDIR /app

# Instala dependências
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copia código
COPY . .

# Cria diretórios de saída
RUN mkdir -p local_storage/zip local_storage/csv

# Executa o script com parâmetro opcional
CMD ["python", "extract/extract.py"]
