# CNES Project Analysis

Este repositório contém o **pipeline de ingestão, processamento e análise dos dados do CNES**
(Cadastro Nacional de Estabelecimentos de Saúde), estruturado em camadas **Bronze / Silver / Gold**
e executado de forma **automatizada e conteinerizada na Azure**.

O objetivo principal é transformar dados públicos brutos do CNES em **bases analíticas confiáveis**
para exploração, visualização e consumo por modelos de Machine Learning.

---

### Visão Geral

- **Fonte de dados**: Servidores públicos do CNES (DATASUS)
- **Orquestração**: Azure Logic Apps
- **Execução**: Azure Container Instances (Docker)
- **Armazenamento**: Azure Data Lake Gen2
- **Processamento**: Python (Pandas / PyArrow)
- **CI/CD**: GitHub Actions + Azure Container Registry

---

## 🥉🥈🥇 Camadas de Dados

### Bronze
- Download automático dos arquivos oficiais do CNES
- Armazenamento **raw**, sem transformação
- Versionamento por competência (year-month)

### Silver
- Limpeza e padronização de schemas
- Normalização de colunas
- Conversão para formatos analíticos (CSV / Parquet)

### Gold
- Agregações analíticas
- Métricas por município, estado, tipo de estabelecimento e especialidade
- Base pronta para consumo por dashboards e modelos de ML

---

## ⚙️ Execução do Pipeline

### Build da imagem Docker
```bash
docker build --platform=linux/amd64 -t cnes-pipeline .
