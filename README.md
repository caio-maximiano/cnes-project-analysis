# Sistema de Recomendação para Alocação Eficiente de Profissionais de Saúde

Este projeto é parte do Trabalho de Conclusão de Curso (TCC) do MBA em *Machine Learning in Production*. O objetivo é desenvolver um sistema de recomendação para apoiar gestores públicos ou operadoras de saúde na tomada de decisão sobre a alocação de profissionais, utilizando dados públicos do CNES (Cadastro Nacional de Estabelecimentos de Saúde).

---

## 🎯 Objetivo

Construir um pipeline completo de machine learning capaz de recomendar, com base em critérios técnicos e contextuais, os locais mais adequados para a alocação de profissionais de saúde, considerando retorno esperado (econômico e/ou social).

---

## 🧠 Hipóteses

- É possível estimar o retorno esperado da alocação de um profissional com base em dados do CNES e contexto regional.
- Gestores tomam melhores decisões com sugestões prescritivas (recomendações) e não apenas predições de risco.
- A carência regional, infraestrutura existente e tipo de profissional impactam significativamente na eficiência da alocação.

---

## 📦 Fonte de Dados

**Cadastro Nacional de Estabelecimentos de Saúde (CNES)** — base pública e oficial com informações como:

- Tipo e categoria do estabelecimento
- Localização (UF, município, zona urbana/rural)
- Equipamentos disponíveis
- Recursos humanos (quantidade e especialidades)
- Especialidades médicas atendidas
- Histórico de alterações no cadastro

---

## 💡 Exemplos de perguntas respondidas pelo sistema

- Em quais municípios é mais eficiente alocar um novo **dermatologista**?
- Qual o **retorno esperado** (uso da estrutura, potencial de atendimento) ao alocar um novo profissional em determinado local?
- Onde estão os **maiores gaps de cobertura** para determinada especialidade?
- Onde uma operadora de saúde teria maior chance de retorno financeiro com uma nova unidade?

---

## 🔧 Técnicas e Modelos

| Finalidade                                  | Abordagem técnica                     | Métrica de avaliação      |
|--------------------------------------------|---------------------------------------|----------------------------|
| Estimar retorno esperado da alocação       | Regressão (Linear, XGBoost, etc.)     | RMSE, R²                   |
| Identificar locais com gaps de cobertura   | Score heurístico + Clustering         | Silhouette Score           |
| Recomendar Top-N locais por especialidade  | Sistema híbrido de recomendação       | Precision@k, MAP, NDCG     |

---

## ⚙️ Ferramentas e Arquitetura Sugerida

Este projeto será desenvolvido com foco em praticidade, reprodutibilidade e aplicação real. A arquitetura abaixo é pensada para ser executável por uma única pessoa com uso de serviços gerenciados na nuvem (preferencialmente **Azure**), além de bibliotecas populares em Python.

### 🔧 Ferramentas

| Etapa                      | Ferramenta                             |
|---------------------------|----------------------------------------|
| Coleta e ingestão         | Python (FTP, requests), Azure Blob     |
| Processamento             | Pandas ou PySpark                      |
| Modelagem                 | Scikit-learn, XGBoost                  |
| Tracking de experimentos  | MLflow                                 |
| Deploy do modelo          | FastAPI                                |
| Interface de usuário      | Streamlit                              |
| Versionamento de código   | Git + GitHub                           |
| Orquestração (opcional)   | Azure Data Factory ou scripts agendados|

### ☁️ Arquitetura simplificada na Azure

```plaintext
                            +-------------------------+
                            |     Fonte de Dados      |
                            |  FTP do CNES / IBGE     |
                            +-----------+-------------+
                                        |
                                        v
                            +-------------------------+
                            |  Script de Coleta e     |
                            |  Pré-processamento      |
                            | (executado localmente ou|
                            |  via Azure Data Factory)|
                            +-----------+-------------+
                                        |
                                        v
                            +-------------------------+
                            |    Azure Blob Storage   |
                            | (armazenamento de dados |
                            |   brutos e tratados)    |
                            +-----------+-------------+
                                        |
                                        v
                            +-------------------------+
                            |   Treinamento do Modelo |
                            |   (Notebook local ou    |
                            | Azure ML / Databricks)  |
                            +-----------+-------------+
                                        |
                                        v
                            +-------------------------+
                            |  MLflow Tracking Server |
                            | (local ou hospedado)    |
                            +-----------+-------------+
                                        |
                                        v
                            +-------------------------+
                            |  Deploy com FastAPI     |
                            | (via Azure App Service) |
                            +-----------+-------------+
                                        |
                                        v
                            +-------------------------+
                            |  Interface Streamlit    |
                            | (local ou em Azure App  |
                            |       Service)          |
                            +-------------------------+
```


### 📌 Considerações práticas

- O deploy pode ser feito via **Docker + Azure App Service**.
- Os artefatos do modelo (joblib/pickle) podem ser armazenados em **Azure Blob Storage**.
- A coleta e atualização podem ser automatizadas com **Azure Data Factory**, `cron` ou Azure Functions.
- A interface em Streamlit pode rodar localmente ou na nuvem.

---

## 🚀 Entregáveis

- Pipeline de machine learning com dados reais
- Sistema de recomendação funcional
- API REST documentada (Swagger/OpenAPI)
- Interface interativa para consulta de gestores
- Repositório Git versionado com documentação
- Relatório técnico de TCC