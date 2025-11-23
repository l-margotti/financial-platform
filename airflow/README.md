# Airflow - Financial Platform

Esta pasta contém os DAGs e configurações do Apache Airflow.

## Estrutura

- `dags/` - Definições de DAGs (workflows)
- `plugins/` - Custom operators, hooks e plugins
- `tests/` - Testes unitários dos DAGs
- `requirements.txt` - Dependências Python extras

## Desenvolvimento

Para adicionar novos DAGs, crie arquivos Python na pasta `dags/`.
O GitSync sincroniza automaticamente a cada 60 segundos.
