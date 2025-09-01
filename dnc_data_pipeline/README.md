# DNC Data Pipeline

Pipeline de dados usando Apache Airflow com arquitetura em três camadas.

## Estrutura

```
├── dags/           # DAGs do Airflow
├── data/           # Dados do pipeline
│   ├── raw/        # Dados brutos
│   ├── bronze/     # Dados carregados
│   ├── silver/     # Dados limpos
│   └── gold/       # Dados agregados
├── config/         # Configurações
├── plugins/        # Plugins
└── logs/           # Logs
```

## Instalação

1. Clone o repositório
2. Instale as dependências: `pip install -r requirements.txt`
3. Execute o setup: `python setup.py`
4. Copie `env.example` para `.env`
5. Inicie: `docker-compose up -d`

## Uso

1. Acesse http://localhost:8080
2. Execute o DAG `data_pipeline_dag`
3. Monitore o progresso das tarefas

## Camadas

- **Bronze**: Dados brutos sem transformação
- **Silver**: Dados limpos e validados  
- **Gold**: Dados agregados para análise

## Tecnologias

- Apache Airflow
- Python
- Docker
- PostgreSQL
- Redis
