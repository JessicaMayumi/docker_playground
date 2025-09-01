from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import os
import logging

# Configuracoes do DAG
default_args = {
    'owner': 'dnc_insight',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definicao do DAG
dag = DAG(
    dag_id='data_pipeline_dag',
    default_args=default_args,
    description='Pipeline de dados para DncInsight Solutions',
    schedule='@daily',
    catchup=False,
    tags=['data_pipeline', 'bronze', 'silver', 'gold']
)

# Funcao para carregar dados brutos na camada Bronze
def upload_raw_data_to_bronze(**context):
    """
    Carrega dados brutos do CSV para a camada Bronze
    """
    try:
        raw_data_path = '/opt/airflow/data/raw/raw_data.csv'
        bronze_path = '/opt/airflow/data/bronze/bronze_data.csv'
        
        if not os.path.exists(raw_data_path):
            raise FileNotFoundError(f"Arquivo bruto nao encontrado: {raw_data_path}")
        
        # Criar diretorio bronze se nao existir
        os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
        
        # Ler dados brutos
        df = pd.read_csv(raw_data_path)
        
        # Salvar na camada Bronze
        df.to_csv(bronze_path, index=False)
        logging.info(f"Dados carregados na camada Bronze: {len(df)} registros")
        
        # Retornar informacoes para a proxima tarefa
        context['task_instance'].xcom_push(key='bronze_count', value=len(df))
        
        return f"Sucesso: {len(df)} registros carregados na camada Bronze"
        
    except Exception as e:
        logging.error(f"Erro ao carregar dados na camada Bronze: {str(e)}")
        raise

def process_bronze_to_silver(**context):
    """
    Processa dados da camada Bronze para Silver com limpeza e transformacoes
    """
    try:
        bronze_path = '/opt/airflow/data/bronze/bronze_data.csv'
        silver_path = '/opt/airflow/data/silver/silver_data.csv'
        
        # Criar diretorio silver se nao existir
        os.makedirs(os.path.dirname(silver_path), exist_ok=True)
        
        # Ler dados da camada Bronze
        df = pd.read_csv(bronze_path)
        initial_count = len(df)
        
        # Limpeza de dados
        # Remover registros com campos nulos (nome, email, data de nascimento)
        df_cleaned = df.dropna(subset=['name', 'email', 'date_of_birth'])
        
        # Corrigir formatos de email invalidos (deve conter @)
        df_cleaned = df_cleaned[df_cleaned['email'].str.contains('@', na=False)]
        
        # Calcular idade dos usuarios com base na data de nascimento
        df_cleaned['birth_date'] = pd.to_datetime(df_cleaned['date_of_birth'], errors='coerce')
        df_cleaned = df_cleaned.dropna(subset=['birth_date'])
        
        # Calcular idade
        current_date = pd.Timestamp.now()
        df_cleaned['age'] = (current_date - df_cleaned['birth_date']).dt.days // 365
        
        # Salvar na camada Silver
        df_cleaned.to_csv(silver_path, index=False)
        final_count = len(df_cleaned)
        removed_count = initial_count - final_count
        
        logging.info(f"Processamento Bronze para Silver concluido:")
        logging.info(f"  - Registros iniciais: {initial_count}")
        logging.info(f"  - Registros removidos: {removed_count}")
        logging.info(f"  - Registros finais: {final_count}")
        
        # Retornar informacoes para a proxima tarefa
        context['task_instance'].xcom_push(key='silver_count', value=final_count)
        
        return f"Sucesso: {final_count} registros processados na camada Silver"
        
    except Exception as e:
        logging.error(f"Erro ao processar dados da camada Bronze para Silver: {str(e)}")
        raise

def process_silver_to_gold(**context):
    """
    Processa dados da camada Silver para Gold com agregacoes e transformacoes
    """
    try:
        silver_path = '/opt/airflow/data/silver/silver_data.csv'
        gold_path = '/opt/airflow/data/gold/gold_data.csv'
        
        # Criar diretorio gold se nao existir
        os.makedirs(os.path.dirname(gold_path), exist_ok=True)
        
        # Ler dados da camada Silver
        df = pd.read_csv(silver_path)
        
        # Transformacoes para a camada Gold
        # Criar faixas etarias
        age_bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        age_labels = ['0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '81-90', '90+']
        
        df['age_group'] = pd.cut(df['age'], bins=age_bins, labels=age_labels, right=False)
        
        aggregated_data = df.groupby(['age_group', 'subscription_status']).size().reset_index(name='user_count')
        
        # Adicionar informacoes demograficas
        demographic_summary = df.groupby('age_group').agg({
            'age': ['count', 'mean', 'min', 'max'],
            'subscription_status': lambda x: (x == 'active').sum()
        }).round(2)
        
        demographic_summary.columns = ['total_users', 'avg_age', 'min_age', 'max_age', 'active_users']
        demographic_summary['inactive_users'] = demographic_summary['total_users'] - demographic_summary['active_users']
        demographic_summary = demographic_summary.reset_index()
        
        # Salvar dados agregados na camada Gold
        aggregated_data.to_csv(gold_path, index=False)
        
        # Salvar resumo demografico
        demographic_path = '/opt/airflow/data/gold/demographic_summary.csv'
        demographic_summary.to_csv(demographic_path, index=False)
        logging.info(f"Processamento Silver para Gold concluido:")
        logging.info(f"  - Registros processados: {len(df)}")
        logging.info(f"  - Faixas etarias criadas: {len(age_labels)}")
        logging.info(f"  - Agregacoes por status: {len(aggregated_data)}")
        
        # Retornar informacoes
        context['task_instance'].xcom_push(key='gold_count', value=len(aggregated_data))
        
        return f"Sucesso: Dados transformados e salvos na camada Gold"
        
    except Exception as e:
        logging.error(f"Erro ao processar dados da camada Silver para Gold: {str(e)}")
        raise

# tasks
start_task = EmptyOperator(
    task_id='start_pipeline',
    dag=dag
)

bronze_task = PythonOperator(
    task_id='upload_raw_data_to_bronze',
    python_callable=upload_raw_data_to_bronze,
    dag=dag
)

silver_task = PythonOperator(
    task_id='process_bronze_to_silver',
    python_callable=process_bronze_to_silver,
    dag=dag
)

gold_task = PythonOperator(
    task_id='process_silver_to_gold',
    python_callable=process_silver_to_gold,
    dag=dag
)

end_task = EmptyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Definicao das dependencias
start_task >> bronze_task >> silver_task >> gold_task >> end_task
