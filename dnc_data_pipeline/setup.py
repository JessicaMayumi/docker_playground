import os
import sys
from pathlib import Path

def criar_diretorios():
    diretorios = [
        "data/raw",
        "data/bronze", 
        "data/silver",
        "data/gold",
        "logs",
        "config",
        "plugins"
    ]
    
    print("Criando estrutura de diretórios...")
    
    for dir_path in diretorios:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"Criado: {dir_path}")
    
    for init_dir in ["data", "dags", "plugins"]:
        init_file = Path(init_dir) / "__init__.py"
        if not init_file.exists():
            init_file.touch()
    
    print("Estrutura criada")

def criar_arquivos():
    for dir_path in ["data/bronze", "data/silver", "data/gold"]:
        gitkeep_file = Path(dir_path) / ".gitkeep"
        if not gitkeep_file.exists():
            gitkeep_file.touch()
    
    readme_content = """# DNC Data Pipeline

Pipeline de dados usando Apache Airflow com três camadas:

## Estrutura
- dags/ - DAGs do Airflow
- data/raw - Dados brutos
- data/bronze - Dados carregados
- data/silver - Dados limpos
- data/gold - Dados agregados
- config/ - Configurações
- plugins/ - Plugins customizados
- logs/ - Logs do Airflow

## Uso
1. Configure .env
2. docker-compose up -d
3. Acesse http://localhost:8080
4. Execute data_pipeline_dag
"""
    
    readme_file = Path("README.md")
    if not readme_file.exists():
        readme_file.write_text(readme_content, encoding='utf-8')
        print("README.md criado")

def validar():
    arquivos = ["docker-compose.yaml", "dags/dag.py", "requirements.txt"]
    
    print("Validando ambiente...")
    
    for file_path in arquivos:
        if Path(file_path).exists():
            print(f"OK: {file_path}")
        else:
            print(f"Faltando: {file_path}")
            return False
    
    return True

def main():
    print("DNC Data Pipeline Setup")
    print("=" * 30)
    
    try:
        criar_diretorios()
        criar_arquivos()
        
        if validar():
            print("Setup concluído")
            print("Próximos passos:")
            print("1. Copie env.example para .env")
            print("2. docker-compose up -d")
            print("3. Acesse http://localhost:8080")
        else:
            print("Setup com avisos")
            sys.exit(1)
            
    except Exception as e:
        print(f"Erro: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
