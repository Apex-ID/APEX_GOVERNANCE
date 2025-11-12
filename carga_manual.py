# carga_manual.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

# Configuração
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

CSV_TO_TABLE_MAP = [
    'ad_users_staging',
    'ad_computers_staging',
    'ad_groups_staging',
    'ad_ous_staging'
]

def carregar_dados_manualmente():
    """
    Script de carga direta para carregar CSVs aprovados nas tabelas de staging.
    """
    logging.info("--- INICIANDO CARGA MANUAL PARA STAGING ---")
    
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASS')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(db_url)

        with engine.connect() as connection:
            for table_name in CSV_TO_TABLE_MAP:
                csv_file = f"{table_name}.csv"
                logging.info(f"--- Processando: {csv_file} -> {table_name} ---")
                
                if not os.path.exists(csv_file):
                    logging.error(f"  ERRO: Arquivo '{csv_file}' não encontrado na pasta raiz. Pulando.")
                    continue

                logging.info(f"  -> Lendo o arquivo '{csv_file}'...")
                df = pd.read_csv(csv_file, dtype=str)

                # Limpeza de dados
                df.dropna(how='all', inplace=True)
                df = df.where(pd.notnull(df), None)
                df = df.map(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
                df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

                logging.info(f"  -> {len(df)} linhas válidas lidas.")
                if df.empty:
                    logging.warning("  AVISO: Nenhum dado para carregar.")
                    continue

                trans = connection.begin()
                try:
                    logging.info(f"  -> Limpando a tabela '{table_name}' (TRUNCATE)...")
                    connection.execute(text(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE;'))

                    logging.info(f"  -> Inserindo {len(df)} linhas em lotes...")
                    df.to_sql(
                        table_name,
                        con=connection,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    trans.commit()
                    logging.info(f"  SUCESSO: {len(df)} linhas carregadas na tabela '{table_name}'.")

                except Exception as e:
                    logging.error(f"  ERRO CRÍTICO ao carregar dados para '{table_name}': {e}")
                    trans.rollback()

    except Exception as e:
        logging.error(f"  ERRO GERAL na conexão ou execução da carga: {e}")
    finally:
        if 'engine' in locals():
            engine.dispose()
            logging.info("--- Carga manual finalizada. Conexão encerrada. ---")

if __name__ == "__main__":
    carregar_dados_manualmente()