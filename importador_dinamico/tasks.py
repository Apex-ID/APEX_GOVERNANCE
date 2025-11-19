from celery import shared_task
import pandas as pd
import os
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from django.conf import settings
import logging
import datetime

logger = logging.getLogger(__name__)

@shared_task(bind=True)
def executar_importacao_dinamica_task(self, file_path, novo_db_nome, colunas_selecionadas, nome_tabela_destino):
    """
    Cria um novo banco de dados e importa as colunas selecionadas do CSV.
    """
    logger.info(f"[IMPORTADOR] Iniciando criação do banco '{novo_db_nome}'...")

    # Credenciais do Admin (para poder criar bancos)
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_host = os.getenv('DB_HOST')
    # Conecta no banco 'postgres' (banco padrão de manutenção)
    
    try:
        # 1. CRIAÇÃO DO BANCO DE DADOS
        # Precisamos usar psycopg2 direto pois CREATE DATABASE não roda em transação
        conn = psycopg2.connect(
            user=db_user, password=db_pass, host=db_host, dbname='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Verifica se o banco já existe
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{novo_db_nome}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f'CREATE DATABASE "{novo_db_nome}"')
            logger.info(f"[IMPORTADOR] Banco '{novo_db_nome}' criado com sucesso.")
        else:
            logger.warning(f"[IMPORTADOR] Banco '{novo_db_nome}' já existe. Usando o existente.")
        
        cursor.close()
        conn.close()

        # 2. LEITURA E LIMPEZA DO CSV
        logger.info(f"[IMPORTADOR] Lendo arquivo: {file_path}")
        # Lê apenas as colunas que o usuário escolheu
        df = pd.read_csv(file_path, usecols=colunas_selecionadas, dtype=str)
        
        # Limpeza básica
        df.dropna(how='all', inplace=True) # Remove linhas vazias
        df.replace('', None, inplace=True) # String vazia vira NULL
        # Remove caracteres nulos e espaços
        df = df.map(lambda x: x.replace('\x00', '').strip() if isinstance(x, str) else x)
        
        total_linhas = len(df)

        # 3. CONEXÃO COM O NOVO BANCO E CARGA
        logger.info(f"[IMPORTADOR] Conectando ao novo banco '{novo_db_nome}'...")
        novo_db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{novo_db_nome}"
        engine = create_engine(novo_db_url)

        with engine.connect() as connection:
            # Cria a tabela de Staging Automaticamente
            # O Pandas to_sql é perfeito aqui, pois ele infere os tipos (geralmente TEXT)
            # Vamos forçar tudo como TEXT para evitar erros de carga
            logger.info(f"[IMPORTADOR] Criando tabela '{nome_tabela_destino}' e carregando dados...")
            
            df.to_sql(
                nome_tabela_destino,
                con=connection,
                if_exists='replace', # Recria a tabela se existir
                index=True, # CRIA O ID AUTOMÁTICO (index do pandas)
                index_label='id', # Nomeia a coluna de index como 'id'
                chunksize=1000,
                method='multi'
            )
            
            # Adiciona chave primária ao ID (O pandas cria como bigint index, mas não PK formalmente no Postgres às vezes)
            connection.execute(text(f'ALTER TABLE "{nome_tabela_destino}" ADD PRIMARY KEY (id);'))
            connection.commit()

        logger.info(f"[IMPORTADOR] Sucesso! {total_linhas} linhas importadas em '{novo_db_nome}'.")
        
        # Remove o arquivo temporário após o sucesso
        if os.path.exists(file_path):
            os.remove(file_path)

        return {'estado': 'CONCLUÍDO', 'mensagem': f"Banco '{novo_db_nome}' criado. Tabela '{nome_tabela_destino}' carregada com {total_linhas} registros."}

    except Exception as e:
        logger.error(f"[IMPORTADOR] Erro Crítico: {e}", exc_info=True)
        return {'estado': 'FALHOU', 'mensagem': str(e)}