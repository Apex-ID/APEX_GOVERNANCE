# Dentro de: transformacao_manual.py

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

def get_process_function_sql(table_name, columns_info):
    """Gera o SQL para a função PL/pgSQL de processamento de uma tabela."""
    staging_table = f"{table_name}_staging"
    prod_columns_info = [c for c in columns_info if c[0] != 'id']
    col_names = ", ".join([f'"{c[0]}"' for c in prod_columns_info])
    
    select_expressions = []
    for col_name, col_type in prod_columns_info:
        if 'bytea' in col_type.lower():
            expression = f"DECODE(NULLIF(s.\"{col_name}\", ''), 'escape')"
        else:
            expression = f"CAST(NULLIF(s.\"{col_name}\", '') AS {col_type})"
        select_expressions.append(expression)
    select_clause = ", ".join(select_expressions)

    function_sql = f"""
        CREATE OR REPLACE FUNCTION process_{staging_table}(staging_row {staging_table})
        RETURNS VOID AS $$
        BEGIN
            INSERT INTO {table_name} ({col_names})
            SELECT {select_clause}
            FROM (SELECT staging_row.*) s;
        EXCEPTION WHEN OTHERS THEN
            INSERT INTO etl_error_log (table_name, staging_row_id, error_message, raw_data)
            VALUES ('{table_name}', staging_row.id, SQLERRM, to_jsonb(staging_row));
        END;
        $$ LANGUAGE plpgsql;
    """
    return text(function_sql)

def executar_transformacao_manual():
    """
    Executa a transformação de Staging -> Produção manualmente.
    """
    logging.info("--- INICIANDO TRANSFORMAÇÃO MANUAL (Staging -> Produção) ---")
    
    tables_to_process = ['ad_computers', 'ad_groups', 'ad_ous', 'ad_users']
    
    try:
        db_user, db_pass, db_host, db_name = (os.getenv('DB_USER'), os.getenv('DB_PASS'), os.getenv('DB_HOST'), os.getenv('DB_NAME'))
        
        if not all([db_user, db_pass, db_host, db_name]):
            logging.error("Credenciais do DB ausentes no .env")
            return

        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(db_url, execution_options={"statement_timeout": 900 * 1000})
        
        total_errors = 0
        total_success = 0

        with engine.connect() as connection:
            logging.info("Conexão com o PostgreSQL estabelecida.")
            
            for table_name in tables_to_process:
                staging_table = f"{table_name}_staging"
                logging.info(f"--- Processando: {staging_table} -> {table_name} ---")
                
                trans = connection.begin()
                try:
                    logging.info("  -> Limpando tabelas de produção e log de erros antigos...")
                    connection.execute(text(f'TRUNCATE TABLE "{table_name}" CASCADE;'))
                    connection.execute(text(f"DELETE FROM etl_error_log WHERE table_name = '{table_name}';"))
                    
                    cols_info_query = text(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;")
                    cols_info = connection.execute(cols_info_query).fetchall()
                    
                    logging.info("  -> Criando/Atualizando a função de processamento...")
                    function_sql = get_process_function_sql(table_name, cols_info)
                    connection.execute(text(f"DROP FUNCTION IF EXISTS process_{staging_table}({staging_table});"))
                    connection.execute(function_sql)
                    
                    total_rows = connection.execute(text(f'SELECT COUNT(*) FROM {staging_table}')).scalar()
                    logging.info(f"  -> Aplicando função em {total_rows} linhas...")
                    
                    connection.execute(text(f"SELECT process_{staging_table}(s) FROM {staging_table} s;"))
                    trans.commit()
                    
                    error_count = connection.execute(text(f"SELECT COUNT(*) FROM etl_error_log WHERE table_name = '{table_name}'")).scalar()
                    success_count = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                    
                    logging.info(f"  SUCESSO: Processamento concluído.")
                    logging.info(f"    - Linhas inseridas com sucesso: {success_count}")
                    logging.info(f"    - Linhas com erro (logadas): {error_count}")
                    
                    total_errors += error_count
                    total_success += success_count
                    connection.commit()
                    
                except Exception as e:
                    logging.error(f"  ERRO CRÍTICO no processamento da tabela '{table_name}': {e}")
                    trans.rollback()

            logging.info("--- RELATÓRIO FINAL DA TRANSFORMAÇÃO ---")
            if total_errors > 0:
                logging.warning(f"Transformação concluída. {total_success} linhas carregadas. ATENÇÃO: {total_errors} erros registrados.")
            else:
                logging.info(f"Transformação concluída. Todas as {total_success} linhas foram carregadas com sucesso.")

    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado: {e}")
    finally:
        if 'engine' in locals():
            engine.dispose()
            logging.info("Conexão com o PostgreSQL foi encerrada.")

if __name__ == "__main__":
    executar_transformacao_manual()