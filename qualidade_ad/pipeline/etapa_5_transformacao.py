# Dentro de: qualidade_ad/pipeline/etapa_5_transformacao.py

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import traceback
from django.utils import timezone
from qualidade_ad.models import LogEtapa

def get_process_function_sql(table_name, columns_info):
    """
    Gera o SQL para a função PL/pgSQL de processamento de uma tabela.
    *** ATUALIZADO PARA A NOVA ARQUITETURA ***
    """
    staging_table = f"{table_name}_staging"
    
    # Filtra a coluna 'id' para não incluí-la no INSERT ou SELECT
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

    # A função PL/pgSQL que tenta inserir; se falhar, registra o erro
    function_sql = f"""
        CREATE OR REPLACE FUNCTION process_{staging_table}(staging_row {staging_table})
        RETURNS VOID AS $$
        BEGIN
            -- Insere nas colunas de produção (pulando o 'id')
            INSERT INTO {table_name} ({col_names})
            SELECT {select_clause}
            FROM (SELECT staging_row.*) s;
        EXCEPTION WHEN OTHERS THEN
            -- ATUALIZADO: Insere o ID da linha de staging que falhou
            INSERT INTO etl_error_log (table_name, staging_row_id, error_message, raw_data)
            VALUES ('{table_name}', staging_row.id, SQLERRM, to_jsonb(staging_row));
        END;
        $$ LANGUAGE plpgsql;
    """
    return text(function_sql)

def executar_transformacao_e_carga(execucao_id):
    """
    Executa a Etapa 5 (Transformação Staging -> Produção) e registra um LogEtapa.
    *** ATUALIZADO PARA A NOVA ARQUITETURA ***
    """
    
    etapa_nome = 'ETAPA_5_TRANSFORMACAO' # Nome atualizado
    timestamp_inicio = timezone.now()
    resumo_da_etapa = ""
    status_final = "SUCESSO"
    
    try:
        print(f"  [Etapa 5] Iniciando transformação (Execução ID: {execucao_id})...")
        
        load_dotenv()
        tables_to_process = ['ad_computers', 'ad_groups', 'ad_ous', 'ad_users']
        
        db_user, db_pass, db_host, db_name = (os.getenv('DB_USER'), os.getenv('DB_PASS'), os.getenv('DB_HOST'), os.getenv('DB_NAME'))
        
        if not all([db_user, db_pass, db_host, db_name]):
            raise ValueError("Credenciais do DB ausentes no .env")

        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(db_url, execution_options={"statement_timeout": 900 * 1000})
        
        total_errors = 0
        total_success = 0

        with engine.connect() as connection:
            print("  [Etapa 5] Conexão estabelecida.")
            
            # A tabela de log já foi criada na Etapa 3
            
            for table_name in tables_to_process:
                staging_table = f"{table_name}_staging"
                print(f"    -> Processando: {staging_table} -> {table_name}")
                
                trans = connection.begin()
                try:
                    print("      -> Limpando tabelas de produção e log de erros antigos...")
                    connection.execute(text(f'TRUNCATE TABLE "{table_name}" CASCADE;'))
                    connection.execute(text(f"DELETE FROM etl_error_log WHERE table_name = '{table_name}';"))
                    
                    # Busca as colunas da tabela de *produção*
                    cols_info_query = text(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;")
                    cols_info = connection.execute(cols_info_query).fetchall()
                    
                    print("      -> Criando/Atualizando a função de processamento...")
                    function_sql = get_process_function_sql(table_name, cols_info)
                    connection.execute(text(f"DROP FUNCTION IF EXISTS process_{staging_table}({staging_table});"))
                    connection.execute(function_sql)
                    
                    total_rows = connection.execute(text(f'SELECT COUNT(*) FROM {staging_table}')).scalar()
                    print(f"      -> Aplicando função em {total_rows} linhas...")
                    
                    connection.execute(text(f"SELECT process_{staging_table}(s) FROM {staging_table} s;"))
                    trans.commit()
                    
                    error_count = connection.execute(text(f"SELECT COUNT(*) FROM etl_error_log WHERE table_name = '{table_name}'")).scalar()
                    success_count = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                    
                    print(f"      SUCESSO: Processamento concluído.")
                    print(f"        - Linhas inseridas com sucesso: {success_count}")
                    print(f"        - Linhas com erro (logadas): {error_count}")
                    
                    total_errors += error_count
                    total_success += success_count
                    connection.commit()
                    
                except Exception as e:
                    trans.rollback()
                    raise e

        if total_errors > 0:
            resumo_da_etapa = f"Transformação concluída. {total_success} linhas carregadas. ATENÇÃO: {total_errors} erros registrados."
        else:
            resumo_da_etapa = f"Transformação concluída. Todas as {total_success} linhas foram carregadas com sucesso."
        print(f"  [Etapa 5] {resumo_da_etapa}")

    except Exception as e:
        status_final = "FALHOU"
        resumo_da_etapa = f"ERRO CRÍTICO: {e}\n{traceback.format_exc()}"
        print(f"  [Etapa 5] ERRO: {resumo_da_etapa}")
        raise e
    
    finally:
        timestamp_fim = timezone.now()
        try:
            LogEtapa.objects.create(
                execucao_id=execucao_id,
                etapa_nome=etapa_nome,
                status=status_final,
                timestamp_inicio=timestamp_inicio,
                timestamp_fim=timestamp_fim,
                resumo_execucao=resumo_da_etapa
            )
            print(f"  [Etapa 5] Log de execução salvo no banco de dados.")
        except Exception as db_error:
            print(f"  [Etapa 5] ERRO CRÍTICO AO SALVAR LOG: {db_error}")
            
    return resumo_da_etapa