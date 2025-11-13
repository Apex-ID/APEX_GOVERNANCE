from celery import shared_task
from django.utils import timezone
import traceback
import logging
from sqlalchemy import create_engine, text
import os
import pandas as pd

# Imports locais
from .models import RelatorioCompletude, RelatorioCompletudeGeral, RelatorioValidadeFormato, RelatorioUnicidadeGeral
from .logica_de_analise.logica_validade import executar_analise_de_validade
from .logica_de_analise.logica_unicidade import analisar_unicidade_tabela_inteira

logger = logging.getLogger(__name__)

# --- TAREFA 1: COMPLETUDE ESPECÍFICA (USUÁRIOS) ---
@shared_task(bind=True)
def executar_analise_completude_task(self):
    logger.info("[ANÁLISE] Iniciando Análise de Completude (Usuários)...")
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASS')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(db_url)

        with engine.connect() as connection:
            sql_query = text("""
            SELECT COUNT(*),
                COUNT(*) FILTER (WHERE "manager" IS NULL OR "manager" = ''),
                COUNT(*) FILTER (WHERE "department" IS NULL OR "department" = ''),
                COUNT(*) FILTER (WHERE "title" IS NULL OR "title" = ''),
                COUNT(*) FILTER (WHERE ("mail" IS NOT NULL AND "mail" != '') AND ("telephoneNumber" IS NOT NULL AND "telephoneNumber" != ''))
            FROM ad_users;
            """)
            result = connection.execute(sql_query).fetchone()
            
            if not result or result[0] == 0:
                return {'estado': 'FALHOU', 'mensagem': 'Tabela ad_users vazia.'}

            total, s_mgr, s_dept, s_title, contato = result
            
            RelatorioCompletude.objects.create(
                total_usuarios=total, sem_gerente=s_mgr, sem_departamento=s_dept, sem_cargo=s_title, contato_completo=contato,
                perc_sem_gerente=(s_mgr/total)*100, perc_sem_departamento=(s_dept/total)*100,
                perc_sem_cargo=(s_title/total)*100, perc_contato_completo=(contato/total)*100
            )
            return {'estado': 'CONCLUÍDO', 'mensagem': 'Análise de usuários concluída.'}
    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        return {'estado': 'FALHOU', 'mensagem': str(e)}

# --- TAREFA 2: COMPLETUDE GERAL (STAGING) ---
@shared_task(bind=True)
def executar_analise_completude_geral_task(self):
    logger.info("[ANÁLISE GERAL] Iniciando...")
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASS')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(db_url)
        
        tabelas = ['ad_users_staging', 'ad_computers_staging', 'ad_groups_staging', 'ad_ous_staging']

        with engine.connect() as connection:
            for tb in tabelas:
                df = pd.read_sql_table(tb, connection)
                if df.empty: continue
                
                df.replace('', pd.NA, inplace=True)
                total_celulas = df.size
                preenchidas = df.count().sum()
                perc = (preenchidas / total_celulas) * 100 if total_celulas > 0 else 0
                
                vazias = df.isnull().sum()
                vazias_dict = {k: int(v) for k, v in vazias[vazias > 0].to_dict().items()}

                RelatorioCompletudeGeral.objects.create(
                    tabela_analisada=tb, total_registros=len(df), total_colunas=len(df.columns),
                    total_celulas=total_celulas, total_celulas_preenchidas=preenchidas,
                    percentual_completude_geral=perc, relatorio_colunas_vazias=vazias_dict
                )
        return {'estado': 'CONCLUÍDO', 'mensagem': 'Análise Geral concluída.'}
    except Exception as e:
        return {'estado': 'FALHOU', 'mensagem': str(e)}

# --- TAREFA 3: VALIDADE (STAGING) ---
@shared_task(bind=True)
def executar_analise_validade_formato_task(self):
    logger.info("[ANÁLISE VALIDADE] Iniciando...")
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASS')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(db_url)
        
        tabelas = ['ad_users_staging', 'ad_computers_staging', 'ad_groups_staging', 'ad_ous_staging']

        with engine.connect() as connection:
            for tb in tabelas:
                df = pd.read_sql_table(tb, connection)
                if df.empty: continue
                
                res = executar_analise_de_validade(df)
                
                RelatorioValidadeFormato.objects.create(
                    tabela_analisada=tb,
                    total_celulas_preenchidas=res['total_celulas_preenchidas'],
                    total_celulas_invalidas=res['total_celulas_invalidas'],
                    total_celulas_vazias=res['total_celulas_vazias'],
                    percentual_validade=res['percentual_validade'],
                    detalhamento_erros=res['detalhamento_erros']
                )
        return {'estado': 'CONCLUÍDO', 'mensagem': 'Análise de Validade concluída.'}
    except Exception as e:
        return {'estado': 'FALHOU', 'mensagem': str(e)}

# --- TAREFA 4: UNICIDADE (STAGING E PRODUÇÃO) ---
def _executar_unicidade_geral(tabelas_alvo):
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
    engine = create_engine(db_url)

    with engine.connect() as connection:
        for table_name in tabelas_alvo:
            logger.info(f"[ANÁLISE UNICIDADE] Processando tabela: {table_name}")
            try:
                df = pd.read_sql_table(table_name, connection)
            except Exception as e:
                logger.warning(f"Tabela {table_name} não encontrada: {e}")
                continue
            
            if df.empty: continue
            
            # Chama a nova lógica que processa a tabela inteira
            resultado = analisar_unicidade_tabela_inteira(df, table_name)
            
            RelatorioUnicidadeGeral.objects.create(
                tabela_analisada=table_name,
                total_registros=resultado['total_registros'],
                total_colunas_analisadas=resultado['total_colunas_analisadas'],
                media_unicidade=resultado['media_unicidade'],
                qtd_colunas_com_duplicatas=resultado['qtd_colunas_com_duplicatas'],
                detalhe_por_coluna=resultado['detalhe_por_coluna']
            )

@shared_task(bind=True)
def executar_analise_unicidade_staging_task(self):
    try:
        _executar_unicidade_geral(['ad_users_staging', 'ad_computers_staging', 'ad_groups_staging', 'ad_ous_staging'])
        return {'estado': 'CONCLUÍDO', 'mensagem': 'Unicidade Staging concluída.'}
    except Exception as e:
        logger.error(f"Falha Unicidade Staging: {e}", exc_info=True)
        return {'estado': 'FALHOU', 'mensagem': str(e)}

@shared_task(bind=True)
def executar_analise_unicidade_producao_task(self):
    try:
        _executar_unicidade_geral(['ad_users', 'ad_computers', 'ad_groups', 'ad_ous'])
        return {'estado': 'CONCLUÍDO', 'mensagem': 'Unicidade Produção concluída.'}
    except Exception as e:
        logger.error(f"Falha Unicidade Produção: {e}", exc_info=True)
        return {'estado': 'FALHOU', 'mensagem': str(e)}