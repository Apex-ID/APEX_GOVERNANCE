# analises_simples/tasks.py

from celery import shared_task
from django.utils import timezone
import traceback
import logging
from sqlalchemy import create_engine, text
import os
from .models import RelatorioCompletude, RelatorioCompletudeGeral
import pandas as pd

logger = logging.getLogger(__name__)

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
            SELECT
                COUNT(*) AS total_usuarios,
                COUNT(*) FILTER (WHERE "manager" IS NULL OR "manager" = '') AS sem_gerente,
                COUNT(*) FILTER (WHERE "department" IS NULL OR "department" = '') AS sem_departamento,
                COUNT(*) FILTER (WHERE "title" IS NULL OR "title" = '') AS sem_cargo,
                COUNT(*) FILTER (
                    WHERE ("mail" IS NOT NULL AND "mail" != '')
                      AND ("telephoneNumber" IS NOT NULL AND "telephoneNumber" != '')
                ) AS contato_completo
            FROM ad_users;
            """)
            result = connection.execute(sql_query).fetchone()
            
            if result is None or result[0] == 0:
                logger.warning("[ANÁLISE] A tabela 'ad_users' está vazia. Análise abortada.")
                return {'estado': 'FALHOU', 'mensagem': 'Tabela ad_users está vazia.'}

            total_usuarios, sem_gerente, sem_departamento, sem_cargo, contato_completo = result
            
            relatorio = RelatorioCompletude.objects.create(
                total_usuarios = total_usuarios, sem_gerente = sem_gerente,
                sem_departamento = sem_departamento, sem_cargo = sem_cargo,
                contato_completo = contato_completo,
                perc_sem_gerente = (sem_gerente / total_usuarios) * 100,
                perc_sem_departamento = (sem_departamento / total_usuarios) * 100,
                perc_sem_cargo = (sem_cargo / total_usuarios) * 100,
                perc_contato_completo = (contato_completo / total_usuarios) * 100
            )
            logger.info(f"[ANÁLISE] Análise de Completude concluída. Relatório #{relatorio.id} salvo.")
            return {'estado': 'CONCLUÍDO', 'mensagem': f'Análise de Completude salva como Relatório #{relatorio.id}'}
    
    except Exception as e:
        logger.error(f"[ANÁLISE] FALHA CRÍTICA na Análise de Completude: {e}", exc_info=True)
        self.update_state(state='FAILURE', meta={'tipo_erro': type(e).__name__, 'mensagem_erro': str(e)})
        
        return {'estado': 'FALHOU', 'mensagem': f"Falha na análise: {e}"}

@shared_task(bind=True)
def executar_analise_completude_geral_task(self):
    logger.info("[ANÁLISE GERAL] Iniciando Análise de Completude Geral (Staging)...")
    timestamp_inicio_analise = timezone.now()
    tabelas_staging = ['ad_users_staging', 'ad_computers_staging', 'ad_groups_staging', 'ad_ous_staging']
    
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASS')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(db_url)

        with engine.connect() as connection:
            for table_name in tabelas_staging:
                logger.info(f"[ANÁLISE GERAL] Processando tabela: {table_name}")
                df = pd.read_sql_table(table_name, connection)

                if df.empty:
                    logger.warning(f"[ANÁLISE GERAL] Tabela {table_name} está vazia. Pulando.")
                    continue
                
                df.replace('', pd.NA, inplace=True) 
                total_registros, total_colunas = df.shape
                total_celulas = df.size
                total_celulas_preenchidas = df.count().sum() 
                percentual_geral = (total_celulas_preenchidas / total_celulas) * 100 if total_celulas > 0 else 0
                
                vazias_por_coluna = df.isnull().sum()
                vazias_filtrado = {k: int(v) for k, v in vazias_por_coluna[vazias_por_coluna > 0].to_dict().items()}

                RelatorioCompletudeGeral.objects.create(
                    timestamp_inicio=timestamp_inicio_analise,
                    timestamp_fim=timezone.now(),
                    tabela_analisada=table_name,
                    total_registros=total_registros,
                    total_colunas=total_colunas,
                    total_celulas=total_celulas,
                    total_celulas_preenchidas=total_celulas_preenchidas,
                    percentual_completude_geral=percentual_geral,
                    relatorio_colunas_vazias=vazias_filtrado
                )
            
            logger.info(f"[ANÁLISE GERAL] Análise de Completude Geral concluída.")
            return {'estado': 'CONCLUÍDO', 'mensagem': f'Análise Geral de Staging concluída.'}

    except Exception as e:
        logger.error(f"[ANÁLISE GERAL] FALHA CRÍTICA na Análise Geral: {e}", exc_info=True)
        self.update_state(state='FAILURE', meta={'tipo_erro': type(e).__name__, 'mensagem_erro': str(e)})
        
        return {'estado': 'FALHOU', 'mensagem': f"Falha na análise geral: {e}"}