# qualidade_ad/tasks.py

from celery import shared_task
from django.utils import timezone
import traceback
import logging

logger = logging.getLogger(__name__)

from .models import ExecucaoPipeline, LogEtapa
from .pipeline.etapa_1_extracao import executar_extracao_ad
from .pipeline.etapa_2_limpeza import executar_limpeza_csvs
from .pipeline.etapa_3_preparacao_banco import executar_preparacao_banco
from .pipeline.etapa_4_carga_staging import executar_carga_staging
from .pipeline.etapa_5_transformacao import executar_transformacao_e_carga

@shared_task(bind=True)
def executar_pipeline_completo_task(self):
    execucao = ExecucaoPipeline.objects.create(status='INICIADO')
    try:
        execucao.status = 'EM_PROGRESSO'
        execucao.save()
        
        logger.info(f"[Execução #{execucao.id}] INICIANDO ETAPA 1: Extração...")
        self.update_state(state='PROGRESS', meta={'passo_atual': 20, 'mensagem_status': 'Iniciando Etapa 1: Extração...'})
        executar_extracao_ad(execucao.id)

        logger.info(f"[Execução #{execucao.id}] INICIANDO ETAPA 2: Limpeza...")
        self.update_state(state='PROGRESS', meta={'passo_atual': 40, 'mensagem_status': 'Iniciando Etapa 2: Limpeza...'})
        executar_limpeza_csvs(execucao.id)

        logger.info(f"[Execução #{execucao.id}] INICIANDO ETAPA 3: Preparação do Banco...")
        self.update_state(state='PROGRESS', meta={'passo_atual': 60, 'mensagem_status': 'Iniciando Etapa 3: Preparação do Banco...'})
        executar_preparacao_banco(execucao.id)

        logger.info(f"[Execução #{execucao.id}] INICIANDO ETAPA 4: Carga Staging...")
        self.update_state(state='PROGRESS', meta={'passo_atual': 80, 'mensagem_status': 'Iniciando Etapa 4: Carga Staging...'})
        executar_carga_staging(execucao.id)

        logger.info(f"[Execução #{execucao.id}] INICIANDO ETAPA 5: Transformação...")
        self.update_state(state='PROGRESS', meta={'passo_atual': 100, 'mensagem_status': 'Iniciando Etapa 5: Transformação...'})
        executar_transformacao_e_carga(execucao.id)

        execucao.status = 'CONCLUIDO'
        execucao.timestamp_fim = timezone.now()
        execucao.save()
        
        logger.info(f"[Execução #{execucao.id}] Pipeline concluído com SUCESSO.")
        return {'estado': 'CONCLUÍDO', 'mensagem': f'Pipeline (Execução #{execucao.id}) concluído com sucesso!'}

    except Exception as e:
        logger.error(f"[Execução #{execucao.id}] FALHA CRÍTICA NO PIPELINE: {e}", exc_info=True)
        execucao.status = 'FALHOU'
        execucao.timestamp_fim = timezone.now()
        execucao.save()
        self.update_state(state='FAILURE', meta={'tipo_erro': type(e).__name__, 'mensagem_erro': str(e)})
        
       
        return {'estado': 'FALHOU', 'mensagem': f"Falha no pipeline: {e}"}
        # -------------------------------


@shared_task(bind=True)
def importar_arquivos_existentes_task(self):
    execucao = ExecucaoPipeline.objects.create(status='INICIADO')
    try:
        execucao.status = 'EM_PROGRESSO'
        execucao.save()

        logger.info(f"[Execução #{execucao.id}] INICIANDO ETAPA 2: Limpeza...")
        self.update_state(state='PROGRESS', meta={'passo_atual': 25, 'mensagem_status': 'Iniciando Etapa 2: Limpeza...'})
        executar_limpeza_csvs(execucao.id)

        logger.info(f"[Execução #{execucao.id}] INICIANDO ETAPA 3: Preparação do Banco...")
        self.update_state(state='PROGRESS', meta={'passo_atual': 50, 'mensagem_status': 'Iniciando Etapa 3: Preparação do Banco...'})
        executar_preparacao_banco(execucao.id)

        logger.info(f"[Execução #{execucao.id}] INICIANDO ETAPA 4: Carga Staging...")
        self.update_state(state='PROGRESS', meta={'passo_atual': 75, 'mensagem_status': 'Iniciando Etapa 4: Carga Staging...'})
        executar_carga_staging(execucao.id)

        logger.info(f"[Execução #{execucao.id}] INICIANDO ETAPA 5: Transformação...")
        self.update_state(state='PROGRESS', meta={'passo_atual': 100, 'mensagem_status': 'Iniciando Etapa 5: Transformação...'})
        executar_transformacao_e_carga(execucao.id)

        execucao.status = 'CONCLUIDO'
        execucao.timestamp_fim = timezone.now()
        execucao.save()
        logger.info(f"[Execução #{execucao.id}] Importação concluída com SUCESSO.")
        return {'estado': 'CONCLUÍDO', 'mensagem': f'Importação (Execução #{execucao.id}) concluída com sucesso!'}

    except Exception as e:
        logger.error(f"[Execução #{execucao.id}] FALHA CRÍTICA NA IMPORTAÇÃO: {e}", exc_info=True)
        execucao.status = 'FALHOU'
        execucao.timestamp_fim = timezone.now()
        execucao.save()
        self.update_state(state='FAILURE', meta={'tipo_erro': type(e).__name__, 'mensagem_erro': str(e)})
        
        
        return {'estado': 'FALHOU', 'mensagem': f"Falha na importação: {e}"}
        # -------------------------------