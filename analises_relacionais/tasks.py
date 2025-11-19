# Em: analises_relacionais/tasks.py

from celery import shared_task
from sqlalchemy import create_engine, text
import os
import pandas as pd
import logging
from .models import RelatorioAnaliseRelacional
from .regras_sql import REGRAS_RELACIONAIS_SQL# Importe a nova lista

logger = logging.getLogger(__name__)

@shared_task(bind=True)
def executar_analises_relacionais_task(self):
    logger.info("[RELACIONAL] Iniciando bateria completa de regras de negócio...")
    
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASS')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(db_url)

        # Limpa relatórios antigos para não poluir (Opcional)
        # RelatorioAnaliseRelacional.objects.all().delete()

        with engine.connect() as connection:
            for regra in REGRAS_RELACIONAIS_SQL:
                logger.info(f"  -> Executando regra: {regra['nome']}")
                
                try:
                    # 1. Executa a query que retorna as FALHAS
                    df_falhas = pd.read_sql_query(text(regra['sql']), connection)
                    qtd_falhas = len(df_falhas)
                    
                    # 2. Tenta descobrir o total de registros da tabela principal para calcular %
                    # Pega a primeira palavra após 'FROM' na query SQL como tabela base
                    # (Esta é uma aproximação simples, mas funciona para nossas queries)
                    sql_lower = regra['sql'].lower()
                    tabela_base = sql_lower.split('from')[1].strip().split(' ')[0]
                    
                    total_registros = connection.execute(text(f'SELECT COUNT(*) FROM {tabela_base}')).scalar()
                    
                    # 3. Calcula percentual de consistência (Qualidade)
                    if total_registros > 0:
                        percentual = ((total_registros - qtd_falhas) / total_registros) * 100
                    else:
                        percentual = 100.0

                    # 4. Prepara exemplos (JSON)
                    exemplos = df_falhas.head(20).to_dict(orient='records')

                    # 5. Salva
                    RelatorioAnaliseRelacional.objects.create(
                        nome_analise=regra['nome'],
                        tabelas_envolvidas=regra['tabelas'],
                        total_registros_analisados=total_registros,
                        total_inconsistencias=qtd_falhas,
                        percentual_consistencia=percentual,
                        descricao_impacto=regra['impacto'],
                        exemplos_inconsistencias=exemplos
                    )
                except Exception as erro_regra:
                    logger.error(f"Erro ao executar regra {regra['nome']}: {erro_regra}")
                    continue

        return {'estado': 'CONCLUÍDO', 'mensagem': 'Análises de Regras de Negócio concluídas.'}

    except Exception as e:
        logger.error(f"Erro Geral Relacional: {e}", exc_info=True)
        return {'estado': 'FALHOU', 'mensagem': str(e)}