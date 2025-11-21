# Em: analises_relacionais/tasks.py

from celery import shared_task
from sqlalchemy import create_engine, text
import os
import pandas as pd
import logging

# 1. Importa a lista correta de regras
from .regras_sql import REGRAS_DE_NEGOCIO_SQL 

# 2. Importa os models do próprio app
from .models import (
    RelatorioAnaliseRelacional, 
    RelatorioDQI, 
    RelatorioRiscoSenha
)

# 3. Importa models de outros apps para o DQI
from analises_simples.models import (
    RelatorioCompletudeGeral, 
    RelatorioValidadeFormato, 
    RelatorioUnicidadeGeral
)

logger = logging.getLogger(__name__)

@shared_task(bind=True)
def executar_analises_relacionais_task(self):
    """
    Executa as 23 regras de negócio definidas em REGRAS_DE_NEGOCIO_SQL.
    """
    logger.info("[RELACIONAL] Iniciando análises cruzadas...")
    
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASS')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(db_url)

        with engine.connect() as connection:
            # Itera sobre a lista importada de regras_sql.py
            for regra in REGRAS_DE_NEGOCIO_SQL:
                logger.info(f"  -> Executando regra: {regra['nome']}")
                
                try:
                    # 1. Executa a query SQL da regra
                    df_falhas = pd.read_sql_query(text(regra['sql']), connection)
                    qtd_falhas = len(df_falhas)
                    
                    # 2. Tenta descobrir o total de registros da tabela principal para calcular %
                    # Lógica: pega a primeira palavra após 'FROM'
                    sql_lower = regra['sql'].lower()
                    tabela_base = sql_lower.split('from')[1].strip().split(' ')[0]
                    # Remove quebras de linha se houver
                    tabela_base = tabela_base.split('\n')[0]
                    
                    try:
                         total_registros = connection.execute(text(f'SELECT COUNT(*) FROM {tabela_base}')).scalar()
                    except:
                         total_registros = 0 # Fallback se falhar a contagem
                    
                    # 3. Calcula % de conformidade
                    if total_registros > 0:
                        percentual = ((total_registros - qtd_falhas) / total_registros) * 100
                    else:
                        percentual = 100.0

                    # 4. Prepara exemplos (JSON)
                    exemplos = df_falhas.head(20).to_dict(orient='records')

                    # 5. Salva o resultado no banco
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

        return {'estado': 'CONCLUÍDO', 'mensagem': 'Análises Relacionais concluídas.'}

    except Exception as e:
        logger.error(f"Erro Geral Relacional: {e}", exc_info=True)
        return {'estado': 'FALHOU', 'mensagem': str(e)}


@shared_task(bind=True)
def executar_metricas_avancadas_task(self):
    """
    Calcula o Histograma de Senhas e o DQI (Data Quality Index).
    """
    logger.info("[AVANÇADO] Iniciando métricas especiais...")
    
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
    engine = create_engine(db_url)

    try:
        # 1. ANÁLISE DE RISCO DE SENHA (Histograma)
        with engine.connect() as connection:
            sql_senha = text("""
                SELECT 
                    COUNT(*) FILTER (WHERE "pwdLastSet" >= NOW() - INTERVAL '90 days') as verde,
                    COUNT(*) FILTER (WHERE "pwdLastSet" < NOW() - INTERVAL '90 days' AND "pwdLastSet" >= NOW() - INTERVAL '180 days') as amarela,
                    COUNT(*) FILTER (WHERE "pwdLastSet" < NOW() - INTERVAL '180 days' AND "pwdLastSet" >= NOW() - INTERVAL '1 year') as vermelha,
                    COUNT(*) FILTER (WHERE "pwdLastSet" < NOW() - INTERVAL '1 year') as critica,
                    COUNT(*) as total
                FROM ad_users
                WHERE "userAccountControl" IS NOT NULL AND (CAST("userAccountControl" AS INTEGER) & 2) = 0 
            """)
            result = connection.execute(sql_senha).fetchone()
            
            RelatorioRiscoSenha.objects.create(
                faixa_verde_90dias=result[0],
                faixa_amarela_180dias=result[1],
                faixa_vermelha_1ano=result[2],
                faixa_critica_velha=result[3],
                total_contas=result[4]
            )
            logger.info("[AVANÇADO] Histograma de senhas gerado.")

        # 2. CÁLCULO DO DQI (Data Quality Index)
        # Busca os últimos resultados disponíveis de cada dimensão
        
        # A. Completude
        last_comp = RelatorioCompletudeGeral.objects.last()
        nota_completude = last_comp.percentual_completude_geral if last_comp else 0
        
        # B. Validade
        last_val = RelatorioValidadeFormato.objects.last()
        nota_validade = last_val.percentual_validade if last_val else 0
        
        # C. Unicidade
        last_uni = RelatorioUnicidadeGeral.objects.filter(tabela_analisada='ad_users').last()
        nota_unicidade = last_uni.media_unicidade if last_uni else 0
        
        # D. Consistência (Média das últimas 20 regras executadas)
        regras_recentes = RelatorioAnaliseRelacional.objects.all().order_by('-timestamp_inicio')[:20]
        if regras_recentes:
            soma_consistencia = sum([r.percentual_consistencia for r in regras_recentes])
            nota_consistencia = soma_consistencia / len(regras_recentes)
        else:
            nota_consistencia = 0

        # E. Fórmula Final Ponderada
        dqi_final = (0.3 * nota_completude) + (0.3 * nota_consistencia) + (0.2 * nota_validade) + (0.2 * nota_unicidade)

        RelatorioDQI.objects.create(
            score_total=dqi_final,
            score_completude=nota_completude,
            score_consistencia=nota_consistencia,
            score_validade=nota_validade,
            score_unicidade=nota_unicidade
        )
        logger.info(f"[AVANÇADO] DQI Calculado: {dqi_final:.2f}")

        return {'estado': 'CONCLUÍDO', 'mensagem': f'Métricas Avançadas e DQI ({dqi_final:.1f}) calculados.'}

    except Exception as e:
        logger.error(f"Erro DQI: {e}", exc_info=True)
        return {'estado': 'FALHOU', 'mensagem': str(e)}