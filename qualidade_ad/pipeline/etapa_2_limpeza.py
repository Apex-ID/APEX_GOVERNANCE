# qualidade_ad/pipeline/etapa_2_limpeza.py

import pandas as pd
import os
from datetime import datetime
import traceback
from django.utils import timezone
from qualidade_ad.models import LogEtapa
import logging

logger = logging.getLogger(__name__)

def executar_limpeza_csvs(execucao_id):
    """
    Executa a Etapa 2 (Limpeza de CSVs) e registra um LogEtapa
    no banco de dados com o resultado.
    """

    etapa_nome = 'ETAPA_2_LIMPEZA'
    timestamp_inicio = timezone.now()
    resumo_da_etapa = ""
    status_final = "SUCESSO"
    
    try:
        logger.info(f"  [Etapa 2] Iniciando limpeza dos CSVs (Execução ID: {execucao_id})...")
        
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        data_path = os.path.join(base_dir, 'temp_data')

        FILES_TO_PROCESS = [
            'ad_users.csv',
            'ad_computers.csv',
            'ad_groups.csv',
            'ad_ous.csv'
        ]
        REPORT_FILE = os.path.join(data_path, 'data_cleaning_report.txt')

        total_cells_trimmed = 0
        total_null_chars_removed = 0

        with open(REPORT_FILE, 'w', encoding='utf-8') as report:
            report.write("RELATÓRIO DE HIGIENIZAÇÃO DE DADOS CSV (PIPELINE)\n")
            report.write(f"Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            for original_file in FILES_TO_PROCESS:
                logger.info(f"    -> Processando arquivo: {original_file}")
                report.write(f"--- Análise do Arquivo: {original_file} ---\n")
                
                original_file_path = os.path.join(data_path, original_file)

                if not os.path.exists(original_file_path):
                    logger.warning(f"    AVISO: Arquivo não encontrado: {original_file_path}. Pulando...")
                    report.write("Status: ARQUIVO NÃO ENCONTRADO (Etapa 1 pode ter falhado)\n\n")
                    continue

                try:
                    df = pd.read_csv(original_file_path, dtype=str)
                    report.write(f"Total de Linhas Lidas: {len(df)}\n")
                except Exception as e:
                    logger.error(f"    ERRO: Não foi possível ler o arquivo {original_file_path}. Erro: {e}")
                    report.write(f"Status: ERRO DE LEITURA\nDetalhes: {e}\n\n")
                    continue
                
                cells_processed = 0
                cells_trimmed = 0
                null_chars_removed = 0

                def clean_cell(value):
                    nonlocal cells_processed, cells_trimmed, null_chars_removed
                    cells_processed += 1
                    if not isinstance(value, str):
                        return value
                    original_value = value
                    cleaned_value = value
                    if '\x00' in cleaned_value:
                        cleaned_value = cleaned_value.replace('\x00', '')
                        if cleaned_value != original_value:
                            null_chars_removed += 1
                    trimmed_value = cleaned_value.strip()
                    if trimmed_value != cleaned_value:
                        cells_trimmed += 1
                    return trimmed_value

                logger.info("    -> Aplicando limpeza de dados...")
                df_cleaned = df.map(clean_cell)

                cleaned_file = original_file.replace('.csv', '_cleaned.csv')
                cleaned_file_path = os.path.join(data_path, cleaned_file)
                
                logger.info(f"    -> Salvando arquivo limpo como '{cleaned_file}'...")
                df_cleaned.to_csv(cleaned_file_path, index=False, encoding='utf-8-sig')
                
                report.write(f"  - Células com espaços removidos: {cells_trimmed}\n")
                report.write(f"  - Células com caracteres nulos removidos: {null_chars_removed}\n")
                report.write(f"  - Arquivo de saída gerado: {cleaned_file}\n\n")
                
                total_cells_trimmed += cells_trimmed
                total_null_chars_removed += null_chars_removed

        resumo_da_etapa = f"Limpeza concluída. {total_cells_trimmed} células aparadas, {total_null_chars_removed} caracteres nulos removidos."
        logger.info(f"  [Etapa 2] {resumo_da_etapa}")

    except Exception as e:
        status_final = "FALHOU"
        resumo_da_etapa = f"ERRO CRÍTICO: {e}\n{traceback.format_exc()}"
        logger.error(f"  [Etapa 2] ERRO: {resumo_da_etapa}")
        raise e
    
    finally:
        timestamp_fim = timezone.now()
        try:
            LogEtapa.objects.create(
                execucao_id=execucao_id, etapa_nome=etapa_nome, status=status_final,
                timestamp_inicio=timestamp_inicio, timestamp_fim=timestamp_fim,
                resumo_execucao=resumo_da_etapa
            )
            logger.info(f"  [Etapa 2] Log de execução salvo no banco de dados.")
        except Exception as db_error:
            logger.critical(f"  [Etapa 2] ERRO CRÍTICO AO SALVAR LOG: {db_error}")

    return resumo_da_etapa