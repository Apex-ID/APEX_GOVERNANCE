import pandas as pd
import logging

logger = logging.getLogger(__name__)

def analisar_unicidade_tabela_inteira(df: pd.DataFrame, tabela_nome: str):
    """
    Percorre TODAS as colunas de um DataFrame e calcula a unicidade para cada uma.
    Retorna um resumo geral e os detalhes por coluna.
    """
    
    resultados_colunas = {}
    soma_unicidades = 0
    colunas_com_duplicatas = 0
    colunas_processadas = 0
    total_registros_tabela = len(df)

    # Define quais colunas ignorar (ex: metadados internos do banco ou do processo ETL)
    colunas_ignorar = ['id'] 

    for coluna_nome in df.columns:
        if coluna_nome in colunas_ignorar:
            continue
            
        colunas_processadas += 1
        coluna_series = df[coluna_nome]
        
        # 1. Limpeza (Ignora vazios)
        coluna_series.replace('', pd.NA, inplace=True)
        dados_preenchidos = coluna_series.dropna()
        registros_preenchidos = len(dados_preenchidos)
        
        # Se a coluna estiver toda vazia, unicidade é considerada 100% (não há duplicatas)
        if registros_preenchidos == 0:
            percentual = 100.0
            registros_unicos = 0
            duplicatas = 0
        else:
            # 2. Lógica Especial (Computadores) vs Padrão
            e_coluna_computador = 'computers' in tabela_nome and (coluna_nome == 'cn' or coluna_nome == 'sAMAccountName')
            
            if e_coluna_computador:
                # Remove '$' e pega o sufixo após o último '-'
                dados_para_checar = dados_preenchidos.str.rstrip('$')
                # Ex: "UFS-TI-12345" -> "12345"
                sufixos = dados_para_checar.str.split('-').str.get(-1)
                registros_unicos = sufixos.nunique()
            else:
                registros_unicos = dados_preenchidos.nunique()
            
            # 3. Cálculos
            duplicatas = registros_preenchidos - registros_unicos
            percentual = (registros_unicos / registros_preenchidos) * 100
        
        # Acumula estatísticas
        soma_unicidades += percentual
        if duplicatas > 0:
            colunas_com_duplicatas += 1
            
        # Salva detalhe
        resultados_colunas[coluna_nome] = {
            'preenchidos': int(registros_preenchidos),
            'unicos': int(registros_unicos),
            'duplicatas': int(duplicatas),
            'percentual': round(percentual, 2)
        }

    # Média geral da tabela
    media_unicidade = (soma_unicidades / colunas_processadas) if colunas_processadas > 0 else 0

    return {
        'total_registros': total_registros_tabela,
        'total_colunas_analisadas': colunas_processadas,
        'media_unicidade': media_unicidade,
        'qtd_colunas_com_duplicatas': colunas_com_duplicatas,
        'detalhe_por_coluna': resultados_colunas
    }