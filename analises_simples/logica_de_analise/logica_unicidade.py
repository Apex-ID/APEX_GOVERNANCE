import pandas as pd
import logging

logger = logging.getLogger(__name__)

def analisar_unicidade_coluna(coluna_series: pd.Series, coluna_nome: str, tabela_nome: str):
    """
    Calcula as métricas de unicidade para uma única coluna.
    Aplica regras de negócio especiais (como sufixo de patrimônio).
    """
    # 1. Limpeza
    coluna_series.replace('', pd.NA, inplace=True)
    
    # 2. Contexto
    total_registros = len(coluna_series)
    registros_vazios = coluna_series.isnull().sum()
    dados_preenchidos = coluna_series.dropna()
    registros_preenchidos = len(dados_preenchidos)
    
    # 3. Lógica Especial (Computadores) vs Padrão
    e_coluna_computador = 'computers' in tabela_nome and (coluna_nome == 'cn' or coluna_nome == 'sAMAccountName')
    
    if e_coluna_computador:
        # Regra de Negócio: Unicidade pelo Patrimônio (Sufixo)
        # Ex: DCOMP-123$ -> 123
        dados_para_checar = dados_preenchidos.str.rstrip('$')
        sufixos = dados_para_checar.str.split('-').str.get(-1)
        registros_unicos = sufixos.nunique()
    else:
        # Lógica Padrão
        registros_unicos = dados_preenchidos.nunique()

    # 4. Fórmula
    percentual_unicidade = (registros_unicos / registros_preenchidos) * 100 if registros_preenchidos > 0 else 100.0

    return {
        'total_registros': int(total_registros),
        'registros_vazios': int(registros_vazios),
        'registros_preenchidos': int(registros_preenchidos),
        'registros_unicos_preenchidos': int(registros_unicos),
        'percentual_unicidade': percentual_unicidade
    }

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

    colunas_ignorar = ['id'] 

    for coluna_nome in df.columns:
        if coluna_nome in colunas_ignorar:
            continue
            
        colunas_processadas += 1
        coluna_series = df[coluna_nome]
        
        # Reutiliza a lógica da função individual para manter consistência
        resultado_coluna = analisar_unicidade_coluna(coluna_series, coluna_nome, tabela_nome)
        
        percentual = resultado_coluna['percentual_unicidade']
        duplicatas = resultado_coluna['registros_preenchidos'] - resultado_coluna['registros_unicos_preenchidos']
        
        soma_unicidades += percentual
        if duplicatas > 0:
            colunas_com_duplicatas += 1
            
        resultados_colunas[coluna_nome] = {
            'preenchidos': resultado_coluna['registros_preenchidos'],
            'unicos': resultado_coluna['registros_unicos_preenchidos'],
            'duplicatas': int(duplicatas),
            'percentual': round(percentual, 2)
        }

    media_unicidade = (soma_unicidades / colunas_processadas) if colunas_processadas > 0 else 0

    return {
        'total_registros': total_registros_tabela,
        'total_colunas_analisadas': colunas_processadas,
        'media_unicidade': media_unicidade,
        'qtd_colunas_com_duplicatas': colunas_com_duplicatas,
        'detalhe_por_coluna': resultados_colunas
    }

# --- NOVA FUNÇÃO MULTICOLUNA ---
def analisar_unicidade_multicoluna(df: pd.DataFrame, colunas_selecionadas: list):
    """
    Verifica a unicidade considerando a COMBINAÇÃO de várias colunas.
    Uma linha só é duplicada se TODOS os valores das colunas selecionadas forem iguais.
    """
    
    # 1. Filtra o DF apenas com as colunas desejadas
    # .copy() para evitar warnings de SettingWithCopy
    df_reduzido = df[colunas_selecionadas].copy()
    
    # 2. Limpeza: Substitui string vazia por texto explícito
    # Isso é necessário porque NaN != NaN em algumas comparações, mas queremos que
    # duas linhas com campos vazios sejam consideradas duplicatas.
    df_reduzido.replace('', '<VAZIO>', inplace=True)
    df_reduzido.fillna('<NULO>', inplace=True)
    
    total_registros = len(df_reduzido)
    
    if total_registros == 0:
        return {
            'total_registros': 0, 'registros_unicos': 0, 
            'registros_duplicados': 0, 'percentual_unicidade': 100.0,
            'exemplos': []
        }

    # 3. Encontra as duplicatas
    # subset=colunas_selecionadas: Usa a combinação das colunas como chave
    # keep=False: Marca TODAS as ocorrências de duplicatas como True (não salva a primeira)
    # Isso nos dá o total de linhas "envolvidas" em duplicação.
    duplicatas_mask = df_reduzido.duplicated(subset=colunas_selecionadas, keep=False)
    qtd_linhas_duplicadas = duplicatas_mask.sum()
    
    # Conta quantas combinações ÚNICAS existem no total (ex: 100 linhas, mas apenas 80 combinações distintas)
    qtd_combinacoes_unicas = len(df_reduzido.drop_duplicates(subset=colunas_selecionadas))

    # Fórmula: (Linhas Únicas / Total) * 100 ? 
    # Ou (Total - Linhas Duplicadas / Total) ?
    # DAMA geralmente foca em "registros únicos". 
    # Vamos usar: Percentual de registros que NÃO são duplicatas.
    registros_nao_duplicados = total_registros - qtd_linhas_duplicadas
    
    # Alternativa mais comum para "Percentual de Unicidade": (Valores Distintos / Total de Linhas)
    # Vamos usar (Combinações Únicas / Total de Registros) para ser consistente com o DAMA
    percentual_unicidade = (qtd_combinacoes_unicas / total_registros) * 100

    # 4. Extrai exemplos das duplicatas para o relatório
    exemplos = []
    if qtd_linhas_duplicadas > 0:
        # Agrupa pelas colunas selecionadas e conta o tamanho de cada grupo
        agrupado = df_reduzido[duplicatas_mask].groupby(colunas_selecionadas).size().reset_index(name='contagem')
        
        # Pega os top 20 casos com mais repetições
        top_duplicatas = agrupado.sort_values('contagem', ascending=False).head(20)
        
        for _, row in top_duplicatas.iterrows():
            # Cria um dicionário com os valores das colunas para mostrar no relatório
            valores = {col: row[col] for col in colunas_selecionadas}
            exemplos.append({
                'valores': valores,
                'repeticoes': int(row['contagem'])
            })

    return {
        'total_registros': int(total_registros),
        'registros_unicos': int(qtd_combinacoes_unicas), # Quantidade de combinações distintas
        'registros_duplicados': int(qtd_linhas_duplicadas), # Quantidade de linhas que são cópias
        'percentual_unicidade': percentual_unicidade,
        'exemplos': exemplos
    }