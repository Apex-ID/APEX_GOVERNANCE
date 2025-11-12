# Em: analises_simples/logica_de_analise/logica_validade.py
# (Arquivo completo e corrigido com a nova lógica)

import pandas as pd
from uuid import UUID
from dateutil.parser import parse
import logging

logger = logging.getLogger(__name__)

# --- DICIONÁRIO DE DADOS ---
SCHEMA_ALVO = {
    'accountExpires': 'timestamp', 'adminCount': 'int', 'badPasswordTime': 'timestamp',
    'badPwdCount': 'int', 'codePage': 'int', 'countryCode': 'int', 'instanceType': 'int',
    'isCriticalSystemObject': 'bool', 'lastLogoff': 'timestamp', 'lastLogon': 'timestamp',
    'lastLogonTimestamp': 'timestamp', 'lockoutTime': 'timestamp', 'logonCount': 'int',
    'msDS-FailedInteractiveLogonCount': 'int',
    'msDS-FailedInteractiveLogonCountAtLastSuccessfulLogon': 'int',
    'msDS-LastFailedInteractiveLogonTime': 'timestamp',
    'msDS-LastSuccessfulInteractiveLogonTime': 'timestamp',
    'msDS-SupportedEncryptionTypes': 'int', 'msNPAllowDialin': 'bool',
    'objectGUID': 'uuid', 'primaryGroupID': 'int', 'pwdLastSet': 'timestamp',
    'sAMAccountType': 'int', 'showInAdvancedViewOnly': 'bool', 'uSNChanged': 'int',
    'uSNCreated': 'int', 'uidNumber': 'int', 'userAccountControl': 'int',
    'whenChanged': 'timestamp', 'whenCreated': 'timestamp',
    'localPolicyFlags': 'int', 'groupType': 'int', 'systemFlags': 'int',
}

# --- FUNÇÃO DE VALIDAÇÃO  ---
def validar_celula(valor, tipo_alvo):
    """
    Tenta converter um valor de texto para um tipo Python.
    Assume que o valor NÃO é nulo (já foi filtrado).
    Retorna True se for bem-sucedido, False se falhar.
    """
    try:
        if tipo_alvo == 'int':
            int(valor)
        elif tipo_alvo == 'timestamp':
            parse(valor)
        elif tipo_alvo == 'uuid':
            UUID(valor, version=4)
        elif tipo_alvo == 'bool':
            if str(valor).lower() not in ['true', 'false']:
                raise ValueError("Valor booleano inválido")
        return True
    except Exception:
        return False

# --- FUNÇÃO DE ANÁLISE  ---
def executar_analise_de_validade(df: pd.DataFrame):
    """
    Analisa um DataFrame (da tabela de staging) contra o SCHEMA_ALVO.
    Implementa a lógica de Validade DAMA (ignora células vazias).
    """
    # Limpeza inicial: Define '' (string vazia) como Nulo (NaN)
    df.replace('', pd.NA, inplace=True)
    
    total_celulas_invalidas = 0
    relatorio_erros_coluna = {}
    
    # 1. Conta o total de células vazias (para o relatório)
    total_celulas_vazias = df.isnull().sum().sum()
    
    # 2. Conta o total de células preenchidas (o denominador da fórmula)
    total_celulas_preenchidas = df.size - total_celulas_vazias

    # 3. Itera APENAS nas colunas que precisam de validação de formato
    colunas_para_validar = [col for col in df.columns if col in SCHEMA_ALVO]
    
    for coluna in colunas_para_validar:
        tipo_alvo = SCHEMA_ALVO[coluna]
            
        # Pega SÓ os dados preenchidos desta coluna para validar
        dados_preenchidos_da_coluna = df[coluna].dropna() 
        
        if not dados_preenchidos_da_coluna.empty:
            # Aplica a função de validação (que retorna False para erros)
            erros_na_coluna = dados_preenchidos_da_coluna.apply(
                lambda celula: not validar_celula(celula, tipo_alvo)
            ).sum()
            
            total_celulas_invalidas += erros_na_coluna
            
            if erros_na_coluna > 0:
                relatorio_erros_coluna[coluna] = int(erros_na_coluna) # Converte para int
        
    # 4. Cálculo final da fórmula DAMA
    total_celulas_validas = total_celulas_preenchidas - total_celulas_invalidas
    percentual_validade = (total_celulas_validas / total_celulas_preenchidas) * 100 if total_celulas_preenchidas > 0 else 100

    # 5. Retorna o dicionário completo
    return {
        'total_celulas_preenchidas': total_celulas_preenchidas,
        'total_celulas_invalidas': total_celulas_invalidas,
        'total_celulas_vazias': total_celulas_vazias,
        'percentual_validade': percentual_validade,
        'detalhamento_erros': relatorio_erros_coluna
    }