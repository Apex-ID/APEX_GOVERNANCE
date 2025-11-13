from django.db import models

# 1. Relatório de Completude Específica (Negócio) - TABELA PRODUÇÃO
class RelatorioCompletude(models.Model):
    timestamp_inicio = models.DateTimeField(auto_now_add=True)
    
    total_usuarios = models.IntegerField()
    sem_gerente = models.IntegerField()
    sem_departamento = models.IntegerField()
    sem_cargo = models.IntegerField()
    contato_completo = models.IntegerField()
    
    perc_sem_gerente = models.FloatField()
    perc_sem_departamento = models.FloatField()
    perc_sem_cargo = models.FloatField()
    perc_contato_completo = models.FloatField()

    def __str__(self):
        return f"Relatório Completude Usuários ({self.timestamp_inicio.strftime('%Y-%m-%d %H:%M')})"

# 2. Relatório de Completude Geral (Células Preenchidas) - TABELA STAGING
class RelatorioCompletudeGeral(models.Model):
    timestamp_inicio = models.DateTimeField(auto_now_add=True)
    timestamp_fim = models.DateTimeField(null=True, blank=True)
    tabela_analisada = models.CharField(max_length=100)
    
    total_registros = models.BigIntegerField()
    total_colunas = models.BigIntegerField()
    total_celulas = models.BigIntegerField()
    total_celulas_preenchidas = models.BigIntegerField()
    percentual_completude_geral = models.FloatField()
    
    relatorio_colunas_vazias = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"Relatório Geral {self.tabela_analisada} ({self.timestamp_inicio.strftime('%Y-%m-%d %H:%M')})"

# 3. Relatório de Validade de Formato - TABELA STAGING
class RelatorioValidadeFormato(models.Model):
    timestamp_inicio = models.DateTimeField(auto_now_add=True)
    tabela_analisada = models.CharField(max_length=100)
    
    total_celulas_preenchidas = models.BigIntegerField()
    total_celulas_invalidas = models.BigIntegerField()
    total_celulas_vazias = models.BigIntegerField()
    percentual_validade = models.FloatField()
    
    detalhamento_erros = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"Relatório Validade {self.tabela_analisada} ({self.timestamp_inicio.strftime('%Y-%m-%d %H:%M')})"

# 4. Relatório de Unicidade - TABELAS STAGING E PRODUÇÃO
class RelatorioUnicidade(models.Model):
    timestamp_inicio = models.DateTimeField(auto_now_add=True)
    tabela_analisada = models.CharField(max_length=100)
    coluna_analisada = models.CharField(max_length=100)
    
    total_registros = models.BigIntegerField()
    registros_vazios = models.BigIntegerField()
    
    registros_preenchidos = models.BigIntegerField()
    registros_unicos_preenchidos = models.BigIntegerField()
    percentual_unicidade = models.FloatField()

    def __str__(self):
        return f"Rel. Unicidade: {self.tabela_analisada}.{self.coluna_analisada}"

    @property
    def duplicatas_encontradas(self):
        if self.registros_preenchidos is not None and self.registros_unicos_preenchidos is not None:
            return self.registros_preenchidos - self.registros_unicos_preenchidos
        return 0

class RelatorioUnicidadeGeral(models.Model):
    """
    Armazena o resultado da análise de Unicidade para UMA TABELA INTEIRA.
    Substitui o RelatorioUnicidade antigo.
    """
    timestamp_inicio = models.DateTimeField(auto_now_add=True)
    tabela_analisada = models.CharField(max_length=100)
    
    # Métricas Gerais
    total_registros = models.BigIntegerField()
    total_colunas_analisadas = models.IntegerField()
    media_unicidade = models.FloatField()
    qtd_colunas_com_duplicatas = models.IntegerField()
    
    # JSON com o detalhe por coluna
    detalhe_por_coluna = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"Rel. Unicidade Geral: {self.tabela_analisada}"