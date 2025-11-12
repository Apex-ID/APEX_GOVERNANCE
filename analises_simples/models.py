# analises_simples/models.py

from django.db import models

class RelatorioCompletude(models.Model):
    """
    Armazena o resultado (snapshot) de uma execução da análise
    de completude da tabela ad_users.
    """
    timestamp_execucao = models.DateTimeField(auto_now_add=True)
    
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
        return f"Relatório de Completude ({self.timestamp_execucao.strftime('%Y-%m-%d %H:%M')})"

class RelatorioCompletudeGeral(models.Model):
    """
    Armazena o resultado da análise de completude geral (nível de célula)
    para uma tabela de staging específica, conforme a fórmula do DAMA.
    """
    timestamp_inicio = models.DateTimeField(auto_now_add=True)
    timestamp_fim = models.DateTimeField(null=True, blank=True)
    
    # A fórmula DAMA: (preenchidas / total) * 100
    total_registros = models.BigIntegerField()
    total_colunas = models.BigIntegerField()
    total_celulas = models.BigIntegerField()
    total_celulas_preenchidas = models.BigIntegerField()
    percentual_completude_geral = models.FloatField()
    
    # Relatório de colunas com dados faltantes (em formato JSON)
    relatorio_colunas_vazias = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"Relatório Geral de {self.tabela_analisada} ({self.timestamp_execucao.strftime('%Y-%m-%d %H:%M')})"

    @property
    def duracao_processamento(self):
        if self.timestamp_fim and self.timestamp_inicio:
            return self.timestamp_fim - self.timestamp_inicio
        return None