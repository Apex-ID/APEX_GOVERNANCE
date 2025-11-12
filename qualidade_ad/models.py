# qualidade_ad/models.py

from django.db import models

class ExecucaoPipeline(models.Model):
    """
    Representa uma única execução completa do pipeline.
    """
    STATUS_CHOICES = [
        ('INICIADO', 'Iniciado'),
        ('EM_PROGRESSO', 'Em Progresso'),
        ('CONCLUIDO', 'Concluído'),
        ('FALHOU', 'Falhou'),
    ]
    
    timestamp_inicio = models.DateTimeField(auto_now_add=True)
    timestamp_fim = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='INICIADO')
    
    def __str__(self):
        return f"Execução #{self.id} - {self.get_status_display()}"

class LogEtapa(models.Model):
    """
    Registra o resultado de uma etapa específica.
    """
    ETAPAS_CHOICES = [
        ('ETAPA_1_EXTRACAO', 'Etapa 1: Extração AD'),
        ('ETAPA_2_LIMPEZA', 'Etapa 2: Limpeza de CSVs'),
        ('ETAPA_3_PREPARACAO_BANCO', 'Etapa 3: Preparação do Banco'),
        ('ETAPA_4_CARGA_STAGING', 'Etapa 4: Carga para Staging'),
        ('ETAPA_5_TRANSFORMACAO', 'Etapa 5: Transformação (Produção)'),
        ('ETAPA_DESCONHECIDA', 'Etapa Desconhecida'),
    ]
    
    STATUS_CHOICES = [
        ('SUCESSO', 'Sucesso'),
        ('FALHOU', 'Falhou'),
    ]

    execucao = models.ForeignKey(ExecucaoPipeline, related_name='logs_etapas', on_delete=models.CASCADE)
    etapa_nome = models.CharField(max_length=50, choices=ETAPAS_CHOICES)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    timestamp_inicio = models.DateTimeField()
    timestamp_fim = models.DateTimeField()
    resumo_execucao = models.TextField(blank=True, null=True)
    
    def __str__(self):
        return f"Log {self.etapa_nome} (Execução #{self.execucao.id}) - {self.status}"