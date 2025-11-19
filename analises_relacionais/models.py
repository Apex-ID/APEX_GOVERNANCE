# analises_relacionais/models.py

from django.db import models

class RelatorioAnaliseRelacional(models.Model):
    """
    Armazena resultados de análises que cruzam múltiplas tabelas
    (Consistência de Integridade e Acurácia).
    """
    timestamp_inicio = models.DateTimeField(auto_now_add=True)
    nome_analise = models.CharField(max_length=200)  # Ex: "Gerentes Inválidos"
    tabelas_envolvidas = models.CharField(max_length=200) # Ex: "ad_users x ad_users"
    
    # Métricas
    total_registros_analisados = models.BigIntegerField()
    total_inconsistencias = models.BigIntegerField()
    percentual_consistencia = models.FloatField()
    
    # Contexto
    descricao_impacto = models.TextField() # O "Porquê"
    
    # JSON para guardar até 20 exemplos das falhas
    # Ex: [{'origem': 'Sergio', 'detalhe': 'Gerente Jose não existe'}, ...]
    exemplos_inconsistencias = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"{self.nome_analise} ({self.timestamp_inicio.strftime('%Y-%m-%d %H:%M')})"