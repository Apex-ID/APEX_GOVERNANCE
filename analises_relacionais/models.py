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
    exemplos_inconsistencias = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"{self.nome_analise} ({self.timestamp_inicio.strftime('%Y-%m-%d %H:%M')})"


class RelatorioDQI(models.Model):
    """
    Data Quality Index - O Score Final.
    """
    timestamp = models.DateTimeField(auto_now_add=True)
    score_total = models.FloatField() # 0 a 100
    
    # Componentes do Score
    score_completude = models.FloatField()
    score_validade = models.FloatField()
    score_unicidade = models.FloatField()
    score_consistencia = models.FloatField()

    def __str__(self):
        return f"DQI: {self.score_total:.1f} ({self.timestamp})"

    # --- PROPRIEDADE DE COR (Para o Template HTML) ---
    @property
    def cor_status(self):
        """Retorna a cor Hex baseada no score para usar no CSS."""
        if self.score_total > 80:
            return '#28a745' # Verde
        elif self.score_total > 50:
            return '#ffc107' # Amarelo
        else:
            return '#dc3545' # Vermelho


class RelatorioRiscoSenha(models.Model):
    """
    Histograma de idade da senha (Password Age).
    """
    timestamp = models.DateTimeField(auto_now_add=True)
    
    # Buckets (Faixas)
    total_contas = models.IntegerField()
    faixa_verde_90dias = models.IntegerField()   # < 90 dias (Bom)
    faixa_amarela_180dias = models.IntegerField() # 90-180 dias (Atenção)
    faixa_vermelha_1ano = models.IntegerField()   # 180-365 dias (Ruim)
    faixa_critica_velha = models.IntegerField()   # > 1 ano (Crítico)

    def __str__(self):
        return f"Risco Senha ({self.timestamp})"

    # --- CÁLCULOS DE PORCENTAGEM (Para o Gráfico de Barras) ---
    
    def _calcular_porcentagem(self, valor):
        """Função auxiliar para evitar divisão por zero."""
        if self.total_contas and self.total_contas > 0:
            return (valor / self.total_contas) * 100
        return 0

    @property
    def perc_verde(self):
        return self._calcular_porcentagem(self.faixa_verde_90dias)

    @property
    def perc_amarela(self):
        return self._calcular_porcentagem(self.faixa_amarela_180dias)

    @property
    def perc_vermelha(self):
        return self._calcular_porcentagem(self.faixa_vermelha_1ano)

    @property
    def perc_critica(self):
        return self._calcular_porcentagem(self.faixa_critica_velha)