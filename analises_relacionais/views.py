from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from .models import (
    RelatorioAnaliseRelacional, 
    RelatorioDQI, 
    RelatorioRiscoSenha
)
from .tasks import (
    executar_analises_relacionais_task, 
    executar_metricas_avancadas_task
)
# -----------------------------------
def dashboard_relacional(request):
    """
    View principal do painel relacional (Dashboard).
    """
    if request.method == 'POST':
        if 'acao_analise_consistencia' in request.POST:
            # Executa as regras normais E as avançadas
            executar_analises_relacionais_task.delay()
            executar_metricas_avancadas_task.delay()
            messages.success(request, 'Análises Relacionais e Cálculo de DQI iniciados!')
        return redirect('dashboard_relacional')

    # Histórico (limitado a 20)
    ultimos_relatorios = RelatorioAnaliseRelacional.objects.all().order_by('-timestamp_inicio')[:20]
    
    # KPIs
    ultimo_dqi = RelatorioDQI.objects.last()
    ultimo_risco = RelatorioRiscoSenha.objects.last()

    context = {
        'titulo': 'APEX - Governança Avançada (DQI & Riscos)',
        'ultimos_relatorios': ultimos_relatorios,
        'dqi': ultimo_dqi,
        'risco_senha': ultimo_risco
    }
    return render(request, 'analises_relacionais/dashboard_relacional.html', context)

def detalhe_relatorio_relacional(request, pk):
    """
    View de detalhe para um relatório específico.
    """
    relatorio = get_object_or_404(RelatorioAnaliseRelacional, pk=pk)
    
    # Define classe CSS baseada no sucesso
    status_classe = "status-sucesso" if relatorio.total_inconsistencias == 0 else "status-falhou"
    
    context = {
        'titulo': f"Detalhe: {relatorio.nome_analise}",
        'relatorio': relatorio,
        'status_classe': status_classe
    }
    return render(request, 'analises_relacionais/detalhe_relatorio_relacional.html', context)
# -----------------------------------