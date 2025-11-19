from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from .models import RelatorioAnaliseRelacional
# Certifique-se de que a tarefa existe em tasks.py ou comente a linha abaixo temporariamente
from .tasks import executar_analises_relacionais_task

def dashboard_relacional(request):
    """
    View do painel de análises relacionais.
    """
    if request.method == 'POST':
        # Dispara a tarefa no Celery
        executar_analises_relacionais_task.delay()
        messages.success(request, 'Análises de Integridade e Consistência iniciadas!')
        return redirect('dashboard_relacional')

    # Busca histórico
    ultimos_relatorios = RelatorioAnaliseRelacional.objects.all().order_by('-timestamp_inicio')[:100]

    context = {
        'titulo': 'APEX - Análises Relacionais (Cruzamento)',
        'ultimos_relatorios': ultimos_relatorios
    }
    return render(request, 'analises_relacionais/dashboard_relacional.html', context)

def detalhe_relatorio_relacional(request, pk):
    relatorio = get_object_or_404(RelatorioAnaliseRelacional, pk=pk)
    status_classe = "status-sucesso" if relatorio.total_inconsistencias == 0 else "status-falhou"
    
    context = {
        'titulo': f"Detalhe: {relatorio.nome_analise}",
        'relatorio': relatorio,
        'status_classe': status_classe
    }
    return render(request, 'analises_relacionais/detalhe_relatorio_relacional.html', context)