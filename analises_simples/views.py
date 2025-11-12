# analises_simples/views.py
    
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from .models import RelatorioCompletude, RelatorioCompletudeGeral
from .tasks import executar_analise_completude_task, executar_analise_completude_geral_task

def dashboard_analises(request):
    """
    View principal que mostra o painel de análises e permite
    acionar os cálculos de métricas.
    """
    
    if request.method == 'POST':
        if 'acao_analise_completude_usuarios' in request.POST:
            executar_analise_completude_task.delay()
            messages.success(request, 'A ANÁLISE DE COMPLETUDE (Usuários) foi iniciada!')
        
        elif 'acao_analise_completude_geral' in request.POST:
            executar_analise_completude_geral_task.delay()
            messages.success(request, 'A ANÁLISE GERAL DE COMPLETUDE (Staging) foi iniciada!')

        return redirect('dashboard_analises')

    ultimos_relatorios_usuarios = RelatorioCompletude.objects.all().order_by('-timestamp_execucao')[:5]
    ultimos_relatorios_gerais = RelatorioCompletudeGeral.objects.all().order_by('-timestamp_inicio')[:5]

    context = {
        'titulo': 'APEX - Dashboard de Métricas de Qualidade',
        'ultimos_relatorios_usuarios': ultimos_relatorios_usuarios, 
        'ultimos_relatorios_gerais': ultimos_relatorios_gerais
    }
    return render(request, 'analises_simples/dashboard_analises.html', context)

def detalhe_relatorio_geral(request, pk):
    """
    View que mostra uma página de "consulta" detalhada para um
    único Relatório de Completude Geral.
    """
    relatorio = get_object_or_404(RelatorioCompletudeGeral, pk=pk)
    colunas_vazias = relatorio.relatorio_colunas_vazias.items()
    
    context = {
        'titulo': f"Relatório Detalhado: {relatorio.tabela_analisada}",
        'relatorio': relatorio,
        'colunas_vazias': colunas_vazias
    }
    return render(request, 'analises_simples/detalhe_relatorio.html', context)