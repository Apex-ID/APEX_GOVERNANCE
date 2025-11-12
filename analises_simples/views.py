# analises_simples/views.py
    
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from .models import RelatorioCompletude, RelatorioCompletudeGeral, RelatorioValidadeFormato
from .tasks import executar_analise_completude_task, executar_analise_completude_geral_task, executar_analise_validade_formato_task

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

        elif 'acao_analise_validade' in request.POST: 
            executar_analise_validade_formato_task.delay()
            messages.success(request, 'A ANÁLISE DE VALIDADE DE FORMATO foi iniciada!')

        return redirect('dashboard_analises')

    ultimos_relatorios_usuarios = RelatorioCompletude.objects.all().order_by('-timestamp_execucao')[:5]
    ultimos_relatorios_gerais = RelatorioCompletudeGeral.objects.all().order_by('-timestamp_inicio')[:5]
    ultimos_relatorios_validade = RelatorioValidadeFormato.objects.all().order_by('-timestamp_execucao')[:4]

    context = {
        'titulo': 'APEX - Dashboard de Métricas de Qualidade',
        'ultimos_relatorios_usuarios': ultimos_relatorios_usuarios, 
        'ultimos_relatorios_gerais': ultimos_relatorios_gerais,
        'ultimos_relatorios_validade': ultimos_relatorios_validade
    }
    return render(request, 'analises_simples/dashboard_analises.html', context)
    

def detalhe_relatorio_geral(request, pk):
    """
    View que mostra uma página de "consulta" detalhada para um
    único Relatório de Completude Geral.
    """
    relatorio = get_object_or_404(RelatorioCompletudeGeral, pk=pk)
    
    # Processa os dados das colunas vazias para o template
    colunas_vazias_processadas = []
    if relatorio.relatorio_colunas_vazias:
        for coluna, contagem in relatorio.relatorio_colunas_vazias.items():
            # Calcula o percentual de incompletude para esta coluna
            percentual = (contagem / relatorio.total_registros) * 100
            colunas_vazias_processadas.append({
                'nome': coluna,
                'contagem': contagem,
                'percentual': percentual
            })
            
    context = {
        'titulo': f"Relatório Detalhado: {relatorio.tabela_analisada}",
        'relatorio': relatorio,
        'colunas_vazias': colunas_vazias_processadas
    }
    return render(request, 'analises_simples/detalhe_relatorio.html', context)
    

def detalhe_relatorio_validade(request, pk):
    """
    View que mostra uma página de "consulta" detalhada para um
    único Relatório de Validade de Formato.
    """
    relatorio = get_object_or_404(RelatorioValidadeFormato, pk=pk)
    
    colunas_com_erros = []
    if relatorio.detalhamento_erros:
        colunas_com_erros = relatorio.detalhamento_erros.items()

    context = {
        'titulo': f"Relatório Detalhado de Validade: {relatorio.tabela_analisada}",
        'relatorio': relatorio,
        'colunas_com_erros': colunas_com_erros
    }
    return render(request, 'analises_simples/detalhe_relatorio_validade.html', context)