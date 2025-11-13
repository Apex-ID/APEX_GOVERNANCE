from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from .models import RelatorioCompletude, RelatorioCompletudeGeral, RelatorioValidadeFormato, RelatorioUnicidadeGeral # <-- NOVO MODEL
from .tasks import (
    executar_analise_completude_task, 
    executar_analise_completude_geral_task,
    executar_analise_validade_formato_task,
    executar_analise_unicidade_staging_task,
    executar_analise_unicidade_producao_task
)

def dashboard_analises(request):
    if request.method == 'POST':
        # (A lógica dos botões permanece a mesma, pois os nomes das tarefas não mudaram)
        if 'acao_analise_completude_usuarios' in request.POST:
            executar_analise_completude_task.delay()
            messages.success(request, 'A ANÁLISE DE COMPLETUDE (Usuários) foi iniciada!')
        elif 'acao_analise_completude_geral' in request.POST:
            executar_analise_completude_geral_task.delay()
            messages.success(request, 'A ANÁLISE GERAL DE COMPLETUDE (Staging) foi iniciada!')
        elif 'acao_analise_validade' in request.POST:
            executar_analise_validade_formato_task.delay()
            messages.success(request, 'A ANÁLISE DE VALIDADE DE FORMATO foi iniciada!')
        elif 'acao_analise_unicidade_staging' in request.POST:
            executar_analise_unicidade_staging_task.delay()
            messages.success(request, 'A ANÁLISE DE UNICIDADE (Staging) foi iniciada!')
        elif 'acao_analise_unicidade_producao' in request.POST:
            executar_analise_unicidade_producao_task.delay()
            messages.success(request, 'A ANÁLISE DE UNICIDADE (Produção) foi iniciada!')
        return redirect('dashboard_analises')

    ultimos_relatorios_usuarios = RelatorioCompletude.objects.all().order_by('-timestamp_inicio')[:5]
    ultimos_relatorios_gerais = RelatorioCompletudeGeral.objects.all().order_by('-timestamp_inicio')[:4]
    ultimos_relatorios_validade = RelatorioValidadeFormato.objects.all().order_by('-timestamp_inicio')[:4]
    
    # --- BUSCA OS NOVOS RELATÓRIOS GERAIS DE UNICIDADE ---
    ultimos_relatorios_unicidade_staging = RelatorioUnicidadeGeral.objects.filter(
        tabela_analisada__endswith='_staging'
    ).order_by('-timestamp_inicio')[:4] # 4 tabelas
    
    ultimos_relatorios_unicidade_producao = RelatorioUnicidadeGeral.objects.exclude(
        tabela_analisada__endswith='_staging'
    ).order_by('-timestamp_inicio')[:4] # 4 tabelas

    context = {
        'titulo': 'APEX - Dashboard de Análises Simples',
        'ultimos_relatorios_usuarios': ultimos_relatorios_usuarios, 
        'ultimos_relatorios_gerais': ultimos_relatorios_gerais,
        'ultimos_relatorios_validade': ultimos_relatorios_validade,
        'ultimos_relatorios_unicidade_staging': ultimos_relatorios_unicidade_staging,
        'ultimos_relatorios_unicidade_producao': ultimos_relatorios_unicidade_producao,
    }
    return render(request, 'analises_simples/dashboard_analises.html', context)

def detalhe_relatorio_geral(request, pk):
    relatorio = get_object_or_404(RelatorioCompletudeGeral, pk=pk)
    colunas_vazias = []
    if relatorio.relatorio_colunas_vazias:
        for col, count in relatorio.relatorio_colunas_vazias.items():
            perc = (count / relatorio.total_registros) * 100 if relatorio.total_registros > 0 else 0
            colunas_vazias.append({'nome': col, 'contagem': count, 'percentual': perc})
    return render(request, 'analises_simples/detalhe_relatorio.html', {'titulo': 'Detalhe Geral', 'relatorio': relatorio, 'colunas_vazias': colunas_vazias})

def detalhe_relatorio_validade(request, pk):
    relatorio = get_object_or_404(RelatorioValidadeFormato, pk=pk)
    erros = relatorio.detalhamento_erros.items() if relatorio.detalhamento_erros else []
    return render(request, 'analises_simples/detalhe_relatorio_validade.html', {'titulo': 'Detalhe Validade', 'relatorio': relatorio, 'colunas_com_erros': erros})

# --- NOVA VIEW DE DETALHE DE UNICIDADE ---
def detalhe_relatorio_unicidade(request, pk):
    relatorio = get_object_or_404(RelatorioUnicidadeGeral, pk=pk)
    
    # Prepara os dados do JSON para o template
    detalhes_colunas = []
    if relatorio.detalhe_por_coluna:
        for col_nome, stats in relatorio.detalhe_por_coluna.items():
            stats['nome'] = col_nome
            detalhes_colunas.append(stats)
            
    # Ordena: colunas com mais duplicatas primeiro
    detalhes_colunas.sort(key=lambda x: x['duplicatas'], reverse=True)

    context = {
        'titulo': f"Detalhe Unicidade: {relatorio.tabela_analisada}",
        'relatorio': relatorio,
        'detalhes_colunas': detalhes_colunas
    }
    return render(request, 'analises_simples/detalhe_relatorio_unicidade.html', context)