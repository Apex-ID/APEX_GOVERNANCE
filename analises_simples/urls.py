#  analises_simples/urls.py

from django.urls import path
from . import views

urlpatterns = [
    path('analises/', views.dashboard_analises, name='dashboard_analises'),
    path('analises/relatorio-geral/<int:pk>/', views.detalhe_relatorio_geral, name='detalhe_relatorio_geral'),
    path('analises/relatorio-validade/<int:pk>/', views.detalhe_relatorio_validade, name='detalhe_relatorio_validade'),
    path('analises/relatorio-unicidade/<int:pk>/', views.detalhe_relatorio_unicidade, name='detalhe_relatorio_unicidade'),
    path('analises/relatorio-personalizado/<int:pk>/', views.detalhe_relatorio_personalizado, name='detalhe_relatorio_personalizado'),
    path('analises/relatorio-regras/<int:pk>/', views.detalhe_relatorio_regras, name='detalhe_relatorio_regras'),
    path('analises/configurar-unicidade/', views.configuracao_unicidade, name='configuracao_unicidade'),
    path('analises/relatorio-personalizado/<int:pk>/', views.detalhe_relatorio_personalizado, name='detalhe_relatorio_personalizado'),
]