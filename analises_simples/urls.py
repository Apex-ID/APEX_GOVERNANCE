#  analises_simples/urls.py

from django.urls import path
from . import views

urlpatterns = [
    path('analises/', views.dashboard_analises, name='dashboard_analises'),
    path('analises/relatorio-geral/<int:pk>/', views.detalhe_relatorio_geral, name='detalhe_relatorio_geral'),
    path('analises/relatorio-validade/<int:pk>/', views.detalhe_relatorio_validade, name='detalhe_relatorio_validade'),
    path('analises/relatorio-unicidade/<int:pk>/', views.detalhe_relatorio_unicidade, name='detalhe_relatorio_unicidade'),
]