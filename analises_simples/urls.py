#  analises_simples/urls.py

from django.urls import path
from . import views

urlpatterns = [
    path('analises/', views.dashboard_analises, name='dashboard_analises'),
    path('analises/relatorio-geral/<int:pk>/', views.detalhe_relatorio_geral, name='detalhe_relatorio_geral'),
]