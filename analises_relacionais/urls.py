from django.urls import path
from . import views

urlpatterns = [
    # A rota principal do painel relacional
    path('analises-relacionais/', views.dashboard_relacional, name='dashboard_relacional'),
    # A rota para os detalhes
    path('analises-relacionais/detalhe/<int:pk>/', views.detalhe_relatorio_relacional, name='detalhe_relatorio_relacional'),
]