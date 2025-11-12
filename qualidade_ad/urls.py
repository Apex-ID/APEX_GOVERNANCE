from django.urls import path
from . import views

urlpatterns = [
    path('painel/', views.painel_de_controle, name='painel_de_controle'),
]