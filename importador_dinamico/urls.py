from django.urls import path
from . import views

urlpatterns = [
    path('importar/upload/', views.upload_csv_view, name='upload_csv'),
    path('importar/configurar/', views.configurar_importacao_view, name='configurar_importacao'),
]