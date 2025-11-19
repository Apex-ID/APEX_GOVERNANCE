"""
URL configuration for apex_project project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    # URLs do Pipeline (Carga)
    path('', include('qualidade_ad.urls')),
    # URLs de Análises Simples (Completude, Validade, Unicidade)
    path('', include('analises_simples.urls')),
    # URLs de Análises Relacionais (Acurácia, Consistência)
    path('', include('analises_relacionais.urls')),
    # URLs do Importador Dinâmico(Importação de CSV para Banco de Dados)
    path('', include('importador_dinamico.urls')),
]
