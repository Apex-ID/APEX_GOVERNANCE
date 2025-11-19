Configuração do Ambiente (Getting Started)
==========================================

Este guia descreve como configurar o ambiente de desenvolvimento do APEX GOVERNANCE no Linux (Ubuntu/WSL).

Pré-requisitos
--------------
* Python 3.12+
* Redis Server (Serviço de Mensageria)
* PostgreSQL (Banco de Dados)
* Git

Instalação Passo a Passo
------------------------

1. **Clone o Repositório:**

   .. code-block:: bash

      git clone https://github.com/seu-usuario/apex_governance.git
      cd apex_governance

2. **Crie o Ambiente Virtual:**

   .. code-block:: bash

      python3 -m venv apexvirtual
      source apexvirtual/bin/activate

3. **Instale as Dependências:**

   .. code-block:: bash

      pip install -r requirements.txt

4. **Configure as Variáveis de Ambiente:**
   Crie um arquivo ``.env`` na raiz do projeto com as credenciais do banco e do AD.

5. **Prepare o Banco de Dados:**

   .. code-block:: bash

      python3 manage.py migrate
      python3 manage.py createsuperuser

Executando o Sistema
--------------------

O sistema requer dois processos simultâneos em terminais separados:

**Terminal 1 (Celery Worker):**

.. code-block:: bash

   python3 -m celery -A apex_project worker -l info

**Terminal 2 (Servidor Web):**

.. code-block:: bash

   python3 manage.py runserver

Acesse o painel em: ``http://127.0.0.1:8000/painel/``