APEX_GOVERNANCESistema de Governança e Qualidade de Dados do Active Directory

Este projeto é um sistema web completo, construído em Django, projetado para automatizar a extração, transformação e carga (ETL) de dados do ActiveD Directory (AD) da UFS. O objetivo final é criar e manter um repositório analítico em PostgreSQL, permitindo o monitoramento contínuo da qualidade dos dados e servindo como uma plataforma para tomada de decisão e governança de identidades.O sistema utiliza Celery para o processamento assíncrono (em segundo plano) das tarefas pesadas de ETL, garantindo que a interface web permaneça rápida e responsiva.

🚀 Status Atual do Projeto (Novembro de 2025)Infraestrutura (100% Funcional): A arquitetura base do sistema está completa e operacional no ambiente Linux. A comunicação entre Django (Web), Celery (Tarefas), Redis (Mensageria) e PostgreSQL (Banco de Dados) foi validada com sucesso.Pipeline de ETL (50% Concluído):

✅ Etapa 1: Extração (Real): A lógica de extração do AD (ad_extractor_and_reporter.py) foi refatorada e integrada ao Celery.

✅ Etapa 2: Limpeza (Real): A lógica de limpeza de CSVs (clean_and_report_csv.py) foi refatorada e integrada ao Celery.

⏳ Etapa 3: Carga Staging (Pendente): A tarefa Celery ainda está usando uma simulação (time.sleep).

⏳ Etapa 4: Transformação (Pendente): A tarefa Celery ainda está usando uma simulação (time.sleep).
Interface (MVP Funcional): Existe uma página de "Painel de Controle" que permite acionar manualmente o pipeline de ETL completo através de um botão.

🛠️ Arquitetura e Tecnologias
Componente                     Tecnologia / Biblioteca                      Propósito 
Framework Web                     Django                               Fornece a interface do usuário, login, painéis e APIs.
Processamento Assíncrono          Celery                               Executa as tarefas pesadas de ETL em segundo plano.
Agendamento                       Celery Beat                          Agenda a execução automática do pipeline (ex: diariamente).
Banco de Dados (App)              PostgreSQL                           Armazena os dados do Django (usuários, logs) e o repositórioanalítico.
Mensageria (Broker)               Redis                                Fila de comunicação entre o Django e os "workers" do Celery.
Análise de Dados                  Pandas                               Utilizado nas etapas de limpeza e transformação dos dados.
Conexão                           ADldap3                              Biblioteca Python para conexão e extração de dados do ActiveDirectory.Ambiente de Dev                   Linux (Ubuntu/WSL)                   Ambiente de desenvolvimento e produção padrão.

🐧 Guia de Instalação e Configuração (Ambiente Linux)

Siga estes passos para configurar um novo ambiente de desenvolvimento do zero.

1. Instalar Dependências do Sistema (Ubuntu/Debian)
Atualize seu sistema e instale as ferramentas essenciais:
    sudo apt update
    sudo apt install python3-pip python3-venv redis-server -y

2. Iniciar o Serviço Redis
Inicie o Redis e habilite-o para iniciar com o sistema:
    sudo service redis-server start
# Opcional: verifique se está rodando
    sudo service redis-server status

3. Configurar o Ambiente Python
Navegue até a pasta do projeto (/mnt/d/Sergio/Documents/GITHUB/APEX_GOVERNANCE/) e crie o ambiente virtual:
# 1. Criar o ambiente virtual
    python3 -m venv apexvirtual
# 2. Ativar o ambiente
    source apexvirtual/bin/activate
(Seu terminal deve agora mostrar (apexvirtual) no início)

4. Instalar Dependências do Python
Primeiro, crie o arquivo requirements.txt se ele não existir:
# Crie o arquivo (use 'nano' ou seu editor)
    nano requirements.txt
#Cole este conteúdo dentro dele:Plaintextdjango
    celery
    redis
    celery[redis]
    python-dotenv
    psycopg2-binary
    pandas
    ldap3
    sqlalchemy
    django-redis

Salve e feche o nano (Ctrl+O, Enter, Ctrl+X).Agora, instale as bibliotecas usando o pip de dentro do ambiente virtual (para evitar erros do  PEP 668):
    python3 -m pip install -r requirements.txt

5. Configurar o Arquivo de Credenciais (.env)
Crie o arquivo .env na raiz do projeto:
    nano .env
Cole e preencha o seguinte modelo. IMPORTANTE: Gere uma nova SECRET_KEY!
# Gere uma nova chave em https://djecrety.ir/
    SECRET_KEY='sua_chave_secreta_aqui'

# --- PostgreSQL Database Credentials ---
    DB_HOST="IP_do HOST"
    DB_NAME="apex_db"
    DB_USER="apex_user"
    DB_PASS="sua_senha_do_banco_sem_acento"
    DB_PORT="5432"

# --- Active Directory Credentials ---
    AD_SERVER="IP_ARCTIVE_DIRECTORY"
    AD_USER="ufs.internal\seu_usuario_de_servico"
    AD_PASSWORD="sua_senha_do_ad"
    AD_SEARCH_BASE="DC=ufs,DC=internal"

6. Preparar o Banco de Dados Django
Execute o migrate para criar as tabelas do Django (usuários, sessões, etc.) no seu PostgreSQL:
    python3 manage.py migrate

Crie um superusuário para acessar a área administrativa (/admin/):
    python3 manage.py createsuperuser

🚀 Como Executar o Sistema em Desenvolvimento
Para rodar o sistema, você precisa de dois terminais abertos, ambos na raiz do projeto e com o ambiente virtual ativado. (O Redis já está rodando como um serviço).

Terminal 1: Iniciar o "Worker" do CeleryEste terminal processará as tarefas em segundo plano.
    (apexvirtual) $ python3 -m celery -A apex_project worker -l info

Aguarde até ver a mensagem celery@... ready. e a lista de tarefas, incluindo qualidade_ad.tasks.executar_pipeline_completo_task.

Terminal 2: Iniciar o Servidor Web DjangoEste terminal servirá as páginas web.
(apexvirtual) $ python3 manage.py runserver

Acesso
Com os dois serviços rodando, acesse o painel de controle no seu navegador:
    http://127.0.0.1:8000/painel/

Ao clicar no botão "Iniciar Execução...", você verá a atividade sendo registrada em tempo real no Terminal 1 (Celery).