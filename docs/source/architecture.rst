Diagrama de Sequência do Pipeline
---------------------------------

.. uml::

   actor Usuario
   participant "Django View" as V
   participant "Redis" as R
   participant "Celery Worker" as W
   database "PostgreSQL" as DB

   Usuario -> V: Clica em "Importar"
   V -> R: Envia Tarefa (Task)
   R -> W: Entrega Tarefa
   activate W
   W -> DB: Cria Tabela Staging
   W -> DB: Insere Dados
   W -> DB: Salva Log
   W --> R: Retorna Sucesso
   deactivate W