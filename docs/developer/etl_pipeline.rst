Pipeline de ETL (Carga de Dados)
================================

O coração do APEX GOVERNANCE é um pipeline de 5 estágios orquestrado pelo Celery.

Fluxo de Execução
-----------------

1. **Etapa 1: Extração (LDAP)**
   Conecta-se ao Active Directory, pagina os resultados e gera arquivos CSV brutos na pasta temporária.

2. **Etapa 2: Limpeza (Pandas)**
   Remove caracteres nulos, espaços em branco e linhas vazias dos CSVs.

3. **Etapa 3: Preparação do Banco**
   **Destrutiva.** Recria as tabelas de Staging e Produção para garantir um estado limpo (TRUNCATE/DROP).

4. **Etapa 4: Carga para Staging**
   Lê os CSVs limpos e insere em lote (bulk insert) nas tabelas ``_staging``.

5. **Etapa 5: Transformação e Tipagem**
   Executa funções SQL PL/pgSQL para mover dados de Staging para Produção.
   * **Tratamento de Erro:** Se uma linha falha na conversão de tipo (ex: data inválida), ela é capturada e salva na tabela ``etl_error_log``, permitindo que o restante do processo continue.

Diagrama de Sequência
---------------------

.. code-block:: text

   [User] -> [Django View] -> [Redis] -> [Celery Worker]
                                               |
                                       [Executa Etapas 1-5]
                                               |
                                          [PostgreSQL]