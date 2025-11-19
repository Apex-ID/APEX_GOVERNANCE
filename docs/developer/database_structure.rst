Estrutura do Banco de Dados
===========================

O APEX GOVERNANCE utiliza uma arquitetura de banco de dados dual para garantir a integridade dos dados durante o processo de ETL.

Camada 1: Staging (Dados Brutos)
--------------------------------
As tabelas de staging possuem o sufixo ``_staging``.
* **Característica:** Todas as colunas são do tipo ``TEXT``.
* **Objetivo:** Receber os dados exatamente como vêm do CSV/LDAP, sem risco de falha por tipo de dado inválido.
* **Chave Primária:** Um ``id`` sequencial (SERIAL) gerado pelo banco.

Exemplo: ``ad_users_staging``, ``ad_computers_staging``.

Camada 2: Produção (Dados Tipados)
----------------------------------
As tabelas finais possuem os tipos de dados corretos (PostgreSQL).
* **Característica:** Colunas tipadas (``TIMESTAMPTZ``, ``UUID``, ``INTEGER``, ``BOOLEAN``).
* **Objetivo:** Servir de base para as análises de qualidade e regras de negócio.
* **Processo:** Os dados são movidos da Staging para cá através da Etapa 5 do Pipeline.

Tabelas de Métricas e Logs
--------------------------
Além dos dados do AD, o sistema mantém tabelas de metadados:

* ``ExecucaoPipeline``: Histórico mestre de cada rodada do ETL.
* ``LogEtapa``: Detalhes de sucesso/erro de cada passo (Extração, Limpeza, etc.).
* ``RelatorioCompletude``, ``RelatorioUnicidade``, ``RelatorioRegraNegocio``: Tabelas analíticas onde os resultados das auditorias são persistidos.