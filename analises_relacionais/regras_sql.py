# Em: analises_relacionais/regras_sql.py

"""
ARQUIVO MESTRE DE REGRAS DE NEGÓCIO (DAMA DMBOK)
------------------------------------------------
Este arquivo define as consultas SQL para auditar a qualidade dos dados
do Active Directory. As regras estão divididas em 5 dimensões.

Total de Regras: 28
"""

REGRAS_DE_NEGOCIO_SQL = [
    # ==============================================================================
    # GRUPO 1: COMPLETUDE (Dados Obrigatórios Faltando)
    # Objetivo: Identificar campos nulos que deveriam estar preenchidos.
    # ==============================================================================
    {
        'nome': '1. Contas de Usuário sem Gerente',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'impacto': 'Contas "órfãs" de responsabilidade. Risco crítico em revisões de acesso, aprovações e processos de desligamento.',
        'sql_filtro_falha': """
            ("manager" IS NULL OR "manager" = '')
        """
    },
    {
        'nome': '2. Contas de Usuário sem Departamento',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'impacto': 'Impede a identificação de custos por centro de custo (CC) e dificulta a aplicação de políticas de acesso baseadas em função (RBAC).',
        'sql_filtro_falha': """
            ("department" IS NULL OR "department" = '')
        """
    },
    {
        'nome': '3. Contas de Usuário sem Cargo (Title)',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'impacto': 'Impede a validação de "Segregação de Funções" (SoD) e a aplicação do princípio do privilégio mínimo.',
        'sql_filtro_falha': """
            ("title" IS NULL OR "title" = '')
        """
    },
    {
        'nome': '4. Usuário com Contato Incompleto (Email/Tel)',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'impacto': 'Dificulta a comunicação institucional, recuperação de senha e o suporte de TI. Exige E-mail E Telefone.',
        # Falha se faltar QUALQUER UM dos dois
        'sql_filtro_falha': """
            ("mail" IS NULL OR "mail" = '') 
            OR 
            ("telephoneNumber" IS NULL OR "telephoneNumber" = '')
        """
    },
    {
        'nome': '5. Computadores sem Dono (ManagedBy)',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_computers', 'ad_computers_staging'],
        'impacto': 'Ativo de TI sem responsável técnico ou administrativo. Risco de furto, perda ou falta de manutenção.',
        'sql_filtro_falha': """
            ("managedBy" IS NULL OR "managedBy" = '')
        """
    },
    {
        'nome': '6. Computadores sem Descrição',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_computers', 'ad_computers_staging'],
        'impacto': 'Falta de documentação sobre a finalidade técnica do ativo (ex: "Servidor de Arquivos" vs "PC do Laboratório").',
        'sql_filtro_falha': """
            ("description" IS NULL OR "description" = '')
        """
    },
    {
        'nome': '7. Grupos sem Dono (ManagedBy)',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_groups', 'ad_groups_staging'],
        'impacto': 'Grupos sem responsável acumulam membros indevidos ao longo do tempo e nunca são revisados (Attestation).',
        'sql_filtro_falha': """
            ("managedBy" IS NULL OR "managedBy" = '')
        """
    },
    {
        'nome': '8. Grupos sem Descrição',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_groups', 'ad_groups_staging'],
        'impacto': 'Impossível determinar a função do negócio do grupo apenas pelo nome, dificultando auditorias.',
        'sql_filtro_falha': """
            ("description" IS NULL OR "description" = '')
        """
    },
    {
        'nome': '9. OUs sem Descrição',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_ous', 'ad_ous_staging'],
        'impacto': 'Falta de documentação sobre o propósito da unidade organizacional na estrutura da empresa.',
        'sql_filtro_falha': """
            ("description" IS NULL OR "description" = '')
        """
    },
    {
        'nome': '10. OUs sem Políticas de Grupo (GPO)',
        'dimensao': 'Completude (Configuração)',
        'tabelas_alvo': ['ad_ous', 'ad_ous_staging'],
        'impacto': 'Risco de Conformidade. Objetos nesta OU podem estar "soltos", sem receber as políticas de segurança e rede.',
        'sql_filtro_falha': """
            ("gPLink" IS NULL OR "gPLink" = '')
        """
    },

    # ==============================================================================
    # GRUPO 2: TEMPORALIDADE (Dados Obsoletos / Higiene)
    # Objetivo: Identificar objetos que deveriam ser arquivados ou deletados.
    # ==============================================================================
    {
        'nome': '11. Usuários Inativos (>90 dias)',
        'dimensao': 'Temporalidade',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'impacto': 'Contas não utilizadas são os vetores de ataque preferidos para movimentação lateral silenciosa.',
        'sql_filtro_falha': """
            CAST(NULLIF("lastLogonTimestamp", '') AS TIMESTAMPTZ) < NOW() - INTERVAL '90 days'
        """
    },
    {
        'nome': '12. Usuários "Nunca Logados"',
        'dimensao': 'Temporalidade',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'impacto': 'Contas criadas e abandonadas. Falha no processo de admissão ou contas de teste esquecidas.',
        'sql_filtro_falha': """
            ("lastLogonTimestamp" IS NULL OR "lastLogonTimestamp" = '')
        """
    },
    {
        'nome': '13. Senha Expirada ou Antiga (>180 dias)',
        'dimensao': 'Temporalidade',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'impacto': 'Violação de política de segurança. Senhas antigas são vulneráveis a vazamentos de credenciais (dumps) antigos.',
        'sql_filtro_falha': """
            CAST(NULLIF("pwdLastSet", '') AS TIMESTAMPTZ) < NOW() - INTERVAL '180 days'
        """
    },
    {
        'nome': '14. Computadores Inativos (>90 dias)',
        'dimensao': 'Temporalidade',
        'tabelas_alvo': ['ad_computers', 'ad_computers_staging'],
        'impacto': 'Máquinas desconectadas não recebem patches de segurança, tornando-se pontos cegos na rede (Shadow IT).',
        'sql_filtro_falha': """
            CAST(NULLIF("lastLogonTimestamp", '') AS TIMESTAMPTZ) < NOW() - INTERVAL '90 days'
        """
    },

    # ==============================================================================
    # GRUPO 3: CONSISTÊNCIA E INTEGRIDADE (Regras Relacionais / Segurança)
    # Objetivo: Cruzar dados para validar lógica de negócio e segurança.
    # ==============================================================================
    {
        'nome': '15. Gerente Inválido (Link Quebrado)',
        'dimensao': 'Consistência (Integridade)',
        'tabelas': 'ad_users (JOIN ad_users)',
        'impacto': 'Inconsistência de dados. O campo manager aponta para um distinguishedName que não existe no AD.',
        'sql': """
            SELECT u."cn" as origem, 'Gerente inexistente: ' || u."manager" as detalhe
            FROM ad_users u
            LEFT JOIN ad_users m ON u."manager" = m."distinguishedName"
            WHERE u."manager" IS NOT NULL 
              AND u."manager" != '' 
              AND m."distinguishedName" IS NULL
        """
    },
    {
        'nome': '16. Gerente Desabilitado',
        'dimensao': 'Consistência (Hierarquia)',
        'tabelas': 'ad_users (JOIN ad_users)',
        'impacto': 'Processo de RH falho. Funcionários ativos reportam a gerentes que já foram desligados.',
        'sql': """
            SELECT u."cn" as origem, 'Gerente desabilitado: ' || m."cn" as detalhe
            FROM ad_users u
            JOIN ad_users m ON u."manager" = m."distinguishedName"
            WHERE (CAST(u."userAccountControl" AS INTEGER) & 2) = 0 -- Funcionario Ativo
              AND (CAST(m."userAccountControl" AS INTEGER) & 2) > 0 -- Gerente Desabilitado
        """
    },
    {
        'nome': '17. Contas Desabilitadas com Permissões Ativas',
        'dimensao': 'Consistência (Segurança)',
        'tabelas': 'ad_users',
        'impacto': 'Falha grave de offboarding. Se a conta for reativada, o usuário recupera acessos que não deveria ter.',
        'sql': """
            SELECT "cn" as origem, 'Conta desativada possui grupos' as detalhe
            FROM ad_users
            WHERE (CAST("userAccountControl" AS INTEGER) & 2) > 0 -- Conta Desabilitada
              AND "memberOf" IS NOT NULL 
              AND "memberOf" != ''
        """
    },
    {
        'nome': '18. Admin Count=1 (Privilégio Elevado)',
        'dimensao': 'Consistência (Segurança)',
        'tabelas': 'ad_users',
        'impacto': 'Contas críticas protegidas pelo sistema (AdminSDHolder). Requer monitoramento contínuo.',
        'sql': """
            SELECT "cn" as origem, 'Conta Protegida (AdminCount)' as detalhe
            FROM ad_users
            WHERE CAST("adminCount" AS INTEGER) = 1
        """
    },
    {
        'nome': '19. Computador Órfão (Dono Inválido)',
        'dimensao': 'Consistência (Integridade)',
        'tabelas': 'ad_computers (JOIN ad_users)',
        'impacto': 'Ativos de TI vinculados a usuários inexistentes. Dificulta responsabilização.',
        'sql': """
            SELECT c."cn" as origem, 'Dono inexistente: ' || c."managedBy" as detalhe
            FROM ad_computers c
            LEFT JOIN ad_users u ON c."managedBy" = u."distinguishedName"
            WHERE c."managedBy" IS NOT NULL 
              AND c."managedBy" != '' 
              AND u."distinguishedName" IS NULL
        """
    },
    {
        'nome': '20. Grupos Aninhados (Nested Groups)',
        'dimensao': 'Consistência (Complexidade)',
        'tabelas': 'ad_groups',
        'impacto': 'Dificulta o rastreamento efetivo de permissões e pode levar a escaladas de privilégio acidentais.',
        'sql': """
            SELECT "cn" as origem, 'Membro de outro grupo' as detalhe
            FROM ad_groups
            WHERE "memberOf" IS NOT NULL AND "memberOf" != ''
        """
    },
    {
        'nome': '21. Computadores no Container Padrão',
        'dimensao': 'Consistência (Organizacional)',
        'tabelas': 'ad_computers',
        'impacto': 'Máquinas fora da estrutura de OUs não recebem GPOs corretas. Devem ser movidas.',
        'sql': """
            SELECT "cn" as origem, 'Localização incorreta (CN=Computers)' as detalhe
            FROM ad_computers
            WHERE "distinguishedName" LIKE '%CN=Computers,%'
        """
    },
    {
        'nome': '22. Grupos "Admin" sem Proteção',
        'dimensao': 'Consistência (Segurança)',
        'tabelas': 'ad_groups',
        'impacto': 'Grupo nomeado como "Admin" mas não protegido pelo sistema. Possível Backdoor ou erro de nomenclatura.',
        'sql': """
            SELECT "cn" as origem, 'AdminCount 0 ou Nulo' as detalhe
            FROM ad_groups
            WHERE "cn" LIKE '%Admin%' 
              AND (CAST(NULLIF("adminCount", '') AS INTEGER) IS NULL OR CAST(NULLIF("adminCount", '') AS INTEGER) = 0)
        """
    },
    {
        'nome': '23. Grupos Vazios (Sem Membros)',
        'dimensao': 'Consistência',
        'tabelas': 'ad_groups',
        'impacto': 'Poluição do diretório. Grupos criados e nunca usados ou abandonados.',
        'sql': """
            SELECT "cn" as origem, 'Grupo vazio' as detalhe
            FROM ad_groups
            WHERE "member" IS NULL OR "member" = ''
        """
    },

    # ==============================================================================
    # GRUPO 4: VALIDADE (Conformidade de Formato e Padrões)
    # Objetivo: Verificar dados fora do padrão técnico ou obsoleto.
    # ==============================================================================
    {
        'nome': '24. UPN Fora do Padrão (@office.ufs.br)',
        'dimensao': 'Validade',
        'tabelas': 'ad_users',
        'impacto': 'Falha na autenticação federada com nuvem (Office 365, Azure AD, Google).',
        'sql': """
            SELECT "sAMAccountName" as origem, "userPrincipalName" as detalhe
            FROM ad_users
            WHERE "userPrincipalName" NOT LIKE '%@office.ufs.br'
        """
    },
    {
        'nome': '25. Logon Legado (SAM) Extenso (>20 chars)',
        'dimensao': 'Validade',
        'tabelas': 'ad_users',
        'impacto': 'Incompatibilidade com sistemas legados (pre-Windows 2000) que têm limite rígido de caracteres.',
        'sql': """
            SELECT "cn" as origem, "sAMAccountName" as detalhe
            FROM ad_users
            WHERE LENGTH("sAMAccountName") > 20
        """
    },
    {
        'nome': '26. Sistemas Operacionais Obsoletos (< 2015)',
        'dimensao': 'Validade (Obsolescência)',
        'tabelas_alvo': ['ad_computers', 'ad_computers_staging'],
        'impacto': 'Sistemas lançados antes de 2015. Geralmente sem suporte (EOL) e altamente vulneráveis.',
        # Busca por nomes legados (XP, Vista, 7) E por anos explícitos anteriores a 2015
        'sql_filtro_falha': """
            "operatingSystem" ILIKE '%Windows XP%' OR 
            "operatingSystem" ILIKE '%Vista%' OR 
            "operatingSystem" ILIKE '%Windows 7%' OR 
            "operatingSystem" ILIKE '%Windows 8%' OR
            "operatingSystem" ILIKE '%2000%' OR 
            "operatingSystem" ILIKE '%2003%' OR 
            "operatingSystem" ILIKE '%2008%' OR 
            "operatingSystem" ILIKE '%2012%'
        """
    },

    # ==============================================================================
    # GRUPO 5: ANÁLISES ESTRUTURAIS AVANÇADAS (SQL Recursivo/Complexo)
    # ==============================================================================
    {
        'nome': '27. Ciclos Hierárquicos (Loop de Gerência)',
        'dimensao': 'Consistência (Estrutural)',
        'tabelas': 'ad_users (Recursivo)',
        'impacto': 'Erro lógico grave. Quebra sistemas de organograma e aprovação (loop infinito).',
        'sql': """
            WITH RECURSIVE Hierarquia AS (
                SELECT "distinguishedName" as raiz, "manager", "distinguishedName" as atual, 1 as nivel
                FROM ad_users
                WHERE "manager" IS NOT NULL AND "manager" != ''
                UNION ALL
                SELECT h.raiz, u."manager", u."distinguishedName", h.nivel + 1
                FROM ad_users u
                INNER JOIN Hierarquia h ON u."distinguishedName" = h."manager"
                WHERE h.nivel < 5 
            )
            SELECT h.raiz as origem, 'Ciclo detectado com: ' || h.atual as detalhe
            FROM Hierarquia h
            WHERE h."manager" = h.raiz 
        """
    },
    {
        'nome': '28. Padronização de Departamento (Variações)',
        'dimensao': 'Consistência (Representação)',
        'tabelas': 'ad_users',
        'impacto': 'Falta de padronização (ex: "TI" vs "T.I.") afeta relatórios de BI e filtros.',
        'sql': """
            SELECT "department" as origem, 'Variação rara (< 3 ocorrências)' as detalhe
            FROM ad_users
            WHERE "department" IN (
                SELECT "department"
                FROM ad_users
                WHERE "department" IS NOT NULL AND "department" != ''
                GROUP BY "department"
                HAVING COUNT(*) < 3
            )
        """
    },
    {
        'nome': '29. Zombie Containers (OUs Vazias/Inativas)',
        'dimensao': 'Precisão / Temporalidade',
        'tabelas': 'ad_ous',
        'impacto': 'Estruturas legadas que poluem a visualização e gestão do diretório.',
        'sql': """
            SELECT ou."distinguishedName" as origem, 'OU potencialmente Zumbi' as detalhe
            FROM ad_ous ou
            WHERE NOT EXISTS (
                SELECT 1 FROM ad_users u 
                WHERE u."distinguishedName" LIKE '%' || ou."distinguishedName"
                AND (CAST(u."userAccountControl" AS INTEGER) & 2) = 0
            )
            AND NOT EXISTS (
                SELECT 1 FROM ad_computers c
                WHERE c."distinguishedName" LIKE '%' || ou."distinguishedName"
                AND CAST(c."lastLogonTimestamp" AS TIMESTAMPTZ) > NOW() - INTERVAL '90 days'
            )
        """
    }
]