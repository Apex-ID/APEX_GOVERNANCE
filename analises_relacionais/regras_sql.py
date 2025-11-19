# Em: analises_relacionais/regras_sql.py

REGRAS_RELACIONAIS_SQL = [
    # ==============================================================================
    # GRUPO 1: COMPLETUDE (Campos Vazios)
    # ==============================================================================
    {
        'nome': 'Contas de Usuário sem Gerente',
        'tabelas': 'ad_users',
        'impacto': 'Contas órfãs de responsabilidade. Risco em revisões de acesso e desligamentos.',
        'sql': """
            SELECT "cn" as origem, 'Campo manager vazio' as detalhe
            FROM ad_users
            WHERE "manager" IS NULL OR "manager" = ''
        """
    },
    {
        'nome': 'Contas sem Departamento',
        'tabelas': 'ad_users',
        'impacto': 'Impede a identificação de custos por área e dificulta a aplicação de RBAC.',
        'sql': """
            SELECT "cn" as origem, 'Campo department vazio' as detalhe
            FROM ad_users
            WHERE "department" IS NULL OR "department" = ''
        """
    },
    {
        'nome': 'Contas sem Cargo (Title)',
        'tabelas': 'ad_users',
        'impacto': 'Impede a validação de Segregação de Funções (SoD).',
        'sql': """
            SELECT "cn" as origem, 'Campo title vazio' as detalhe
            FROM ad_users
            WHERE "title" IS NULL OR "title" = ''
        """
    },
    {
        'nome': 'Usuário com Contato Incompleto',
        'tabelas': 'ad_users',
        'impacto': 'Dificulta a comunicação institucional e o suporte de TI.',
        'sql': """
            SELECT "cn" as origem, 'Sem E-mail ou Telefone' as detalhe
            FROM ad_users
            WHERE ("mail" IS NULL OR "mail" = '') OR ("telephoneNumber" IS NULL OR "telephoneNumber" = '')
        """
    },
    {
        'nome': 'Computadores sem Dono (ManagedBy)',
        'tabelas': 'ad_computers',
        'impacto': 'Risco de segurança física e lógica. Dificulta inventário.',
        'sql': """
            SELECT "cn" as origem, 'Campo managedBy vazio' as detalhe
            FROM ad_computers
            WHERE "managedBy" IS NULL OR "managedBy" = ''
        """
    },
    {
        'nome': 'Computadores sem Descrição',
        'tabelas': 'ad_computers',
        'impacto': 'Falta de documentação sobre a finalidade do ativo.',
        'sql': """
            SELECT "cn" as origem, 'Campo description vazio' as detalhe
            FROM ad_computers
            WHERE "description" IS NULL OR "description" = ''
        """
    },
    {
        'nome': 'Grupos sem Dono',
        'tabelas': 'ad_groups',
        'impacto': 'Grupos sem responsável acumulam permissões excessivas.',
        'sql': """
            SELECT "cn" as origem, 'Campo managedBy vazio' as detalhe
            FROM ad_groups
            WHERE "managedBy" IS NULL OR "managedBy" = ''
        """
    },
    {
        'nome': 'Grupos sem Descrição',
        'tabelas': 'ad_groups',
        'impacto': 'Impossibilita saber a finalidade do grupo.',
        'sql': """
            SELECT "cn" as origem, 'Campo description vazio' as detalhe
            FROM ad_groups
            WHERE "description" IS NULL OR "description" = ''
        """
    },

    # ==============================================================================
    # GRUPO 2: TEMPORALIDADE (Dados Obsoletos)
    # ==============================================================================
    {
        'nome': 'Usuários Inativos (>90 dias)',
        'tabelas': 'ad_users',
        'impacto': 'Vetores primários para ataques laterais.',
        'sql': """
            SELECT "cn" as origem, 'Último logon: ' || CAST("lastLogonTimestamp" AS TEXT) as detalhe
            FROM ad_users
            WHERE "lastLogonTimestamp" < NOW() - INTERVAL '90 days'
        """
    },
    {
        'nome': 'Usuários Nunca Logados',
        'tabelas': 'ad_users',
        'impacto': 'Contas fantasma criadas desnecessariamente.',
        'sql': """
            SELECT "cn" as origem, 'Nunca realizou logon' as detalhe
            FROM ad_users
            WHERE "lastLogonTimestamp" IS NULL
        """
    },
    {
        'nome': 'Senha Expirada ou Antiga (>180 dias)',
        'tabelas': 'ad_users',
        'impacto': 'Violação de política. Senhas antigas são vulneráveis.',
        'sql': """
            SELECT "cn" as origem, 'Senha de: ' || CAST("pwdLastSet" AS TEXT) as detalhe
            FROM ad_users
            WHERE "pwdLastSet" < NOW() - INTERVAL '180 days'
        """
    },
    {
        'nome': 'Computadores Inativos (>90 dias)',
        'tabelas': 'ad_computers',
        'impacto': 'Máquinas vulneráveis sem patches de segurança.',
        'sql': """
            SELECT "cn" as origem, 'Inativo desde: ' || CAST("lastLogonTimestamp" AS TEXT) as detalhe
            FROM ad_computers
            WHERE "lastLogonTimestamp" < NOW() - INTERVAL '90 days'
        """
    },

    # ==============================================================================
    # GRUPO 3: CONSISTÊNCIA E INTEGRIDADE (Relacionamentos)
    # ==============================================================================
    {
        'nome': 'Gerente Inválido (Link Quebrado)',
        'tabelas': 'ad_users (JOIN ad_users)',
        'impacto': 'Usuários reportam a gerentes que não existem no AD.',
        'sql': """
            SELECT u."cn" as origem, 'Gerente inexistente: ' || u."manager" as detalhe
            FROM ad_users u
            LEFT JOIN ad_users m ON u."manager" = m."distinguishedName"
            WHERE u."manager" IS NOT NULL AND u."manager" != '' AND m."distinguishedName" IS NULL
        """
    },
    {
        'nome': 'Gerente Desabilitado',
        'tabelas': 'ad_users (JOIN ad_users)',
        'impacto': 'Funcionários ativos reportam a gerentes desligados.',
        'sql': """
            SELECT u."cn" as origem, 'Gerente desabilitado: ' || m."cn" as detalhe
            FROM ad_users u
            JOIN ad_users m ON u."manager" = m."distinguishedName"
            WHERE (CAST(u."userAccountControl" AS INTEGER) & 2) = 0
              AND (CAST(m."userAccountControl" AS INTEGER) & 2) > 0
        """
    },
    {
        'nome': 'Conta Desabilitada com Permissões',
        'tabelas': 'ad_users',
        'impacto': 'Contas desligadas mantêm acesso latente.',
        'sql': """
            SELECT "cn" as origem, 'Conta desativada possui grupos' as detalhe
            FROM ad_users
            WHERE (CAST("userAccountControl" AS INTEGER) & 2) > 0 
              AND "memberOf" IS NOT NULL AND "memberOf" != ''
        """
    },
    {
        'nome': 'Admin Count=1 (Privilégio Elevado)',
        'tabelas': 'ad_users',
        'impacto': 'Contas protegidas pelo sistema (AdminSDHolder).',
        'sql': """
            SELECT "cn" as origem, 'Conta Protegida' as detalhe
            FROM ad_users
            WHERE CAST("adminCount" AS INTEGER) = 1
        """
    },
    {
        'nome': 'Computador Órfão',
        'tabelas': 'ad_computers (JOIN ad_users)',
        'impacto': 'Ativos de TI vinculados a usuários inexistentes.',
        'sql': """
            SELECT c."cn" as origem, 'Dono inexistente: ' || c."managedBy" as detalhe
            FROM ad_computers c
            LEFT JOIN ad_users u ON c."managedBy" = u."distinguishedName"
            WHERE c."managedBy" IS NOT NULL AND c."managedBy" != '' AND u."distinguishedName" IS NULL
        """
    },
    {
        'nome': 'Grupos Aninhados',
        'tabelas': 'ad_groups',
        'impacto': 'Dificuldade de auditoria de permissões.',
        'sql': """
            SELECT "cn" as origem, 'Está dentro de outro grupo' as detalhe
            FROM ad_groups
            WHERE "memberOf" IS NOT NULL AND "memberOf" != ''
        """
    },

    # ==============================================================================
    # GRUPO 4: VALIDADE (Formato)
    # ==============================================================================
    {
        'nome': 'UPN Fora do Padrão',
        'tabelas': 'ad_users',
        'impacto': 'Falha na autenticação federada (Office 365).',
        'sql': """
            SELECT "sAMAccountName" as origem, "userPrincipalName" as detalhe
            FROM ad_users
            WHERE "userPrincipalName" NOT LIKE '%@office.ufs.br'
        """
    },
    {
        'nome': 'Logon Legado > 20 Caracteres',
        'tabelas': 'ad_users',
        'impacto': 'Incompatibilidade com sistemas legados.',
        'sql': """
            SELECT "cn" as origem, "sAMAccountName" as detalhe
            FROM ad_users
            WHERE LENGTH("sAMAccountName") > 20
        """
    }
]