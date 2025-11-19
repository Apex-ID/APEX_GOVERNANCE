# Em: analises_simples/logica_de_analise/regras_negocio.py

REGRAS_DE_QUALIDADE = [
    # ==============================================================================
    # DIMENSÃO 1: COMPLETUDE (Dados faltando)
    # ==============================================================================
    {
        'nome': 'Contas de Usuário sem Gerente',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """("manager" IS NULL OR "manager" = '')""",
        'impacto': 'Contas órfãs de responsabilidade. Risco em revisões de acesso e desligamentos.'
    },
    {
        'nome': 'Contas de Usuário sem Departamento',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """("department" IS NULL OR "department" = '')""",
        'impacto': 'Impede a identificação de custos por área e dificulta a aplicação de RBAC.'
    },
    {
        'nome': 'Contas de Usuário sem Cargo (Title)',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """("title" IS NULL OR "title" = '')""",
        'impacto': 'Impede a validação de "Segregação de Funções" (SoD) e privilégios mínimos.'
    },
    {
        'nome': 'Usuário com Contato Incompleto',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """("mail" IS NULL OR "mail" = '') OR ("telephoneNumber" IS NULL OR "telephoneNumber" = '')""",
        'impacto': 'Dificulta a comunicação institucional e o suporte de TI.'
    },
    {
        'nome': 'Computadores sem Dono (ManagedBy)',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_computers', 'ad_computers_staging'],
        'sql_filtro_falha': """("managedBy" IS NULL OR "managedBy" = '')""",
        'impacto': 'Risco de segurança física e lógica. Dificulta a renovação de parque e inventário.'
    },
    {
        'nome': 'Computadores sem Descrição',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_computers', 'ad_computers_staging'],
        'sql_filtro_falha': """("description" IS NULL OR "description" = '')""",
        'impacto': 'Falta de documentação sobre a finalidade do ativo (ex: Laboratório vs. Servidor).'
    },
    {
        'nome': 'Grupos sem Dono (ManagedBy)',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_groups', 'ad_groups_staging'],
        'sql_filtro_falha': """("managedBy" IS NULL OR "managedBy" = '')""",
        'impacto': 'Grupos sem responsável acumulam permissões excessivas e nunca são revisados.'
    },
    {
        'nome': 'Grupos sem Descrição',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_groups', 'ad_groups_staging'],
        'sql_filtro_falha': """("description" IS NULL OR "description" = '')""",
        'impacto': 'Impossibilita saber a finalidade do grupo, aumentando risco de uso incorreto.'
    },
    # [NOVA]
    {
        'nome': 'OUs sem Descrição',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_ous', 'ad_ous_staging'],
        'sql_filtro_falha': """("description" IS NULL OR "description" = '')""",
        'impacto': 'Falta de documentação sobre o propósito da unidade organizacional.'
    },
    # [NOVA]
    {
        'nome': 'OUs sem Políticas de Grupo (GPO)',
        'dimensao': 'Completude',
        'tabelas_alvo': ['ad_ous', 'ad_ous_staging'],
        'sql_filtro_falha': """("gPLink" IS NULL OR "gPLink" = '')""",
        'impacto': 'Risco de Conformidade. Objetos nesta OU podem não estar recebendo políticas de segurança essenciais.'
    },

    # ==============================================================================
    # DIMENSÃO 2: TEMPORALIDADE (Dados obsoletos)
    # ==============================================================================
    {
        'nome': 'Usuários Inativos (>90 dias)',
        'dimensao': 'Temporalidade',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """CAST(NULLIF("lastLogonTimestamp", '') AS TIMESTAMPTZ) < NOW() - INTERVAL '90 days'""",
        'impacto': 'Contas antigas não utilizadas são vetores primários para ataques laterais.'
    },
    {
        'nome': 'Usuários Nunca Logados',
        'dimensao': 'Temporalidade',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """("lastLogonTimestamp" IS NULL OR "lastLogonTimestamp" = '')""",
        'impacto': 'Contas "fantasma" criadas desnecessariamente ou falha no processo de admissão.'
    },
    {
        'nome': 'Senha Expirada ou Antiga (>180 dias)',
        'dimensao': 'Temporalidade',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """CAST(NULLIF("pwdLastSet", '') AS TIMESTAMPTZ) < NOW() - INTERVAL '180 days'""",
        'impacto': 'Violação de política de segurança. Senhas antigas são mais vulneráveis a vazamentos.'
    },
    {
        'nome': 'Computadores Inativos (>90 dias)',
        'dimensao': 'Temporalidade',
        'tabelas_alvo': ['ad_computers', 'ad_computers_staging'],
        'sql_filtro_falha': """CAST(NULLIF("lastLogonTimestamp", '') AS TIMESTAMPTZ) < NOW() - INTERVAL '90 days'""",
        'impacto': 'Máquinas que não se conectam não recebem patches de segurança, tornando a rede vulnerável.'
    },

    # ==============================================================================
    # DIMENSÃO 3: CONSISTÊNCIA (Lógica de negócio e Riscos)
    # ==============================================================================
    {
        'nome': 'Contas Desabilitadas com Permissões Ativas',
        'dimensao': 'Consistência',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """
            (CAST(NULLIF("userAccountControl", '') AS INTEGER) & 2) > 0 
            AND ("memberOf" IS NOT NULL AND "memberOf" != '')
        """,
        'impacto': 'Falha grave de offboarding. Usuários demitidos mantêm acesso latente via grupos.'
    },
    {
        'nome': 'Contas com Privilégio Elevado (AdminCount)',
        'dimensao': 'Consistência',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """CAST(NULLIF("adminCount", '') AS INTEGER) = 1""",
        'impacto': 'Risco Crítico. Contas protegidas pelo AdminSDHolder têm privilégios administrativos altos.'
    },
    {
        'nome': 'Grupos Vazios',
        'dimensao': 'Consistência',
        'tabelas_alvo': ['ad_groups', 'ad_groups_staging'],
        'sql_filtro_falha': """("member" IS NULL OR "member" = '')""",
        'impacto': 'Poluição do diretório. Aumenta a complexidade da gestão sem agregar valor.'
    },
    {
        'nome': 'Grupos Aninhados (Nested Groups)',
        'dimensao': 'Consistência',
        'tabelas_alvo': ['ad_groups', 'ad_groups_staging'],
        'sql_filtro_falha': """("memberOf" IS NOT NULL AND "memberOf" != '')""",
        'impacto': 'Dificulta o rastreamento efetivo de permissões e pode levar a escaladas de privilégio acidentais.'
    },
    # [NOVA]
    {
        'nome': 'Computadores no Container Padrão (CN=Computers)',
        'dimensao': 'Consistência',
        'tabelas_alvo': ['ad_computers', 'ad_computers_staging'],
        # Verifica se o distinguishedName contém "CN=Computers,"
        'sql_filtro_falha': """"distinguishedName" LIKE '%CN=Computers,%'""",
        'impacto': 'Máquinas fora da estrutura de OUs não recebem políticas de segurança (GPOs) customizadas.'
    },
    # [NOVA]
    {
        'nome': 'Grupos "Admin" sem Proteção (AdminCount=0)',
        'dimensao': 'Consistência',
        'tabelas_alvo': ['ad_groups', 'ad_groups_staging'],
        # Nome tem Admin, mas adminCount é nulo ou 0
        'sql_filtro_falha': """
            "cn" LIKE '%Admin%' AND 
            (CAST(NULLIF("adminCount", '') AS INTEGER) IS NULL OR CAST(NULLIF("adminCount", '') AS INTEGER) = 0)
        """,
        'impacto': 'Grupos que parecem administrativos mas não são protegidos pelo sistema. Possível Backdoor ou erro de nomeação.'
    },

    # ==============================================================================
    # DIMENSÃO 4: VALIDADE (Formato)
    # ==============================================================================
    {
        'nome': 'UPN Fora do Padrão (@office.ufs.br)',
        'dimensao': 'Validade',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """"userPrincipalName" NOT LIKE '%@office.ufs.br'""",
        'impacto': 'Falha na autenticação federada (Office 365, Azure AD, Google).'
    },
    {
        'nome': 'Logon Legado (SAM) > 20 Caracteres',
        'dimensao': 'Validade',
        'tabelas_alvo': ['ad_users', 'ad_users_staging'],
        'sql_filtro_falha': """LENGTH("sAMAccountName") > 20""",
        'impacto': 'Incompatibilidade com sistemas legados (pre-Windows 2000) e possíveis erros de sincronização.'
    },
    # [NOVA]
    {
        'nome': 'Sistemas Operacionais Obsoletos (Win7/XP/2008)',
        'dimensao': 'Validade (Obsolescência)',
        'tabelas_alvo': ['ad_computers', 'ad_computers_staging'],
        'sql_filtro_falha': """
            "operatingSystem" ILIKE '%Windows 7%' OR 
            "operatingSystem" ILIKE '%Windows XP%' OR 
            "operatingSystem" ILIKE '%Server 2008%' OR
            "operatingSystem" ILIKE '%Server 2003%'
        """,
        'impacto': 'Risco Crítico de Segurança. Sistemas sem suporte oficial do fabricante e vulneráveis a exploits conhecidos.'
    }
]