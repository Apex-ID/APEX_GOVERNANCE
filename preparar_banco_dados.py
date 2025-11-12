import psycopg2
import os
import subprocess
import sys
from dotenv import load_dotenv

# --- DEFINIÇÃO DAS TABELAS ---

# Nomes de todas as tabelas que vamos criar
TABLE_NAMES = [
    "ad_users_staging", "ad_computers_staging", "ad_groups_staging", "ad_ous_staging",
    "ad_users", "ad_computers", "ad_groups", "ad_ous",
    "etl_error_log"
]

# Dicionário com todos os comandos SQL para criar as tabelas
SQL_COMMANDS = {
    # --- TABELAS DE STAGING (Tudo como TEXT) ---
    "ad_users_staging": """
        CREATE TABLE ad_users_staging (
            id SERIAL PRIMARY KEY, "accountExpires" TEXT, "adminCount" TEXT, "badPasswordTime" TEXT, "badPwdCount" TEXT, "c" TEXT, "cn" TEXT, "co" TEXT, "codePage" TEXT, "company" TEXT, "countryCode" TEXT, "dSCorePropagationData" TEXT, "department" TEXT, "description" TEXT, "directReports" TEXT, "displayName" TEXT, "distinguishedName" TEXT, "division" TEXT, "facsimileTelephoneNumber" TEXT, "givenName" TEXT, "homePhone" TEXT, "info" TEXT, "initials" TEXT, "instanceType" TEXT, "ipPhone" TEXT, "isCriticalSystemObject" TEXT, "lastKnownParent" TEXT, "lastLogoff" TEXT, "lastLogon" TEXT, "lastLogonTimestamp" TEXT, "lockoutTime" TEXT, "logonCount" TEXT, "logonHours" TEXT, "mS-DS-ConsistencyGuid" TEXT, "mail" TEXT, "managedObjects" TEXT, "manager" TEXT, "memberOf" TEXT, "mobile" TEXT, "msDS-FailedInteractiveLogonCount" TEXT, "msDS-FailedInteractiveLogonCountAtLastSuccessfulLogon" TEXT, "msDS-LastFailedInteractiveLogonTime" TEXT, "msDS-LastKnownRDN" TEXT, "msDS-LastSuccessfulInteractiveLogonTime" TEXT, "msDS-SupportedEncryptionTypes" TEXT, "msNPAllowDialin" TEXT, "name" TEXT, "objectCategory" TEXT, "objectClass" TEXT, "objectGUID" TEXT, "objectSid" TEXT, "pager" TEXT, "physicalDeliveryOfficeName" TEXT, "primaryGroupID" TEXT, "pwdLastSet" TEXT, "sAMAccountName" TEXT, "sAMAccountType" TEXT, "scriptPath" TEXT, "servicePrincipalName" TEXT, "showInAdvancedViewOnly" TEXT, "sn" TEXT, "streetAddress" TEXT, "telephoneNumber" TEXT, "thumbnailPhoto" TEXT, "title" TEXT, "uSNChanged" TEXT, "uSNCreated" TEXT, "uid" TEXT, "uidNumber" TEXT, "unixUserPassword" TEXT, "userAccountControl" TEXT, "userCertificate" TEXT, "userParameters" TEXT, "userPrincipalName" TEXT, "userWorkstations" TEXT, "whenChanged" TEXT, "whenCreated" TEXT
        );
    """,
    "ad_computers_staging": """
        CREATE TABLE ad_computers_staging (
            id SERIAL PRIMARY KEY, "accountExpires" TEXT, "adminCount" TEXT, "badPasswordTime" TEXT, "badPwdCount" TEXT, "cn" TEXT, "codePage" TEXT, "countryCode" TEXT, "dNSHostName" TEXT, "dSCorePropagationData" TEXT, "description" TEXT, "displayName" TEXT, "distinguishedName" TEXT, "instanceType" TEXT, "isCriticalSystemObject" TEXT, "lastLogoff" TEXT, "lastLogon" TEXT, "lastLogonTimestamp" TEXT, "localPolicyFlags" TEXT, "lockoutTime" TEXT, "logonCount" TEXT, "mS-DS-CreatorSID" TEXT, "managedBy" TEXT, "memberOf" TEXT, "msDFSR-ComputerReferenceBL" TEXT, "msDS-GroupMSAMembership" TEXT, "msDS-HostServiceAccount" TEXT, "msDS-HostServiceAccountBL" TEXT, "msDS-KeyCredentialLink" TEXT, "msDS-ManagedPasswordId" TEXT, "msDS-ManagedPasswordInterval" TEXT, "msDS-ManagedPasswordPreviousId" TEXT, "msDS-SupportedEncryptionTypes" TEXT, "name" TEXT, "netbootSCPBL" TEXT, "networkAddress" TEXT, "objectCategory" TEXT, "objectClass" TEXT, "objectGUID" TEXT, "objectSid" TEXT, "operatingSystem" TEXT, "operatingSystemServicePack" TEXT, "operatingSystemVersion" TEXT, "primaryGroupID" TEXT, "pwdLastSet" TEXT, "rIDSetReferences" TEXT, "sAMAccountName" TEXT, "sAMAccountType" TEXT, "serverReferenceBL" TEXT, "servicePrincipalName" TEXT, "uSNChanged" TEXT, "uSNCreated" TEXT, "userAccountControl" TEXT, "userCertificate" TEXT, "whenChanged" TEXT, "whenCreated" TEXT
        );
    """,
    "ad_groups_staging": """
        CREATE TABLE ad_groups_staging (
            id SERIAL PRIMARY KEY, "adminCount" TEXT, "cn" TEXT, "dSCorePropagationData" TEXT, "description" TEXT, "distinguishedName" TEXT, "groupType" TEXT, "instanceType" TEXT, "isCriticalSystemObject" TEXT, "managedBy" TEXT, "managedObjects" TEXT, "member" TEXT, "memberOf" TEXT, "name" TEXT, "objectCategory" TEXT, "objectClass" TEXT, "objectGUID" TEXT, "objectSid" TEXT, "sAMAccountName" TEXT, "sAMAccountType" TEXT, "systemFlags" TEXT, "uSNChanged" TEXT, "uSNCreated" TEXT, "whenChanged" TEXT, "whenCreated" TEXT
        );
    """,
    "ad_ous_staging": """
        CREATE TABLE ad_ous_staging (
            id SERIAL PRIMARY KEY, "dSCorePropagationData" TEXT, "description" TEXT, "displayName" TEXT, "distinguishedName" TEXT, "gPLink" TEXT, "gPOptions" TEXT, "instanceType" TEXT, "isCriticalSystemObject" TEXT, "managedBy" TEXT, "name" TEXT, "objectCategory" TEXT, "objectClass" TEXT, "objectGUID" TEXT, "ou" TEXT, "showInAdvancedViewOnly" TEXT, "systemFlags" TEXT, "uSNChanged" TEXT, "uSNCreated" TEXT, "whenChanged" TEXT, "whenCreated" TEXT
        );
    """,
    
    # --- TABELAS DE PRODUÇÃO (Tipos Corretos) ---
    "ad_users": """
        CREATE TABLE ad_users (
            id SERIAL PRIMARY KEY, "accountExpires" TIMESTAMPTZ, "adminCount" INTEGER, "badPasswordTime" TIMESTAMPTZ, "badPwdCount" INTEGER, "c" VARCHAR(255), "cn" VARCHAR(255), "co" VARCHAR(255), "codePage" INTEGER, "company" VARCHAR(255), "countryCode" INTEGER, "dSCorePropagationData" TEXT, "department" VARCHAR(255), "description" TEXT, "directReports" TEXT, "displayName" VARCHAR(255), "distinguishedName" TEXT, "division" VARCHAR(255), "facsimileTelephoneNumber" VARCHAR(255), "givenName" VARCHAR(255), "homePhone" VARCHAR(255), "info" TEXT, "initials" VARCHAR(255), "instanceType" INTEGER, "ipPhone" VARCHAR(255), "isCriticalSystemObject" BOOLEAN, "lastKnownParent" TEXT, "lastLogoff" TIMESTAMPTZ, "lastLogon" TIMESTAMPTZ, "lastLogonTimestamp" TIMESTAMPTZ, "lockoutTime" TIMESTAMPTZ, "logonCount" INTEGER, "logonHours" BYTEA, "mS-DS-ConsistencyGuid" TEXT, "mail" VARCHAR(255), "managedObjects" TEXT, "manager" TEXT, "memberOf" TEXT, "mobile" VARCHAR(255), "msDS-FailedInteractiveLogonCount" INTEGER, "msDS-FailedInteractiveLogonCountAtLastSuccessfulLogon" INTEGER, "msDS-LastFailedInteractiveLogonTime" TIMESTAMPTZ, "msDS-LastKnownRDN" TEXT, "msDS-LastSuccessfulInteractiveLogonTime" TIMESTAMPTZ, "msDS-SupportedEncryptionTypes" INTEGER, "msNPAllowDialin" BOOLEAN, "name" VARCHAR(255), "objectCategory" TEXT, "objectClass" TEXT, "objectGUID" UUID, "objectSid" VARCHAR(255), "pager" VARCHAR(255), "physicalDeliveryOfficeName" VARCHAR(255), "primaryGroupID" INTEGER, "pwdLastSet" TIMESTAMPTZ, "sAMAccountName" VARCHAR(255), "sAMAccountType" INTEGER, "scriptPath" TEXT, "servicePrincipalName" TEXT, "showInAdvancedViewOnly" BOOLEAN, "sn" VARCHAR(255), "streetAddress" TEXT, "telephoneNumber" VARCHAR(255), "thumbnailPhoto" BYTEA, "title" VARCHAR(255), "uSNChanged" BIGINT, "uSNCreated" BIGINT, "uid" VARCHAR(255), "uidNumber" INTEGER, "unixUserPassword" TEXT, "userAccountControl" INTEGER, "userCertificate" TEXT, "userParameters" TEXT, "userPrincipalName" VARCHAR(255), "userWorkstations" TEXT, "whenChanged" TIMESTAMPTZ, "whenCreated" TIMESTAMPTZ
        );
    """,
    "ad_computers": """
        CREATE TABLE ad_computers (
            id SERIAL PRIMARY KEY, "accountExpires" TIMESTAMPTZ, "adminCount" INTEGER, "badPasswordTime" TIMESTAMPTZ, "badPwdCount" INTEGER, "cn" VARCHAR(255), "codePage" INTEGER, "countryCode" INTEGER, "dNSHostName" VARCHAR(255), "dSCorePropagationData" TIMESTAMPTZ, "description" TEXT, "displayName" VARCHAR(255), "distinguishedName" TEXT, "instanceType" INTEGER, "isCriticalSystemObject" BOOLEAN, "lastLogoff" TIMESTAMPTZ, "lastLogon" TIMESTAMPTZ, "lastLogonTimestamp" TIMESTAMPTZ, "localPolicyFlags" INTEGER, "lockoutTime" TIMESTAMPTZ, "logonCount" INTEGER, "mS-DS-CreatorSID" TEXT, "managedBy" TEXT, "memberOf" TEXT, "msDFSR-ComputerReferenceBL" TEXT, "msDS-GroupMSAMembership" TEXT, "msDS-HostServiceAccount" TEXT, "msDS-HostServiceAccountBL" TEXT, "msDS-KeyCredentialLink" TEXT, "msDS-ManagedPasswordId" TEXT, "msDS-ManagedPasswordInterval" TEXT, "msDS-ManagedPasswordPreviousId" TEXT, "msDS-SupportedEncryptionTypes" INTEGER, "name" VARCHAR(255), "netbootSCPBL" TEXT, "networkAddress" TEXT, "objectCategory" TEXT, "objectClass" TEXT, "objectGUID" UUID, "objectSid" VARCHAR(255), "operatingSystem" VARCHAR(255), "operatingSystemServicePack" VARCHAR(255), "operatingSystemVersion" VARCHAR(255), "primaryGroupID" INTEGER, "pwdLastSet" TIMESTAMPTZ, "rIDSetReferences" TEXT, "sAMAccountName" VARCHAR(255), "sAMAccountType" INTEGER, "serverReferenceBL" TEXT, "servicePrincipalName" TEXT, "uSNChanged" BIGINT, "uSNCreated" BIGINT, "userAccountControl" INTEGER, "userCertificate" TEXT, "whenChanged" TIMESTAMPTZ, "whenCreated" TIMESTAMPTZ
        );
    """,
    "ad_groups": """
        CREATE TABLE ad_groups (
            id SERIAL PRIMARY KEY, "adminCount" INTEGER, "cn" VARCHAR(255), "dSCorePropagationData" TIMESTAMPTZ, "description" TEXT, "distinguishedName" TEXT, "groupType" INTEGER, "instanceType" INTEGER, "isCriticalSystemObject" BOOLEAN, "managedBy" TEXT, "managedObjects" TEXT, "member" TEXT, "memberOf" TEXT, "name" VARCHAR(255), "objectCategory" TEXT, "objectClass" TEXT, "objectGUID" UUID, "objectSid" VARCHAR(255), "sAMAccountName" VARCHAR(255), "sAMAccountType" INTEGER, "systemFlags" INTEGER, "uSNChanged" BIGINT, "uSNCreated" BIGINT, "whenChanged" TIMESTAMPTZ, "whenCreated" TIMESTAMPTZ
        );
    """,
    "ad_ous": """
        CREATE TABLE ad_ous (
            id SERIAL PRIMARY KEY, "dSCorePropagationData" TEXT, "description" TEXT, "displayName" VARCHAR(255), "distinguishedName" TEXT, "gPLink" TEXT, "gPOptions" VARCHAR(255), "instanceType" INTEGER, "isCriticalSystemObject" BOOLEAN, "managedBy" TEXT, "name" VARCHAR(255), "objectCategory" TEXT, "objectClass" TEXT, "objectGUID" UUID, "ou" VARCHAR(255), "showInAdvancedViewOnly" BOOLEAN, "systemFlags" INTEGER, "uSNChanged" BIGINT, "uSNCreated" BIGINT, "whenChanged" TIMESTAMPTZ, "whenCreated" TIMESTAMPTZ
        );
    """,
    
    # --- TABELA DE LOG DE ERROS  ---
    "etl_error_log": """
        CREATE TABLE etl_error_log (
            log_id SERIAL PRIMARY KEY,
            table_name VARCHAR(255),
            staging_row_id INTEGER,
            error_message TEXT,
            error_timestamp TIMESTAMPTZ DEFAULT NOW(),
            raw_data JSONB
        );
    """
}

def preparar_banco_dados():
    """
    Executa a Etapa 4 do pipeline:
    1. Conecta ao PostgreSQL.
    2. Recria todas as tabelas de Staging e Produção (com id SERIAL PK).
    3. Recria a tabela de Log de Erros (com staging_row_id).
    4. Executa o 'migrate' do Django para criar as tabelas de Relatório.
    """
    print("="*60)
    print("INICIANDO ETAPA 4: PREPARAÇÃO DO BANCO DE DADOS")
    print("="*60)
    
    conn = None
    try:
        # ETAPA 4.1: Conectar ao banco de dados
        print(f"\n[ETAPA 4.1] Conectando ao banco de dados em {os.getenv('DB_HOST')}...")
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS')
        )
        cursor = conn.cursor()
        print("SUCESSO: Conexão com o PostgreSQL estabelecida.")

        # ETAPA 4.2: Recriar as tabelas (Staging, Produção, Log)
        print("\n[ETAPA 4.2] Recriando as tabelas de dados e de log...")
        for table_name in TABLE_NAMES:
            print(f"  -> Processando tabela: '{table_name}'")
            
            print(f"     - Removendo tabela antiga (se existir)...")
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
            
            print(f"     - Criando nova tabela...")
            cursor.execute(SQL_COMMANDS[table_name])
            
        conn.commit()
        print("SUCESSO: Tabelas de dados e de log recriadas.")

    except psycopg2.OperationalError as e:
        print("\nERRO CRÍTICO: Não foi possível conectar ao banco de dados.")
        print("-> Verifique se o IP, porta, usuário e senha no .env estão corretos.")
        print(f"-> Detalhes do erro: {e}")
        return False
    except Exception as e:
        print(f"\nOcorreu um erro inesperado durante a execução do SQL: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn is not None:
            cursor.close()
            conn.close()
            print("\nConexão com o PostgreSQL (via psycopg2) foi encerrada.")

    # ETAPA 4.3: Executar o 'migrate' do Django
    print("\n[ETAPA 4.3] Executando o Django 'migrate' para criar/atualizar as tabelas do sistema...")
    try:
        # Garante que estamos usando o python3 do ambiente virtual
        python_executable = os.path.join(sys.prefix, 'bin', 'python3')
        if not os.path.exists(python_executable):
             python_executable = os.path.join(sys.prefix, 'bin', 'python') # Fallback para 'python'
        
        result = subprocess.run(
            [python_executable, "manage.py", "migrate"],
            capture_output=True, text=True, check=True
        )
        print(result.stdout)
        print("SUCESSO: Tabelas do Django e de relatórios criadas/atualizadas.")
    except subprocess.CalledProcessError as e:
        print("\nERRO CRÍTICE ao executar o 'migrate' do Django:")
        print(e.stderr)
        return False
    except FileNotFoundError:
        print(f"\nERRO CRÍTICO: Não foi possível encontrar o executável '{python_executable}'.")
        print("Certifique-se de que o ambiente virtual está ativo e o Django está instalado.")
        return False

    print("\n" + "="*60)
    print("ETAPA 4: PREPARAÇÃO DO BANCO DE DADOS CONCLUÍDA COM SUCESSO!")
    print("="*60)
    return True

if __name__ == "__main__":
    print("INFO: Carregando configurações do arquivo .env...")
    load_dotenv()
    
    db_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASS']
    if not all(os.getenv(var) for var in db_vars):
        print("\nERRO: Credenciais do banco de dados não encontradas no .env.")
    else:
        preparar_banco_dados()