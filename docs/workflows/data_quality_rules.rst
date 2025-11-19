Regras de Qualidade de Dados (DAMA)
===================================

O sistema implementa um motor de regras baseado nas dimensões de qualidade do DAMA DMBOK.

Dimensões Implementadas
-----------------------

1. **Completude (Completeness):**
   Verifica se dados obrigatórios estão presentes.
   * *Exemplo:* Usuários sem Gerente, Computadores sem Dono.

2. **Validade (Validity):**
   Verifica se os dados respeitam o formato e domínio esperados.
   * *Exemplo:* UPN com sufixo incorreto, Logons legados muito longos.

3. **Unicidade (Uniqueness):**
   Verifica duplicidades em chaves primárias e regras de negócio.
   * *Lógica Especial:* Para computadores, a unicidade é verificada pelo **Patrimônio** (sufixo numérico do nome), ignorando prefixos de localidade.

4. **Consistência e Acurácia (Relacional):**
   Verifica a integridade entre tabelas diferentes.
   * *Exemplo:* O gerente listado no cadastro do usuário existe na tabela de usuários e está ativo?

Motor de Regras
---------------
As regras são definidas no arquivo ``regras_negocio.py`` e executadas dinamicamente via SQL. Isso permite adicionar novas regras de auditoria sem alterar a estrutura do código.