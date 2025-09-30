import pandas as pd
from datetime import datetime

class TabelaRegistros:
    def __init__(self, dcConfig, dcParameter, logger, db_manager):
        self.logger = logger
        self.db_manager = db_manager
        self.dcConfig = dcConfig
        self.dcParameter = dcParameter
        self.databasename = self.dcConfig['databasename']
        self.schemaname = self.dcConfig['dbschema']
        self.tabtelprocesso = self.dcConfig['tabrelprocessname']
        self.tabcontroledados = self.dcConfig['tabcontroledados']

    def confere_conexao_do_banco(self):
        """Verifica e estabelece conexão com o banco de dados"""
        try:
            # Corrigido: usar get_connection() ao invés de connection
            if self.db_manager.get_connection():
                self.logger.log_info("confere_conexao_do_banco", "db_manager está conectado")
            else:
                self.db_manager.connect()
                self.logger.log_info("confere_conexao_do_banco", "Conexão estabelecida com sucesso")
            return True
        except Exception as exception:
            self.logger.log_error("confere_conexao_do_banco", f"Erro ao conectar no banco de dados: {str(exception)}", exception)
            return False

    def _tabela_existe(self, nome_tabela):
        """Verifica se a tabela existe no banco de dados"""
        try:
            query = f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{self.schemaname}' 
            AND TABLE_NAME = '{nome_tabela}'
            """
            # Corrigido: tratar o retorno como tupla (success, result)
            success, result = self.db_manager.execute_query(query)
            if success and result:
                return result[0][0] > 0
            return False
        except Exception as e:
            self.logger.log_error("_tabela_existe", f"Erro ao verificar existência da tabela {nome_tabela}: {str(e)}", e)
            return False

    def _criar_tabela(self):
        """Cria a tabela relatorios_processo_info"""
        try:
            create_table_sql = f"""
            USE [{self.databasename}]
            
            CREATE TABLE [{self.schemaname}].[{self.tabtelprocesso}](
                [id] [int] IDENTITY(1,1) NOT NULL,
                [id_controle_dado] [int] NULL,
                [documento] [varchar](255) NULL,
                [nome_interessado] [varchar](255) NULL,
                [data_protocolo] [date] NULL,
                [situacao] [varchar](255) NULL,
                [uf] [varchar](255) NULL,
                [numero_processo] [varchar](255) NULL,
                [documento_origem] [varchar](255) NULL,
                [procedencia] [varchar](255) NULL,
                [nome_assunto] [varchar](255) NULL,
                [tipo] [varchar](255) NULL,
                [sistema_profisc] [varchar](255) NULL,
                [sistema_processo] [varchar](255) NULL,
                [sistema_sief] [varchar](255) NULL,
                [orgao_origem] [varchar](255) NULL,
                [orgao_destino] [varchar](255) NULL,
                [orgao_outro] [varchar](255) NULL,
                [data_movimentado] [varchar](255) NULL,
                [sequencia] [int] NULL,
                [relacao] [int] NULL,
                [data_disjuntada] [varchar](255) NULL,
                [numero_sequencia_disjuntada] [varchar](255) NULL,
                [numero_aviso] [varchar](255) NULL,
                [numero_processo_principal] [varchar](255) NULL,
                [nome_orgao_disjuntada] [varchar](255) NULL,
                [codigo_tipo_movimento_processo] [varchar](255) NULL,
                [created_at] [datetime2](7) NULL,
                [status] [varchar](50) NULL,
                [status_mensagem] [varchar](max) NULL
            ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
            """
            
            # Corrigido: tratar o retorno como tupla e usar commit=True
            success, result = self.db_manager.execute_query(create_table_sql, commit=True)
            if not success:
                raise Exception(f"Falha ao criar tabela: {result}")
            
            # Adicionar constraints
            self._adicionar_constraints()
            
            self.logger.log_success("_criar_tabela", f"Tabela {self.tabtelprocesso} criada com sucesso")
            return True
            
        except Exception as e:
            self.logger.log_error("_criar_tabela", f"Erro ao criar tabela {self.tabtelprocesso}: {str(e)}", e)
            return False

    def _adicionar_constraints(self):
        """Adiciona as constraints padrão à tabela"""
        try:
            constraints_sql = [
                f"ALTER TABLE [{self.schemaname}].[{self.tabtelprocesso}] ADD DEFAULT (NULL) FOR [id_controle_dado]",
                f"ALTER TABLE [{self.schemaname}].[{self.tabtelprocesso}] ADD DEFAULT (getdate()) FOR [created_at]"
            ]
            
            for constraint in constraints_sql:
                # Corrigido: tratar o retorno como tupla e usar commit=True
                success, result = self.db_manager.execute_query(constraint, commit=True)
                if not success:
                    self.logger.log_warning("_adicionar_constraints", f"Falha ao adicionar constraint: {result}")
                
            self.logger.log_info("_adicionar_constraints", "Constraints processadas")
            
        except Exception as e:
            self.logger.log_error("_adicionar_constraints", f"Erro ao adicionar constraints: {str(e)}", e)

    def verificar_tabela(self):
        """Verifica se a tabela existe e a cria se necessário"""
        try:
            self.logger.log_info("verificar_tabela", "Iniciando verificação de tabela")
            
            if not self.confere_conexao_do_banco():
                self.logger.log_error("verificar_tabela", "Falha na conexão com banco de dados", None)
                return False
            if self._tabela_existe(self.tabtelprocesso):
                self.logger.log_info("verificar_tabela", f"Tabela {self.tabtelprocesso} já existe e está apta para uso")
                return True
            else:
                self.logger.log_info("verificar_tabela", f"Tabela {self.tabtelprocesso} não existe. Criando...")
                return self._criar_tabela()
                
        except Exception as e:
            self.logger.log_error("verificar_tabela", f"Erro ao verificar/criar tabela: {str(e)}", e)
            return False

    def inserir_registro(self, dados):
        """Insere um registro na tabela"""
        if not self.confere_conexao_do_banco():
            return False
            
        try:
            # Construir query de inserção dinamicamente baseada nos dados fornecidos
            colunas = ', '.join([f'[{col}]' for col in dados.keys()])
            valores = ', '.join(['?' for _ in dados.values()])
            
            query = f"INSERT INTO [{self.schemaname}].[{self.tabtelprocesso}] ({colunas}) VALUES ({valores})"
            
            # Corrigido: tratar o retorno como tupla e usar commit=True
            success, result = self.db_manager.execute_query(query, tuple(dados.values()), commit=True)
            if success:
                self.logger.log_success("inserir_registro", "Registro inserido com sucesso")
                return True
            else:
                raise Exception(f"Falha na inserção: {result}")
            
        except Exception as e:
            self.logger.log_error("inserir_registro", f"Erro ao inserir registro: {str(e)}", e)
            return False

    def consultar_registros(self, filtros=None, limite=None):
        """Consulta registros da tabela com filtros opcionais"""
        if not self.confere_conexao_do_banco():
            return None
            
        try:
            query = f"SELECT * FROM [{self.schemaname}].[{self.tabtelprocesso}]"
            parametros = []
            
            if filtros:
                condicoes = []
                for coluna, valor in filtros.items():
                    condicoes.append(f"[{coluna}] = ?")
                    parametros.append(valor)
                query += " WHERE " + " AND ".join(condicoes)
            
            if limite:
                query = f"SELECT TOP {limite} * FROM ({query}) AS subquery"
            
            # Corrigido: tratar o retorno como tupla
            success, resultados = self.db_manager.execute_query(query, tuple(parametros) if parametros else None)
            if success:
                self.logger.log_info("consultar_registros", f"Consulta executada com sucesso. {len(resultados) if resultados else 0} registros encontrados")
                return resultados
            else:
                raise Exception(f"Falha na consulta: {resultados}")
            
        except Exception as e:
            self.logger.log_error("consultar_registros", f"Erro ao consultar registros: {str(e)}", e)
            return None

    def fechar_conexao(self):
        """Fecha a conexão com o banco de dados"""
        try:
            self.db_manager.close()
            self.logger.log_info("fechar_conexao", "Conexão com banco de dados fechada")
        except Exception as e:
            self.logger.log_error("fechar_conexao", f"Erro ao fechar conexão: {str(e)}", e)
            
    def executar_update_pendentes(self):
        """Executa UPDATE para abortar registros pendentes há mais de 3 dias"""
        if not self.confere_conexao_do_banco():
            return False
            
        try:
            query = f"""
            UPDATE [{self.schemaname}].[{self.tabcontroledados}] 
            SET status = 'ABORTADO', 
                status_mensagem = 'EXCEDIDO TENTATIVAS DURANTE 3 DIAS'
            WHERE created_at < DATEADD(day, -3, GETDATE()) 
            AND status = 'PENDENTE'
            """
            
            success, result = self.db_manager.execute_query(query, commit=True)
            
            if success:
                # Verificar quantas linhas foram afetadas
                affected_rows = result if isinstance(result, int) else 0
                self.logger.log_success("executar_update_pendentes", f"UPDATE executado com sucesso. {affected_rows} registros atualizados para ABORTADO")
                return True
            else:
                raise Exception(f"Falha no UPDATE: {result}")
                
        except Exception as e:
            self.logger.log_error("executar_update_pendentes", f"Erro ao executar UPDATE na tabela {nome_tabela}: {str(e)}", e)
            return False
    
    def consultar_cnpjs_pendentes(self):
        """Consulta CNPJs pendentes dos últimos 3 dias (somente entre 07h e 22h)"""
        hora_atual = datetime.now().hour
        if not (7 <= hora_atual < 22):
            self.logger.log_warning("consultar_cnpjs_pendentes", f"Consulta ignorada por estar fora do horário permitido: {hora_atual}h")
            return None

        if not self.confere_conexao_do_banco():
            return None
            
        try:
            query = f"""
            SELECT *
            FROM [{self.schemaname}].[{self.tabcontroledados}] 
            WHERE created_at >= DATEADD(day, -3, GETDATE()) 
            AND status = 'PENDENTE'
            """
            #"\n            SELECT *\n            FROM [Comprot].[] \n            WHERE created_at >= DATEADD(day, -3, GETDATE()) \n            AND status = 'PENDENTE'\n            "
            # Gravar no df a consulta da query
            success, result = self.db_manager.execute_query(query)
            if success:
                if result and len(result) > 0:
                    # Só criar DataFrame se houver dados
                    df = pd.DataFrame(result)
                    self.logger.log_info("consultar_cnpjs_pendentes", f"Consulta executada com sucesso. {len(df)} registros encontrados")
                    return df
                else:
                    # Não há dados
                    self.logger.log_warning("consultar_cnpjs_pendentes", "Nenhum CNPJ pendente encontrado nos últimos 3 dias")
                    return None
            else:
                self.logger.log_error("consultar_cnpjs_pendentes", f"Falha na consulta: {result}")
                return None
        except Exception as e:
            self.logger.log_error("consultar_cnpjs_pendentes", f"Erro ao consultar CNPJs pendentes: {str(e)}")
            return None
        