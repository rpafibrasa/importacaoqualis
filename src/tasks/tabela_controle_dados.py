import pandas as pd
from datetime import datetime
    

class TabelaControleDados:
    def __init__(self, dcConfig, dcParameter, logger, db_manager):
        self.logger = logger
        self.db_manager = db_manager
        self.dcConfig = dcConfig
        self.dcParameter = dcParameter
        self.databasename = self.dcConfig['databasename']
        self.schemaname = self.dcConfig['dbschema']
        self.tabcontroledados = self.dcConfig['tabcontroledados']

    def confere_conexao_do_banco(self):
        """Verifica e estabelece conexão com o banco de dados"""
        try:
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
            success, result = self.db_manager.execute_query(query)
            if success and result:
                return result[0][0] > 0
            return False
        except Exception as e:
            self.logger.log_error("_tabela_existe", f"Erro ao verificar existência da tabela {nome_tabela}: {str(e)}", e)
            return False

    def _criar_tabela(self):
        """Cria a tabela controle_dados"""
        try:
            create_table_sql = f"""
            CREATE TABLE [{self.schemaname}].[{self.tabcontroledados}](
                [id] [int] IDENTITY(1,1) NOT NULL,
                [id_controle_execucao] [int] NULL,
                [cnpj] [varchar](20) NOT NULL,
                [data_de] [varchar](50) NULL,
                [data_ate] [varchar](50) NULL,
                [created_at] [datetime2](0) NULL,
                [started_at] [datetime2](0) NULL,
                [finished_at] [datetime2](0) NULL,
                [status] [varchar](100) NULL,
                [status_mensagem] [varchar](max) NULL,
                [arquivo] [varchar](255) NULL,
                CONSTRAINT [PK_{self.tabcontroledados}] PRIMARY KEY CLUSTERED ([id] ASC)
            )
            """
            
            success, result = self.db_manager.execute_query(create_table_sql, commit=True)
            if not success:
                raise Exception(f"Falha ao criar tabela: {result}")
            
            # Adicionar constraints
            self._adicionar_constraints()
            
            self.logger.log_success("_criar_tabela", f"Tabela {self.tabcontroledados} criada com sucesso")
            return True
            
        except Exception as e:
            self.logger.log_error("_criar_tabela", f"Erro ao criar tabela {self.tabcontroledados}: {str(e)}", e)
            return False

    def _adicionar_constraints(self):
        """Adiciona as constraints padrão à tabela"""
        try:
            constraints_sql = [
                f"ALTER TABLE [{self.schemaname}].[{self.tabcontroledados}] ADD DEFAULT (getdate()) FOR [created_at]",
                f"ALTER TABLE [{self.schemaname}].[{self.tabcontroledados}] ADD DEFAULT ('PENDENTE') FOR [status]"
            ]
            
            for constraint in constraints_sql:
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
                
            if self._tabela_existe(self.tabcontroledados):
                self.logger.log_info("verificar_tabela", f"Tabela {self.tabcontroledados} já existe e está apta para uso")
                return True
            else:
                self.logger.log_info("verificar_tabela", f"Tabela {self.tabcontroledados} não existe. Criando...")
                return self._criar_tabela()
                
        except Exception as e:
            self.logger.log_error("verificar_tabela", f"Erro ao verificar/criar tabela: {str(e)}", e)
            return False


    def inserir_registro(self, df):
        """Insere registros na tabela baseado no DataFrame"""
        if not self.confere_conexao_do_banco():
            return False
            
        try:
            registros_inseridos = 0
            registros_existentes = 0
            
            # Loop para cada linha do DataFrame
            for index, row in df.iterrows():
                # Extrair dados das colunas
                cnpj = str(row['cnpj']).strip()
                
                dataini_raw = str(row['dataini']).strip()
                datafim_raw = str(row['datafim']).strip()
                
                # Converter datas de DDMMYYYY para YYYY-MM-DD
                try:
                    # Remover possíveis caracteres não numéricos e garantir que seja string
                    dataini_clean = ''.join(filter(str.isdigit, dataini_raw))
                    datafim_clean = ''.join(filter(str.isdigit, datafim_raw))
                    
                    # Verificar se tem 8 dígitos (DDMMYYYY)
                    if len(dataini_clean) != 8 or len(datafim_clean) != 8:
                        self.logger.log_error("inserir_registro", f"Formato de data inválido para CNPJ {cnpj}. DataIni: {dataini_raw}, DataFim: {datafim_raw}", None)
                        continue
                    
                    # Converter de DDMMYYYY para YYYY-MM-DD
                    dataini = datetime.strptime(dataini_clean, '%d%m%Y').strftime('%Y-%m-%d')
                    datafim = datetime.strptime(datafim_clean, '%d%m%Y').strftime('%Y-%m-%d')
                except ValueError as e:
                    self.logger.log_error("inserir_registro", f"Erro ao converter data para CNPJ {cnpj}. DataIni: {dataini_raw}, DataFim: {datafim_raw}. Erro: {str(e)}", e)
                    continue
                
                # Verificar se o registro já existe
                select_query = f"""
                SELECT COUNT(*) 
                FROM [{self.schemaname}].[{self.tabcontroledados}] 
                WHERE [cnpj] = ? AND [data_de] = ? AND [data_ate] = ?
                """
                
                success, result = self.db_manager.execute_query(select_query, (cnpj, dataini, datafim))
                
                if success and result and result[0][0] > 0:
                    # Registro já existe
                    self.logger.log_info("inserir_registro", f"Registro já inserido anteriormente - CNPJ: {cnpj}, Data De: {dataini}, Data Até: {datafim}")
                    registros_existentes += 1
                else:
                    # Inserir novo registro
                    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    arquivo = self.dcParameter.get('excelaprocessarfile', '')
                    
                    insert_query = f"""
                    INSERT INTO [{self.schemaname}].[{self.tabcontroledados}] 
                    ([id_controle_execucao], [cnpj], [data_de], [data_ate], [created_at], [started_at], [finished_at], [status], [status_mensagem], [arquivo]) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    valores = (
                        0,  # id_controle_execucao sempre zero
                        cnpj,
                        dataini,
                        datafim,
                        agora,  # created_at
                        agora,  # started_at
                        None,   # finished_at (deixa vazio)
                        'PENDENTE',  # status
                        'AGUARDANDO PROCESSAR',  # status_mensagem
                        arquivo  # arquivo
                    )
                    
                    success, result = self.db_manager.execute_query(insert_query, valores, commit=True)
                    
                    if success:
                        self.logger.log_success("inserir_registro", f"Registro inserido com sucesso - CNPJ: {cnpj}, Data De: {dataini}, Data Até: {datafim}")
                        registros_inseridos += 1
                    else:
                        self.logger.log_error("inserir_registro", f"Falha ao inserir registro para CNPJ {cnpj}: {result}", None)
            
            # Log do resumo FORA do loop
            self.logger.log_info("inserir_registro", f"Processamento concluído - Inseridos: {registros_inseridos}, Já existentes: {registros_existentes}")
            return True
                
        except Exception as e:
            self.logger.log_error("inserir_registro", f"Erro ao processar registros: {str(e)}", e)
            return False

    def consultar_registros(self, filtros=None, limite=None):
        """Consulta registros da tabela com filtros opcionais"""
        if not self.confere_conexao_do_banco():
            return None
            
        try:
            query = f"SELECT * FROM [{self.schemaname}].[{self.tabcontroledados}]"
            parametros = []
            
            if filtros:
                condicoes = []
                for coluna, valor in filtros.items():
                    condicoes.append(f"[{coluna}] = ?")
                    parametros.append(valor)
                query += " WHERE " + " AND ".join(condicoes)
            
            if limite:
                query = f"SELECT TOP {limite} * FROM ({query}) AS subquery"
            
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