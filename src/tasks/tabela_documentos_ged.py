import pandas as pd
from datetime import datetime
    

class TabelaDocumentosGed:
    def __init__(self, logger, dcConfig, dcParameter, db_manager):
        self.logger = logger
        self.db_manager = db_manager
        self.dcConfig = dcConfig
        self.dcParameter = dcParameter
        self.databasename = self.dcConfig['databasename']
        self.schema = self.dcConfig['dbschema']
        self.tabdocumentosged = self.dcConfig['tabdocumentosged']

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

    def inserir_registro(self, dados_processados):
        """Insere registros na tabela documentos_ged baseado na lista de dados processados do Excel"""
        if not self.confere_conexao_do_banco():
            return False
            
        try:
            registros_inseridos = 0
            registros_existentes = 0
            registros_erro = 0
            
            # Verificar se dados_processados é uma lista
            if not isinstance(dados_processados, list):
                self.logger.log_error("inserir_registro", "Dados processados devem ser uma lista de dicionários", None)
                return False
            
            # Loop para cada registro processado
            for registro in dados_processados:
                try:
                    # Extrair dados do registro
                    unidade = str(registro.get('unidade', '')).strip()
                    setor = str(registro.get('setor', '')).strip()
                    tipo_documento = str(registro.get('tipo_documento', '')).strip()
                    link = str(registro.get('link', '')).strip()
                    tipo_aba = str(registro.get('tipo_aba', '')).strip()
                    
                    # Validar campos obrigatórios
                    if not link:
                        self.logger.log_warning("inserir_registro", f"Link vazio encontrado, pulando registro: {registro}")
                        registros_erro += 1
                        continue
                    
                    if not unidade:
                        self.logger.log_warning("inserir_registro", f"Unidade vazia encontrada, pulando registro: {registro}")
                        registros_erro += 1
                        continue
                    
                    # Verificar se o registro já existe (baseado no link único)
                    select_query = f"""
                    SELECT COUNT(*) 
                    FROM {self.schema}.{self.tabdocumentosged} 
                    WHERE link = %s
                    """
                    
                    success, result = self.db_manager.execute_query(select_query, (link,))
                    
                    if success and result and result[0][0] > 0:
                        # Registro já existe
                        self.logger.log_info("inserir_registro", f"Registro já inserido anteriormente - Link: {link}")
                        registros_existentes += 1
                    else:
                        # Inserir novo registro
                        agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        arquivo = self.dcParameter.get('excelaprocessarfile', '')
                        
                        insert_query = f"""
                        INSERT INTO {self.schema}.{self.tabdocumentosged} 
                        (unidade, setor, tipo_documento, link, tipo_aba, status, created_at, updated_at) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        valores = (
                            unidade,
                            setor if setor else None,  # setor pode ser NULL para registros da aba UNIDADE
                            tipo_documento,
                            link,
                            tipo_aba,
                            'PENDENTE',  # status padrão
                            agora,  # created_at
                            agora   # updated_at
                        )
                        
                        success, result = self.db_manager.execute_query(insert_query, valores, commit=True)
                        
                        if success:
                            self.logger.log_success("inserir_registro", f"Registro inserido com sucesso - Unidade: {unidade}, Setor: {setor}, Link: {link}")
                            registros_inseridos += 1
                        else:
                            self.logger.log_error("inserir_registro", f"Falha ao inserir registro - Link: {link}, Erro: {result}", None)
                            registros_erro += 1
                            
                except Exception as e:
                    self.logger.log_error("inserir_registro", f"Erro ao processar registro individual: {str(e)}, Registro: {registro}", e)
                    registros_erro += 1
                    continue
            
            # Log do resumo FORA do loop
            self.logger.log_info("inserir_registro", f"Processamento concluído - Inseridos: {registros_inseridos}, Já existentes: {registros_existentes}, Erros: {registros_erro}")
            return registros_inseridos > 0 or registros_existentes > 0  # Retorna True se houve sucesso em pelo menos alguns registros
                
        except Exception as e:
            self.logger.log_error("inserir_registro", f"Erro ao processar registros: {str(e)}", e)
            return False


    def consultar_registros(self, filtros=None, limite=None):
        """Consulta registros da tabela documentos_ged com filtros opcionais"""
        if not self.confere_conexao_do_banco():
            return None
            
        try:
            query = f"SELECT * FROM {self.schema}.documentos_ged"
            parametros = []
            
            if filtros:
                condicoes = []
                for coluna, valor in filtros.items():
                    condicoes.append(f"{coluna} = %s")
                    parametros.append(valor)
                query += " WHERE " + " AND ".join(condicoes)
            
            if limite:
                query += f" LIMIT {limite}"
            
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
