import os
import pandas as pd
import time
import gc


class PlanilhaProcessar:
    def __init__(self, logger, dcParameter):
        self.logger = logger
        self.dcParameter = dcParameter

        self.foldercapturados = self.dcParameter['foldercapturados']
        self.folderprocessados = self.dcParameter['folderprocessados']
    

    def carregardadosplanilha(self, filepath, filename):
        self.logger.log_info("carregardadosplanilha", f"Carregando dados da planilha {filepath}")
        xl_file = None
        try:
            # Verificar se o arquivo Excel tem múltiplas abas
            xl_file = pd.ExcelFile(filepath)
            self.logger.log_info("carregardadosplanilha", f"Abas encontradas: {xl_file.sheet_names}")
            
            # Dicionário para armazenar os dados de cada aba
            dados_abas = {}
            
            # Processar cada aba do Excel
            for sheet_name in xl_file.sheet_names:
                self.logger.log_info("carregardadosplanilha", f"Processando aba: {sheet_name}")
                
                # Carregar dados da aba específica
                df_aba = pd.read_excel(xl_file, sheet_name=sheet_name, dtype=str)
                
                # Verificar se a aba não está vazia
                if df_aba.empty:
                    self.logger.log_warning("carregardadosplanilha", f"A aba '{sheet_name}' está vazia")
                    continue
                
                self.logger.log_info("carregardadosplanilha", f"Aba '{sheet_name}' carregada com sucesso. Linhas: {len(df_aba)}, Colunas: {list(df_aba.columns)}")
                
                # Armazenar os dados da aba
                dados_abas[sheet_name] = df_aba
            
            # Verificar se pelo menos uma aba foi carregada
            if not dados_abas:
                self.logger.log_warning("carregardadosplanilha", "Todas as abas estão vazias")
                
                # move arquivo filename para pasta de erro foldererroprocessar
                if hasattr(self, 'foldererroprocessar'):
                    os.rename(filepath, os.path.join(self.foldererroprocessar, filename))
                    self.logger.log_info("carregardadosplanilha", f"Arquivo movido para pasta de erro: {os.path.join(self.foldererroprocessar, filename)}")
                
                # remove arquivo da pasta de processar
                os.remove(filepath)
                self.logger.log_info("carregardadosplanilha", f"Arquivo removido da pasta de processar: {filepath}")
                return None
            
            # Retornar o dicionário com os dados de todas as abas
            self.logger.log_info("carregardadosplanilha", f"Dados carregados com sucesso de {len(dados_abas)} aba(s)")
            return dados_abas

        except Exception as e:
            self.logger.log_error("carregardadosplanilha", f"Erro ao carregar dados da planilha: {str(e)}", e)
            return None
        finally:
            # Garantir que o arquivo Excel seja fechado adequadamente
            if xl_file is not None:
                try:
                    xl_file.close()
                except:
                    pass
            # Forçar garbage collection para liberar recursos
            gc.collect()
            # Pequeno delay para garantir que o arquivo seja liberado
            time.sleep(0.5)

    
    def processar_aba_unidade(self, df_unidade):
        """
        Processa os dados da aba 'LINK POR UNIDADE'
        Colunas esperadas: ['Unidade', 'Tipo de documento', 'Link']
        """
        try:
            self.logger.log_info("processar_aba_unidade", f"Processando {len(df_unidade)} registros da aba LINK POR UNIDADE")
            
            # Validar se as colunas esperadas existem
            colunas_esperadas = ['Unidade', 'Tipo de documento', 'Link']
            colunas_existentes = list(df_unidade.columns)
            
            for coluna in colunas_esperadas:
                if coluna not in colunas_existentes:
                    self.logger.log_warning("processar_aba_unidade", f"Coluna '{coluna}' não encontrada. Colunas disponíveis: {colunas_existentes}")
            
            # Processar dados específicos da aba de unidade
            dados_processados = []
            for index, row in df_unidade.iterrows():
                registro = {
                    'unidade': row.get('Unidade', ''),
                    'tipo_documento': row.get('Tipo de documento', ''),
                    'link': row.get('Link', ''),
                    'tipo_aba': 'UNIDADE'
                }
                dados_processados.append(registro)
            
            self.logger.log_info("processar_aba_unidade", f"Processados {len(dados_processados)} registros da aba UNIDADE")
            return dados_processados
            
        except Exception as e:
            self.logger.log_error("processar_aba_unidade", f"Erro ao processar aba UNIDADE: {str(e)}", e)
            return []


    def processar_aba_setor(self, df_setor):
        """
        Processa os dados da aba 'LINK POR SETOR'
        Colunas esperadas: ['Unidade', 'Setor', 'Tipo de documento', 'Link']
        """
        try:
            self.logger.log_info("processar_aba_setor", f"Processando {len(df_setor)} registros da aba LINK POR SETOR")
            
            # Validar se as colunas esperadas existem
            colunas_esperadas = ['Unidade', 'Setor', 'Tipo de documento', 'Link']
            colunas_existentes = list(df_setor.columns)
            
            for coluna in colunas_esperadas:
                if coluna not in colunas_existentes:
                    self.logger.log_warning("processar_aba_setor", f"Coluna '{coluna}' não encontrada. Colunas disponíveis: {colunas_existentes}")
            
            # Processar dados específicos da aba de setor
            dados_processados = []
            for index, row in df_setor.iterrows():
                registro = {
                    'unidade': row.get('Unidade', ''),
                    'setor': row.get('Setor', ''),
                    'tipo_documento': row.get('Tipo de documento', ''),
                    'link': row.get('Link', ''),
                    'tipo_aba': 'SETOR'
                }
                dados_processados.append(registro)
            
            self.logger.log_info("processar_aba_setor", f"Processados {len(dados_processados)} registros da aba SETOR")
            return dados_processados
            
        except Exception as e:
            self.logger.log_error("processar_aba_setor", f"Erro ao processar aba SETOR: {str(e)}", e)
            return []


    def processar_dados_completos(self, dados_abas):
        """
        Processa todos os dados das abas do Excel
        """
        try:
            self.logger.log_info("processar_dados_completos", f"Processando dados de {len(dados_abas)} aba(s)")
            
            todos_dados = []
            
            # Processar cada aba
            for nome_aba, df_aba in dados_abas.items():
                self.logger.log_info("processar_dados_completos", f"Processando aba: {nome_aba}")
                
                if nome_aba == 'LINK POR UNIDADE':
                    dados_unidade = self.processar_aba_unidade(df_aba)
                    todos_dados.extend(dados_unidade)
                    
                elif nome_aba == 'LINK POR SETOR':
                    dados_setor = self.processar_aba_setor(df_aba)
                    todos_dados.extend(dados_setor)
                    
                else:
                    self.logger.log_warning("processar_dados_completos", f"Aba '{nome_aba}' não reconhecida, pulando processamento")
            
            self.logger.log_info("processar_dados_completos", f"Total de registros processados: {len(todos_dados)}")
            return todos_dados
            
        except Exception as e:
            self.logger.log_error("processar_dados_completos", f"Erro ao processar dados completos: {str(e)}", e)
            return []


    def _tentar_mover_arquivo(self, origem, destino, max_tentativas=3, delay=1):
        """
        Tenta mover um arquivo com retry em caso de erro de arquivo em uso
        """
        for tentativa in range(max_tentativas):
            try:
                os.rename(origem, destino)
                self.logger.log_info("_tentar_mover_arquivo", f"Arquivo movido com sucesso: {destino}")
                return True
            except PermissionError as e:
                if tentativa < max_tentativas - 1:
                    self.logger.log_warning("_tentar_mover_arquivo", f"Tentativa {tentativa + 1} falhou, arquivo em uso. Tentando novamente em {delay}s...")
                    time.sleep(delay)
                    delay *= 2  # Aumenta o delay exponencialmente
                else:
                    self.logger.log_error("_tentar_mover_arquivo", f"Falha ao mover arquivo após {max_tentativas} tentativas: {str(e)}")
                    return False


    def _tentar_remover_arquivo(self, filepath, max_tentativas=3, delay=1):
        """
        Tenta remover um arquivo com retry em caso de erro de arquivo em uso
        """
        for tentativa in range(max_tentativas):
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
                    self.logger.log_info("_tentar_remover_arquivo", f"Arquivo removido com sucesso: {filepath}")
                    return True
                else:
                    self.logger.log_info("_tentar_remover_arquivo", f"Arquivo não existe mais: {filepath}")
                    return True
            except PermissionError as e:
                if tentativa < max_tentativas - 1:
                    self.logger.log_warning("_tentar_remover_arquivo", f"Tentativa {tentativa + 1} falhou, arquivo em uso. Tentando novamente em {delay}s...")
                    time.sleep(delay)
                    delay *= 2  # Aumenta o delay exponencialmente
                else:
                    self.logger.log_error("_tentar_remover_arquivo", f"Falha ao remover arquivo após {max_tentativas} tentativas: {str(e)}")
                    return False
            except Exception as e:
                self.logger.log_error("_tentar_remover_arquivo", f"Erro inesperado ao remover arquivo: {str(e)}")
                return False
        return False


    def verifica_arq_existe(self):
        try:
            # Verifica se a pasta existe
            if not os.path.exists(self.foldercapturados):
                self.logger.log_warning("verifica_arq_existe", f"Pasta não encontrada: {self.foldercapturados}")
                return None, None
            
            # Lista todos os arquivos na pasta
            arquivos = os.listdir(self.foldercapturados)
            
            # Procura por arquivos .xlsx
            for arquivo in arquivos:
                # if 'arquivo' contem .xlsx na variavel text
                if '.xlsx' in arquivo:
                    self.logger.log_info("verifica_arq_existe", "Arquivo encontrado")
                    
                    # Obtém o nome do arquivo
                    filename = os.path.basename(arquivo)
                    filepath = os.path.join(self.foldercapturados, filename)
                    self.logger.log_info("verifica_arq_existe", f"Nome do arquivo: {filename}")

                    #carrega dados de planilha
                    dados_abas = self.carregardadosplanilha(filepath, filename)
                    if dados_abas is None:
                        self.logger.log_error("verifica_arq_existe", f"Erro ao carregar dados da planilha {filepath}")
                        # Não tenta mover ou remover se houve erro no carregamento
                        continue

                    # Processar os dados das abas
                    dados_processados = self.processar_dados_completos(dados_abas)
                    
                    if dados_processados:
                        self.logger.log_info("verifica_arq_existe", f"Dados processados com sucesso: {len(dados_processados)} registros")
                        
                        # Tenta mover para pasta processados com retry
                        destino = os.path.join(self.folderprocessados, filename)
                        if self._tentar_mover_arquivo(filepath, destino):
                            self.logger.log_info("verifica_arq_existe", f"Arquivo processado com sucesso")
                        else:
                            self.logger.log_warning("verifica_arq_existe", f"Arquivo processado mas não foi possível mover para pasta processados")
                            # Tenta remover o arquivo original se não conseguiu mover
                            if not self._tentar_remover_arquivo(filepath):
                                self.logger.log_warning("verifica_arq_existe", f"Arquivo não pôde ser removido da pasta de origem")
                    else:
                        self.logger.log_warning("verifica_arq_existe", f"Nenhum dado foi processado do arquivo {filename}")

                    # Adiciona o caminho completo do arquivo no dcParameter
                    self.dcParameter['excelaprocessarfile'] = filepath
                    
                    return self.dcParameter, dados_processados
            
            # Se chegou até aqui, não encontrou nenhum arquivo .xlsx
            self.logger.log_critical("verifica_arq_existe", f"Não existe arquivo .xlsx na pasta {self.foldercapturados}")
            
            return None, None
            
        except Exception as e:
            self.logger.log_error("verifica_arq_existe", f"Erro ao verificar arquivo: {str(e)}", e)
            raise Exception(f"Arquivo de processamento não encontrado: {str(e)}")
