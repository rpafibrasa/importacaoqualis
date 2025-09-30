import os
import pandas as pd
import numpy as np
import shutil

class PlanilhaProcessar:
    def __init__(self, dcConfig, dcParameter, logger, db_manager):
        self.logger = logger
        self.db_manager = db_manager
        self.dcConfig = dcConfig
        self.dcParameter = dcParameter
        self.databasename = self.dcConfig['databasename']
        self.schemaname = self.dcConfig['dbschema']

        self.folderrede = self.dcParameter['folderrede']
        self.foldercapturados = self.dcParameter['foldercapturados']
    
    def verifica_arq_existe(self):
        try:
            # 1. Etapa - verificar arquivo da rede e mover para pasta local

            # verifica se existe a pasta de rede
            if not os.path.exists(self.folderrede):
                self.logger.log_error("captura_arquivos_rede", f"A pasta de rede {self.folderrede} não existe ou não está acessível")
                return None
            
            # verifica a existência de arquivos na pasta de rede
            arquivos_rede = [f for f in os.listdir(self.folderrede) if os.path.isfile(os.path.join(self.folderrede, f))]
            if not arquivos_rede:
                self.logger.log_info("verifica_arq_existe", "Nenhum arquivo encontrado na rede")
                return None

            for arquivo in arquivos_rede:
                caminho_origem = os.path.join(self.folderrede, arquivo)
                caminho_destino = os.path.join(self.foldercapturados, arquivo)

                try:
                    shutil.copy2(caminho_origem, caminho_destino)
                    self.logger.log_info("verifica_arq_existe", f"Arquivo {arquivo} movido da rede para pasta local com sucesso")
                except Exception as e:
                    self.logger.log_error("verifica_arq_existe", f"Erro ao mover arquivo da rede para pasta a processar: {str(e)}")
                    return None

            # Verifica se a pasta existe
            if not os.path.exists(self.foldercapturados):
                self.logger.log_warning("verifica_arq_existe", f"Pasta não encontrada: {self.foldercapturados}")
                return None
            
            # Lista todos os arquivos na pasta
            arquivos = os.listdir(self.foldercapturados)
            
            # Procura por arquivos .xlsx
            for arquivo in arquivos:
                #if 'arquivo' contem .xlsx na variavel text
                if '.xlsx' in arquivo:
                    self.logger.log_info("verifica_arq_existe", "Arquivo encontrado")
                    
                    # Obtém o nome do arquivo
                    filename = os.path.basename(arquivo)
                    filepath = os.path.join(self.foldercapturados, filename)
                    self.logger.log_info("verifica_arq_existe", f"Nome do arquivo: {filename}")

                    #carrega dados de planilha
                    df = self.carregardadosplanilha(filepath, filename)
                    if df is None:
                        self.logger.log_error("verifica_arq_existe", f"Erro ao carregar dados da planilha {filepath}")

                        #move arquivo para pasta de erro
                        os.rename(filename, os.path.join(self.foldererroprocessar, filename))
                        self.logger.log_info("verifica_arq_existe", f"Arquivo movido para pasta de erro: {os.path.join(self.foldererroprocessar, filename)}")

                        #remove arquivo da pasta de processar
                        if os.path.exists(filepath):
                            os.remove(filepath)
                            self.logger.log_info("verifica_arq_existe", f"Arquivo removido da pasta de processar: {filepath}")
                            raise FileNotFoundError()
                    else:
                        #mova para pasta processados
                        try:
                            os.rename(filepath, os.path.join(self.foldercapturados, filename))
                            self.logger.log_info("verifica_arq_existe", f"Arquivo movido para pasta processados: {os.path.join(self.foldercapturados, filename)}")
                        except Exception as e:
                            self.logger.log_error("verifica_arq_existe", f"Erro ao mover arquivo para pasta processados: {str(e)}")

                        #remove arquivo da pasta de processar
                        if os.path.exists(filepath):
                            os.remove(filepath)
                            self.logger.log_info("verifica_arq_existe", f"Arquivo removido da pasta de processar: {filepath}")

                    # Adiciona o caminho completo do arquivo no dcParameter
                    self.dcParameter['excelaprocessarfile'] = os.path.join(self.foldercapturados, filename)
                    
                    return self.dcParameter, df
            
            # Se chegou até aqui, não encontrou nenhum arquivo .xlsx
            self.logger.log_critical("verifica_arq_existe", f"Não existe arquivo .xlsx na pasta {self.foldercapturados}")
            
            return None, None
            
        except Exception as e:
            self.logger.log_error("verifica_arq_existe", f"Erro ao verificar arquivo: {str(e)}", e)
            raise Exception(f"Arquivo de processamento não encontrado: {str(e)}")


    def carregardadosplanilha(self, filepath, filename):
        self.logger.log_info("carregardadosplanilha", f"Carregando dados da planilha {filepath}")
        try:
            # Carregar dados da planilha capturar todos os dados com valor de text para que venha 0 a esquerda
            df = pd.read_excel(filepath, dtype=str)
            self.logger.log_info("carregardadosplanilha", f"Dados carregados com sucesso. Linhas: {len(df)}")

            # confere se df é vazia
            if df.empty:
                self.logger.log_warning("carregardadosplanilha", "A planilha está vazia")

                # move arquivo filename para pasta de erro foldererroprocessar
                os.rename(filepath, os.path.join(self.foldererroprocessar, filename))
                self.logger.log_info("carregardadosplanilha", f"Arquivo movido para pasta de erro: {os.path.join(self.foldererroprocessar, filename)}")

                # remove arquivo da pasta de processar
                os.remove(filepath)
                self.logger.log_info("carregardadosplanilha", f"Arquivo removido da pasta de processar: {filepath}")
                return None
            
            #renomeia header do df col1 cnpj, col2 dataini, col3 datafim
            df.columns = ['cnpj', 'dataini', 'datafim']
            return df
            
        except Exception as e:
            self.logger.log_error("carregardadosplanilha", f"Erro ao carregar dados da planilha: {str(e)}", e)
            raise Exception(f"Erro ao carregar dados da planilha: {str(e)}")
