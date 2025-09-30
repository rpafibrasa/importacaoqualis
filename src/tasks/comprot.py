import pandas as pd
import os
from tkinter.constants import E
from dotenv import load_dotenv
load_dotenv()

from src.tasks.comprot_consulta_cnpj import consultar_processos_comprot


class Comprot:
    def __init__(self, dcConfig, dcParameter, logger, db_manager):
        self.logger = logger
        self.db_manager = db_manager
        self.dcConfig = dcConfig
        self.dcParameter = dcParameter
        self.databasename = self.dcConfig['databasename']
        self.schemaname = self.dcConfig['dbschema']
        self.tabcontroledados = self.dcConfig['tabcontroledados']
        

    def processar_cnpj(self, df):
        try:
            # Verificar se df é None ou vazio
            if df is None or df.empty:
                self.logger.log_warning('processar_cnpj', 'DataFrame é None ou vazio, nenhum CNPJ para processar')
                return
            
            # para cada arquivo em foldertemp se for extensao json delete
            foldertemp = self.dcParameter['foldertemp']
            for arquivo in os.listdir(foldertemp):
                if arquivo.endswith('.json'):
                    os.remove(os.path.join(foldertemp, arquivo))
                    self.logger.log_info('processar_cnpj', f'Arquivo {arquivo} removido')
            
            
            for idx, row in df.iterrows():
                resultado = None
                # Extrai o array de dados da linha (coluna 0)
                lista_dados = row.values[0]
                
                # CNPJ e formatação
                cnpj = lista_dados[2]
                cnpj_formatado = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
                
                # Datas e formatação
                data_ini = lista_dados[3]  # '202X-02-17'
                data_ini_formatada = f"{data_ini[8:]}/{data_ini[5:7]}/{data_ini[:4]}"
                data_inicial = data_ini_formatada
                
                data_fim = lista_dados[4]  # '202X-02-24'
                data_fim_formatada = f"{data_fim[8:]}/{data_fim[5:7]}/{data_fim[:4]}"
                data_final = data_fim_formatada
                
                
                #Iniciando consulta para o cpf na data de ate
                self.logger.log_info('Comprot', f'Iniciando consulta para o cnpj {cnpj_formatado} na data de {data_ini_formatada} ate {data_fim_formatada}')
                # cnpj = "28144326000101"
                # cnpj_formatado = "28.144.326/0001-01"
                # data_inicial = "12/08/2024"
                # data_final = "12/08/202X"
                #Executa a consulta completa de 1 cnpj de row
                numpaginas = 0
                numprocessostotal = 0
                idultimoprocesso = None

                while True:
                    resultado, numpaginas, numprocessostotal, idultimoprocesso = consultar_processos_comprot(cnpj, cnpj_formatado, data_inicial, data_final, max_tentativas=1, idultimoprocesso=idultimoprocesso)
                    
                    self.logger.log_info('Comprot', f'Consulta concluída para o cnpj {cnpj_formatado} na data de {data_ini_formatada} ate {data_fim_formatada}')
                    self.logger.log_info('Comprot', f'Foram encontrados {numprocessostotal} processos')
                    self.logger.log_info('Comprot', f'Foram encontrados {numpaginas} páginas')
                    self.logger.log_info('Comprot', f'Foram encontrados {idultimoprocesso} idultimoprocesso')
        
                    if resultado == 'SemProcesso':
                        query = f"""
                            UPDATE [{self.schemaname}].[{self.tabcontroledados}]
                            SET status = 'SEMPROCESSO',
                                status_mensagem = 'CONSULTA ABORTADA'
                            WHERE cnpj = '{cnpj}'
                            AND data_de = '{data_ini}'
                            AND data_ate = '{data_fim}'
                        """
                        self.db_manager.execute_query(query, commit=True)
                    
                    if resultado == 'Sucesso':
                        query = f"""
                            UPDATE [{self.schemaname}].[{self.tabcontroledados}]
                            SET status = 'CONCLUIDO',
                                status_mensagem = 'COLETADO PROCESSOS'
                            WHERE cnpj = '{cnpj}'
                            AND data_de = '{data_ini}'
                            AND data_ate = '{data_fim}'
                        """
                        self.db_manager.execute_query(query, commit=True)

                    # Executar tratamento do JSON após sucesso
                    self.logger.log_info('processar_cnpj', f'Executando tratamento de JSON para CNPJ {cnpj_formatado}')
                    self.logger.log_info('processar_cnpj', f'Tratamento de JSON concluído para CNPJ {cnpj_formatado}')

                    for file in os.listdir('data\\temp'):
                        if file.endswith('.json'):
                            os.remove(os.path.join('data\\temp', file))

                    if numpaginas == 1 or numpaginas == 0:
                        break

        except Exception as e:
            self.logger.log_error('Coprocessar_cnpjprot', f'Erro: {str(e)}')
