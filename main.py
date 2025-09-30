import os, json
import traceback
import pandas as pd

from datetime import datetime
from src.services.services import Services; services = Services()
from src.tasks.consultardadosprocesso import submain

def main():
    try:
        consulta_cnpj = True
        for file in os.listdir('data\\temp'):
            if file.endswith('.json'):
                os.remove(os.path.join('data\\temp', file))

        # Conectar Banco - DBManager
        services.db_manager.connect()

        # Verificar Tabela - TabelaRegistros
        services.tabela_registros.verificar_tabela()

        # Verificar Tabela - TabelaControleDados
        services.tabela_controle_dados.verificar_tabela()

        # Verificar se Planilha de Processamento, carregar df e se df é vazia
        services.dcParameter, df = services.planilha_processar.verifica_arq_existe()
        if services.dcParameter is None or df is None:
            services.logger.log_critical('Main', 'Planilha de Processamento não encontrada ou Vazia')

        # Inserir registros na tabela de Registros
        if not df is None:
            services.tabela_controle_dados.inserir_registro(df)

        # Atualizar tabela de Registros para CNPJ que não foram processados por mais de 3 dias
        if consulta_cnpj:
            services.tabela_registros.executar_update_pendentes()

        #limpar df
        df = None

        #Consultar CNPJs pendentes
        if consulta_cnpj:
            df = services.tabela_registros.consultar_cnpjs_pendentes()

        if df is None or df.empty:
            services.logger.log_info('Main', 'Nenhum CNPJ para consulta de processos em aberto encontrado')

        #Processar CNPJs pendentes
        if consulta_cnpj:
            services.comprot.processar_cnpj(df)

        #Loop for each file in data\temp if is extension json delete it
        for file in os.listdir('data\\temp'):
            if file.endswith('.json'):
                os.remove(os.path.join('data\\temp', file))
                
        #TODO: Etapa de consulta de processo será direta mas terá alteracao futura
        #Limpar df
        df = None

        #Fazer um select SELECT * FROM [rpa_prd].[Comprot].[relatorios_processo_info];
        query = f"SELECT * FROM [{services.dcConfig['databasename']}].[{services.dcConfig['dbschema']}].[{services.dcConfig['tabrelprocessname']}] WHERE status = 'Consultar' AND created_at >= DATEADD(day, -20, GETDATE())"
        df = services.db_manager.execute_query(query)

        # Check if df exists and has the expected properties
        try:
            # Check if df is a tuple with length 2 and second element is a list
            if isinstance(df, tuple) and len(df) == 2 and isinstance(df[1], list):
                # Extract values from position 7 of each tuple in the list
                lista_processos = [item[7] for item in df[1] if len(item) > 7]
                print(lista_processos)
            else:
                lista_processos = []
                print("No processes found")
        except Exception as e:
            services.logger.log_error('Main', f'Error processing DataFrame: {str(e)}')
            lista_processos = []
            print("Error processing DataFrame")
        
        submain(lista_processos)
        # Carregar data\temp\processorelacao.json em uma variavel de dicionario
        #se arquivo data\temp\processorelacao.json existe

    except Exception as e:
        services.logger.log_critical('Main', f'Error: {str(e)}\nTraceback: {traceback.format_exc()}')

if __name__ == '__main__':
    main()