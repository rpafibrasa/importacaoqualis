import os, json
import traceback
import pandas as pd

from datetime import datetime

from src.services.services import Services

from src.tasks.consultardadosprocesso import submain


def main():
    services = None
    try:
        services = Services()

        # services.executarhelloworld.run()

        # Verificar se Planilha de Processamento, carregar df e se df é vazia
        services.dcParameter, df = services.planilha_processar.verifica_arq_existe()
        if services.dcParameter is None or df is None:
            services.logger.log_critical('Main', 'Planilha de Processamento não encontrada ou Vazia')
        
        # Inserir registros na tabela de documentos GED
        if df is not None:
            services.logger.log_info('Main', f'Iniciando inserção de {len(df)} registros na tabela documentos_ged')
            sucesso_insercao = services.tabela_documentos_ged.inserir_registro(df)
            if sucesso_insercao:
                services.logger.log_success('Main', 'Registros inseridos com sucesso na tabela documentos_ged')
            else:
                services.logger.log_error('Main', 'Falha ao inserir registros na tabela documentos_ged', None)

        services.sharepoint_acesso.executar_acesso_sharepoint()
        # services.qualis_acesso_portal.executar_acesso_portal_qualis()

    except Exception as e:
        print(f"❌ ERRO CAPTURADO: {str(e)}")
        # Tentar usar o logger se disponível, senão usar print
        if services is not None:
            try:
                services.logger.log_critical('Main', f'Erro crítico: {str(e)}\nTraceback: {traceback.format_exc()}')
            except Exception as log_error:
                print(f'Erro crítico no logger: {str(log_error)}')
                print(f'Erro original: {str(e)}\nTraceback: {traceback.format_exc()}')
        else:
            print(f'Erro crítico na inicialização: {str(e)}\nTraceback: {traceback.format_exc()}')
    
    print("=== FINALIZANDO MAIN.PY ===")

if __name__ == '__main__':
    main()