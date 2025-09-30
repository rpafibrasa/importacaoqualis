import requests
import json
import os
import time, traceback
from datetime import datetime
from dotenv import load_dotenv
from src.services.logsrc import EnhancedLogger
from src.services.databasesrc import DBManager
from src.services.configsrc import config

#import log
dcConfig, dcParameter = config.loadconfig()
logger = EnhancedLogger(dcConfig, dcParameter)
db_manager = DBManager(dcConfig, dcParameter, logger)  # Corrigido: removido self.logger
dbschema = dcConfig.get('dbschema')  # Default to 'dbo' if dbschema not found in parameters
tabrelprocessname = dcConfig.get('tabrelprocessname')  # Default to 'relatorios_processo_infov2' if tabrelprocessname not found in parameters
dbname = dcConfig.get('databasename')  # Default to 'rpa_prd' if databasename not found in parameters

# Carregar variáveis de ambiente
load_dotenv()

def consultar_processo_individual(numero_processo, session=None, max_tentativas=3):
    """
    Consulta os detalhes de um processo específico
    """
    if session is None:
        session = requests.Session()
    
    logger.log_info("consultar_processo_individual", f"Consultando processo: {numero_processo}")
    
    for tentativa in range(max_tentativas):
        logger.log_info("consultar_processo_individual", f"Tentativa {tentativa + 1} de {max_tentativas}")
        
        try:
            # Gerar timestamp atual
            timestamp_atual = int(time.time() * 1000)
            
            # URL da API para consultar processo individual
            url_processo = f"https://comprot.fazenda.gov.br/comprotegov/api/processo/{numero_processo}?_={timestamp_atual}"
            
            # Headers para a requisição
            headers = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,pt;q=0.7',
                'Connection': 'keep-alive',
                'Host': 'comprot.fazenda.gov.br',
                'Referer': 'https://comprot.fazenda.gov.br/comprotegov/site/index.html?',
                'Sec-Ch-Ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest'
            }
            
            # Fazer a requisição
            response = session.get(url_processo, headers=headers, verify=False)
            
            logger.log_info("consultar_processo_individual", f"Status da resposta: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    dados_processo = response.json()
                    logger.log_success("consultar_processo_individual", f"✅ Processo {numero_processo} consultado com sucesso!")
                    return dados_processo
                except json.JSONDecodeError:
                    logger.log_error("consultar_processo_individual", f"Erro ao decodificar JSON para processo {numero_processo}")
                    logger.log_info("consultar_processo_individual", f"Response: {response.text[:200]}...")
                    return None
            elif response.status_code == 422:
                logger.log_warning("consultar_processo_individual", f"Status 422 - Captcha inválido ou expirado para processo {numero_processo}")
                if tentativa < max_tentativas - 1:
                    logger.log_info("consultar_processo_individual", "Aguardando antes de tentar novamente...")
                    time.sleep(10)
                    continue
            elif response.status_code == 404:
                logger.log_warning("consultar_processo_individual", f"Processo {numero_processo} não encontrado (404)")
                return None
            elif response.status_code == 400:
                logger.log_warning("consultar_processo_individual", f"Processo {numero_processo} com resposta de falha interna.")
                return None
            else:
                logger.log_warning("consultar_processo_individual", f"Status {response.status_code} - {response.text[:100]}...")
                if tentativa < max_tentativas - 1:
                    time.sleep(5)
                    continue
                    
        except Exception as e:
            logger.log_error("consultar_processo_individual", f"Erro ao consultar processo {numero_processo}: {e}")
            if tentativa < max_tentativas - 1:
                time.sleep(5)
                continue
    
    logger.log_error("consultar_processo_individual", f"❌ Falha ao consultar processo {numero_processo} após {max_tentativas} tentativas")
    return None

def consultar_todos_processos(lista_processos, salvar_json=True):
    """
    Consulta todos os processos da lista e salva os resultados
    """
    session = requests.Session()
    resultados = []
    
    # Acessar página inicial para obter cookies
    url_inicial = os.getenv('URL_INICIAL', 'https://comprot.fazenda.gov.br/comprotegov/site/index.html?')
    headers_inicial = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8'
    }
    
    try:
        response_inicial = session.get(url_inicial, headers=headers_inicial, verify=False)
        logger.log_info("consultar_todos_processos", f"Página inicial acessada: {response_inicial.status_code}")
    except Exception as e:
        logger.log_error("consultar_todos_processos", f"Erro ao acessar página inicial: {e}")
    
    logger.log_info("consultar_todos_processos", f"=== INICIANDO CONSULTA DE {len(lista_processos)} PROCESSOS ===")
    
    for i, numero_processo in enumerate(lista_processos, 1):
        for file in os.listdir('data\\temp'):
            if file.endswith('.json'):
                os.remove(os.path.join('data\\temp', file))


        logger.log_info("consultar_todos_processos", f"[{i}/{len(lista_processos)}] Consultando processo: {numero_processo}")
        
        dados_processo = None
        resultado = None
        dados = None
        data = None
        resultados = []

        dados_processo = consultar_processo_individual(numero_processo, session)

        if dados_processo is None:
            logger.log_error("consultar_todos_processos", f"❌ Processo sem retorno {numero_processo}")
            continue
        if dados_processo:
            resultado = {
                'numero_processo': numero_processo,
                'data_consulta': datetime.now().isoformat(),
                'dados': dados_processo,
                'status': 'sucesso'
            }
            resultados.append(resultado)
            logger.log_success("consultar_todos_processos", f"✅ Processo {numero_processo} adicionado aos resultados")
        else:
            resultado = {
                'numero_processo': numero_processo,
                'data_consulta': datetime.now().isoformat(),
                'dados': None,
                'status': 'erro'
            }
            resultados.append(resultado)
            logger.log_error("consultar_todos_processos", f"❌ Falha ao consultar processo {numero_processo}")
        
        # Aguardar entre consultas para evitar sobrecarga
        if i < len(lista_processos):
            logger.log_info("consultar_todos_processos", "Aguardando 1 segundos antes da próxima consulta...")
            time.sleep(1)
    
        # Save results to JSON
        if salvar_json:
            nome_arquivo = f"data/temp/processosrelacao.json"
            #se arquivo existe, apague
            if os.path.exists(nome_arquivo):
                os.remove(nome_arquivo)
                logger.log_info("consultar_todos_processos", f"Arquivo existente removido: {nome_arquivo}")
            
            # Create directory if it doesn't exist
            os.makedirs("data/temp", exist_ok=True)
            
            try:
                with open(nome_arquivo, 'w', encoding='utf-8') as f:
                    json.dump(resultados, f, ensure_ascii=False, indent=2)
                logger.log_success("consultar_todos_processos", f"✅ Resultados salvos em: {nome_arquivo}")
            except Exception as e:
                logger.log_error("consultar_todos_processos", f"❌ Erro ao salvar arquivo JSON: {e}")

        if os.path.exists('data\\temp\\processosrelacao.json'):
            try:
                with open('data\\temp\\processosrelacao.json', 'r', encoding='utf-8') as f:

                        data = json.load(f)
                        for item in data:
                            # Remover o .strip() pois item já é um dicionário, não uma string
                            if item:  # Verificar se o item não está vazio
                                # Não precisa fazer json.loads() novamente, pois item já é um dicionário

                                #{'processo': {'numeroProcessoEditado': '10980.919101/2020-75', 'dataProtocolo': 20201126, 'numeroDocumentoOrigem': '', 'nomeProcedencia': '', 'nomeAssunto': 'DCOMP - ELETRONICO - SALDO NEGATIVO DO IRPJ', 'nomeInteressado': 'SISTECHNE - INTERTECHNE SISTEMAS S.A.', 'indicadorCpfCnpj': 2, 'numeroCpfCnpj': 95391462000193, 'nomeOrgaoOrigem': 'PROTOCOLO GERAL DA SAMF-PR', 'nomeOrgaoDestino': 'ARQUIVO ELETRONICO DO SIEF-9 RF-SRF', 'nomeOutroOrgao': '', 'dataMovimento': 20201126, 'numeroSequencia': '0001', 'numeroRelacao': '00000', 'situacao': 'ARQUIVADO                        ', 'siglaUfMovimento': 'PR', 'indicadorVirtual': 'V', 'indicadorProfisc': '', 'indicadorEProcesso': 0, ...}, 'movimentos': [], 'posicionamentos': [], 'mensagemErroMovimento': 'Processo esta na primeira distribuicao', 'mensagemErroPosicionamento': None}


                                dados = item
                                processo = dados['dados']['processo']
                                documento = processo['numeroCpfCnpj']

                                tipo = ""
                                tipo = processo['indicadorVirtual']
                                if tipo == 'D':
                                    tipo = "DIGITAL"
                                if tipo == 'V':
                                    tipo = "VIRTUAL"
                
                                nomeinteressado = processo['nomeInteressado']

                                data_protocolo = str(processo['dataProtocolo'])
                                try:                        
                                    data_protocolo = datetime.strptime(data_protocolo, '%Y%m%d').strftime('%Y-%m-%d')
                                except:
                                    data_protocolo = '1900-01-01'


                                data_movimento = str(processo['dataMovimento'])
                                try:                        
                                    data_movimento = datetime.strptime(data_movimento, '%Y%m%d').strftime('%Y-%m-%d')
                                except:
                                    data_movimento = '1900-01-01'   
                                
                                situacao = ""
                                situacao = processo["situacao"]
                                #se situacao tem espaco ou ' remover
                                if ' ' in situacao:
                                    situacao = situacao.replace(' ', '')
                                if "'" in situacao:
                                    situacao = situacao.replace("'", "")

                                uf = processo["siglaUfMovimento"]
                                documento_origem = processo["numeroDocumentoOrigem"]
                                procedencia = processo["nomeProcedencia"]
                                nome_assunto = processo["nomeAssunto"]
                                sistema_profisc = processo["indicadorProfisc"]

                                sistema_processo = ""
                                sistema_processo = processo["indicadorEProcesso"]

                                if sistema_processo == 1:
                                    sistema_processo = 'SIM'
                                if sistema_processo == 0:
                                    sistema_processo = 'NAO'

                                sistema_sief = ""
                                sistema_sief = processo["indicadorSief"]

                                if sistema_sief == 2:
                                    sistema_sief = 'Protocolizado e Cadastrado pelo SIEF'
                                if sistema_sief == 1:
                                    sistema_sief = 'Aguardando Cadastramento SIEF'


                                orgao_origem = processo["nomeOrgaoOrigem"]
                                orgao_destino = processo["nomeOrgaoDestino"]
                                orgao_outro = processo["nomeOutroOrgao"]
                                sequencia = int(processo["numeroSequencia"])
                                relacao = int(processo["numeroRelacao"])
                                data_disjuntada = processo["dataDisjuntada"]
                                numero_sequencia_disjuntada = processo["numeroSequenciaDisjuntada"]
                                numero_aviso = processo["numeroAviso"]
                                numero_processo_principal = processo["numeroProcessoPrincipal"]
                                nome_orgao_disjuntada = processo["nomeOrgaoDisjuntada"]
                                codigo_tipo_movimento_processo = processo["codigoTipoMovimentoProcesso"]
                                status = dados["status"]
                                status_mensagem = f'Coletado em {datetime.now().strftime("%d/%m/%Y")}'
                                numero_processo = dados["numero_processo"]
                                
                                query = f"""
                                        USE [{dbname}]
                                        UPDATE [{dbschema}].[{tabrelprocessname}]
                                        SET [id_controle_dado] = 0
                                            ,[documento] = '{documento}'
                                            ,[nome_interessado] = '{nomeinteressado}'
                                            ,[data_protocolo] = '{data_protocolo}'
                                            ,[situacao] = '{situacao}'
                                            ,[uf] = '{uf}'
                                            ,[documento_origem] = '{documento_origem}'
                                            ,[procedencia] = '{procedencia}'
                                            ,[nome_assunto] = '{nome_assunto}'
                                            ,[tipo] = '{tipo}'
                                            ,[sistema_profisc] = '{sistema_profisc}'
                                            ,[sistema_processo] = '{sistema_processo}'
                                            ,[sistema_sief] = '{sistema_sief}'
                                            ,[orgao_origem] = '{orgao_origem}'
                                            ,[orgao_destino] = '{orgao_destino}'
                                            ,[orgao_outro] = '{orgao_outro}'
                                            ,[data_movimentado] = '{data_movimento}'
                                            ,[sequencia] = {sequencia}
                                            ,[relacao] = {relacao}
                                            ,[data_disjuntada] = '{data_disjuntada}'
                                            ,[numero_sequencia_disjuntada] = '{numero_sequencia_disjuntada}'
                                            ,[numero_aviso] = {numero_aviso}
                                            ,[numero_processo_principal] = '{numero_processo_principal}'
                                            ,[nome_orgao_disjuntada] = '{nome_orgao_disjuntada}'
                                            ,[codigo_tipo_movimento_processo] = '{codigo_tipo_movimento_processo}'
                                            ,[status] = '{status}'
                                            ,[status_mensagem] = '{status_mensagem}'
                                        WHERE [numero_processo] = '{numero_processo}'
                                """
                                db_manager.execute_query(query, commit=True)
                            #Se Update deu certo E status = sucesso enviar o mesmo para a tabela relatorios_processo_infov2, considerando se existe: update se nao existe insert
                            if status == 'sucesso':
                                query = f"""
                                        USE [{dbname}]
                                        IF EXISTS (SELECT 1 FROM [{dbschema}].[relatorios_processo_infov2] WHERE [numero_processo] = '{numero_processo}')
                                        BEGIN
                                            UPDATE [{dbschema}].[relatorios_processo_infov2]
                                            SET [id_controle_dado] = 0
                                                ,[documento] = '{documento}'
                                                ,[nome_interessado] = '{nomeinteressado}'
                                                ,[data_protocolo] = '{data_protocolo}'
                                                ,[situacao] = '{situacao}'
                                                ,[uf] = '{uf}'
                                                ,[numero_processo] = '{numero_processo}'
                                                ,[documento_origem] = '{documento_origem}'
                                                ,[procedencia] = '{procedencia}'
                                                ,[nome_assunto] = '{nome_assunto}'
                                                ,[tipo] = '{tipo}'
                                                ,[sistema_profisc] = '{sistema_profisc}'
                                                ,[sistema_processo] = '{sistema_processo}'
                                                ,[sistema_sief] = '{sistema_sief}'
                                                ,[orgao_origem] = '{orgao_origem}'
                                                ,[orgao_destino] = '{orgao_destino}'
                                                ,[orgao_outro] = '{orgao_outro}'
                                                ,[data_movimentado] = '{data_movimento}'
                                                ,[sequencia] = {sequencia}
                                                ,[relacao] = {relacao}
                                                ,[data_disjuntada] = '{data_disjuntada}'
                                                ,[numero_sequencia_disjuntada] = '{numero_sequencia_disjuntada}'
                                                ,[numero_aviso] = '{numero_aviso}'
                                                ,[numero_processo_principal] = '{numero_processo_principal}'
                                                ,[nome_orgao_disjuntada] = '{nome_orgao_disjuntada}'
                                                ,[codigo_tipo_movimento_processo] = '{codigo_tipo_movimento_processo}'
                                                ,[status] = '{status}'
                                                ,[status_mensagem] = '{status_mensagem}'
                                            WHERE [numero_processo] = '{numero_processo}'
                                        END
                                        ELSE
                                        BEGIN
                                            INSERT INTO [{dbschema}].[relatorios_processo_infov2]
                                            ([id_controle_dado]
                                            ,[documento]
                                            ,[nome_interessado]
                                            ,[data_protocolo]
                                            ,[situacao]
                                            ,[uf]
                                            ,[numero_processo]
                                            ,[documento_origem]
                                            ,[procedencia]
                                            ,[nome_assunto]
                                            ,[tipo]
                                            ,[sistema_profisc]
                                            ,[sistema_processo]
                                            ,[sistema_sief]
                                            ,[orgao_origem]
                                            ,[orgao_destino]
                                            ,[orgao_outro]
                                            ,[data_movimentado]
                                            ,[sequencia]
                                            ,[relacao]
                                            ,[data_disjuntada]
                                            ,[numero_sequencia_disjuntada]
                                            ,[numero_aviso]
                                            ,[numero_processo_principal]
                                            ,[nome_orgao_disjuntada]
                                            ,[codigo_tipo_movimento_processo]
                                            ,[created_at]
                                            ,[status]
                                            ,[status_mensagem])
                                            VALUES
                                            (0
                                            ,'{documento}'
                                            ,'{nomeinteressado}'
                                            ,'{data_protocolo}'
                                            ,'{situacao}'
                                            ,'{uf}'
                                            ,'{numero_processo}'
                                            ,'{documento_origem}'
                                            ,'{procedencia}'
                                            ,'{nome_assunto}'
                                            ,NULL
                                            ,'{sistema_profisc}'
                                            ,'{sistema_processo}'
                                            ,'{sistema_sief}'
                                            ,'{orgao_origem}'
                                            ,'{orgao_destino}'
                                            ,'{orgao_outro}'
                                            ,'{data_movimento}'
                                            ,{sequencia}
                                            ,{relacao}
                                            ,'{data_disjuntada}'
                                            ,'{numero_sequencia_disjuntada}'
                                            ,'{numero_aviso}'
                                            ,'{numero_processo_principal}'
                                            ,'{nome_orgao_disjuntada}'
                                            ,'{codigo_tipo_movimento_processo}'
                                            ,GETDATE()
                                            ,'{status}'
                                            ,'{status_mensagem}')
                                        END
                                """
                                db_manager.execute_query(query, commit=True)
            except Exception as e:
                logger.log_error("consultar_todos_processos", f"❌ Falha ao consultar processo {e} - Line: {traceback.extract_tb(e.__traceback__)[0][1]} ")

    # Estatísticas finais
    sucessos = len([r for r in resultados if r['status'] == 'sucesso'])
    erros = len([r for r in resultados if r['status'] == 'erro'])
    
    logger.log_info("consultar_todos_processos", "=== RESUMO FINAL ===")
    logger.log_info("consultar_todos_processos", f"Total de processos: {len(lista_processos)}")
    logger.log_info("consultar_todos_processos", f"Sucessos: {sucessos}")
    logger.log_info("consultar_todos_processos", f"Erros: {erros}")
    # Calculate success rate safely handling zero division
    total_processos = len(lista_processos)
    if total_processos > 0:
        sucessos = sum(1 for r in resultados if r['status'] == 'sucesso')
        taxa_sucesso = (sucessos/total_processos) * 100
        logger.log_info("consultar_todos_processos", f"Taxa de sucesso: {taxa_sucesso:.1f}%")
    else:
        logger.log_info("consultar_todos_processos", "Nenhum processo para calcular taxa de sucesso")
    
    return resultados

def extrair_numeros_processos_do_resultado(resultado_consulta):
    """
    Extrai os números dos processos do resultado da consulta principal
    """
    if not resultado_consulta:
        return []
    
    processos = resultado_consulta.get("processos", [])
    numeros_processos = []
    
    for processo in processos:
        numero = processo.get("numeroProcessoPrincipal")
        if numero:
            numeros_processos.append(numero)
    
    return numeros_processos

def submain(lista_processos):
    # Exemplo de uso - lista de processos para testar
    processos_exemplo = lista_processos
    
    logger.log_info("submain", "=== TESTE DE CONSULTA DE PROCESSOS INDIVIDUAIS ===")
    resultados = consultar_todos_processos(lista_processos)
    
    logger.log_info("submain", f"Consulta finalizada. {len(resultados) if resultados else 0} resultados obtidos.")