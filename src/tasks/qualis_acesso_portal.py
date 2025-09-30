from venv import logger
import requests
import datetime
import time
import json
import os
import math
from dotenv import load_dotenv
from src.services.configsrc import config
from src.services.logsrc import EnhancedLogger

from playwright.sync_api import sync_playwright


# Carregar variáveis de ambiente
load_dotenv()

#carregar o logger 
dcConfig, dcParameter = config.loadconfig()
logger = EnhancedLogger(dcConfig, dcParameter)


# def consultar_processos_comprot
def acessar_portal_qualis(unidade, max_tentativas=3, idultimoprocesso=None):    
    for tentativa in range(max_tentativas):
        logger.log_info('acessar_portal_qualis', f"=== TENTATIVA {tentativa + 1} DE {max_tentativas} ===")
        
        try:
            logger.log_info('acessar_portal_qualis', "Abrindo o navegador")
            navegador = pw.chromium.launch(headless=False, args=["--start-maximized"]) # headless permite ver o navegador abrindo

            logger.log_info('acessar_portal_qualis', "Acessando página inicial")
            url_qualis = os.getenv('URL_QUALIS')
            pagina.goto(url_qualis)
            pagina.get_by_text("MS-SymbolLockup Login com").click()

            logger.log_info('acessar_portal_qualis', "Efetuando login")
            campo_email = pagina.get_by_role("textbox", name="someone@example.com")
            if campo_email:
                campo_email.fill("robo.rpa@fibrasa.com.br")
                
                btn_avancar = pagina.get_by_role("button", name="Avançar")
                btn_avancar.click()

                campo_senha = pagina.get_by_role("textbox", name="Insira a senha para robo.rpa@")
                campo_senha.fill("<wqXD0J3[3rw")

                btn_entrar = pagina.get_by_role("button", name="Entrar")
                btn_entrar.click()

                chk_nao_mostrar_novamente = pagina.get_by_role("checkbox", name="Não mostrar isso novamente")
                chk_nao_mostrar_novamente.click()

                btn_sim = pagina.get_by_role("button", name="Sim")
                btn_sim.click()


            response_inicial = session.get(url_inicial, headers=headers_base, verify=False)
            logger.log_info('consultar_processos_comprot', f"Status página inicial: {response_inicial.status_code}")
            logger.log_info('consultar_processos_comprot', f"Cookies obtidos: {session.cookies}")
            
            # Passo 2: 
            logger.log_info('consultar_processos_comprot', "Passo 3: Consultando processos...")
            timestamp_atual = DatetimeUtils.timestamp_atual()
            
            # Converter datas para timestamp
            data_inicial_formatada = data_inicial.replace('/', '-')
            data_final_formatada = data_final.replace('/', '-')
            
            # Converter DD-MM-YYYY para YYYY-MM-DD
            dia_i, mes_i, ano_i = data_inicial_formatada.split('-')
            dia_f, mes_f, ano_f = data_final_formatada.split('-')
            
            timestamp_data_de = DatetimeUtils.data_para_timestamp(f"{ano_i}-{mes_i}-{dia_i} 00:00:00")
            timestamp_data_ate = DatetimeUtils.data_para_timestamp(f"{ano_f}-{mes_f}-{dia_f} 00:00:00")
            
            url_api = os.getenv('URL_API_PROCESSO')
            logger.log_info('consultar_processos_comprot', f"URL API: {url_api}")
            
            if idultimoprocesso:
                url_get = f'{url_api}?cpfCnpjComMascara={cnpj_formatado}&cpfCnpj={cnpj}&nomeInteressado=&tipoPesquisa=cnpj&dataInicial={timestamp_data_de}&dataFinal={timestamp_data_ate}&numeroUltimoProcesso={idultimoprocesso}&_={timestamp_atual}'
            else:
                url_get = f'{url_api}?cpfCnpjComMascara={cnpj_formatado}&cpfCnpj={cnpj}&nomeInteressado=&tipoPesquisa=cnpj&dataInicial={timestamp_data_de}&dataFinal={timestamp_data_ate}&numeroUltimoProcesso=&_={timestamp_atual}'

            headers_get = {
                'Accept': '*/*',
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Pragma': 'no-cache',
                'Referer': url_inicial,
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest',
                'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'
            }

            #Captura dados de Processo 
            response = session.get(url_get, headers=headers_get, verify=False)

            #capturar valor de totalDeProcessosEncontrados
            if response.status_code == 204:
                logger.log_info('consultar_processos_comprot', "Nenhum processo encontrado")
                return 'SemProcesso', 0, 0, ''

            if response.status_code == 200:
                logger.log_error('consultar_processos_comprot', f"Erro na consulta: {response.status_code}")

            response_data = response.json()
            numprocessostotal = response_data.get('totalDeProcessosEncontrados', 0)

            #quantidade de paginas *cada pagina tem at[e 30] processos
            numpaginas = math.ceil(numprocessostotal / 30)
            logger.log_info('consultar_processos_comprot', f"Quantidade de processos: {numprocessostotal}")
            logger.log_info('consultar_processos_comprot', f"Quantidade de páginas: {numpaginas}")
            
            #numero do ultimo processo - verificar se existe processos na resposta
            processos = response_data.get('processos', [])
            if processos and isinstance(processos, list) and len(processos) > 0:
                idultimoprocesso = processos[-1].get('numeroProcessoPrincipal', '')
                logger.log_info('consultar_processos_comprot', f"ID do último processo: {idultimoprocesso}")
            else:
                logger.log_warning('consultar_processos_comprot', "Nenhum processo encontrado na resposta ou estrutura inválida")
                idultimoprocesso = ''

            #salvar resonse como um arquivo json em data\temp
            try:
                timestamparquivo = DatetimeUtils.timestamp_atual()
                with open(f'data\\temp\\{cnpj}{timestamparquivo}.json', 'w') as f:
                    json.dump(response.json(), f)
                logger.log_info('consultar_processos_comprot', "Response salvo em data\\temp\\relprocessos.json")
            except Exception as e:
                logger.log_error('consultar_processos_comprot', f"Erro ao salvar response: {e}")
            
            logger.log_info('consultar_processos_comprot', f"Status: {response.status_code}")
            
            # Implementar lógica baseada no status code
            if response.status_code == 200:
                logger.log_success('consultar_processos_comprot', "=== SUCESSO! Status 200 ===")
                processoupdated = processar_resposta_sucesso(response)
                return processoupdated, numpaginas, numprocessostotal, idultimoprocesso
            elif response.status_code == 422:
                logger.log_warning('consultar_processos_comprot', "=== Status 422 - Tentando novamente ===")
                if tentativa < max_tentativas - 1:
                    logger.log_info('consultar_processos_comprot', "Aguardando 5 segundos antes da próxima tentativa...")
                    time.sleep(5)
                    continue
                else:
                    logger.log_error('consultar_processos_comprot', "Máximo de tentativas atingido com status 422")
                    processoupdated = 'SemProcesso'
                    return processoupdated, 0, 0, ''
            elif response.status_code == 204:
                # codigfo 204 consta como sem processo
                logger.log_info('consultar_processos_comprot', "Status 204 - Sem processos encontrados")
                processoupdated = 'SemProcesso'
                return processoupdated, 0, 0, ''
            else:
                logger.log_error('consultar_processos_comprot', f"=== Status {response.status_code} - Erro não tratado ===")
                if tentativa < max_tentativas - 1:
                    logger.log_info('consultar_processos_comprot', "Tentando novamente...")
                    time.sleep(5)
                    continue
                else:
                    logger.log_error('consultar_processos_comprot', "Máximo de tentativas atingido")
                    processoupdated = 'Pendente'
                    return processoupdated, 0, 0, ''
                
        except requests.exceptions.RequestException as e:
            logger.log_error('consultar_processos_comprot', f"Erro ao fazer request: {e}")
            if tentativa < max_tentativas - 1:
                logger.log_info('consultar_processos_comprot', "Tentando novamente...")
                time.sleep(5)
                continue
            else:
                processoupdated = 'Pendente'
                return processoupdated, 0, 0, ''
        except Exception as e:
            logger.log_error('consultar_processos_comprot', f"Erro geral: {e}")
            if tentativa < max_tentativas - 1:
                logger.log_info('consultar_processos_comprot', "Tentando novamente...")
                time.sleep(5)
                continue
            else:
                processoupdated = 'Pendente'
                return processoupdated, 0, 0, ''
    
    logger.log_error('consultar_processos_comprot', "=== TODAS AS TENTATIVAS FALHARAM ===")
    processoupdated = 'Pendente'

    return processoupdated, 0, 0, ''
