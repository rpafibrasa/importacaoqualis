import time
import os

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright


class SharePointAcesso:
    def __init__(self, services, logger, dcConfig, db_manager):
        """
        Inicializa a classe SharePointAcesso
        
        Args:
            services: Instância da classe Services
            logger: Instância do logger
        """

        self.services = services
        self.logger = logger
        self.playwright = None
        self.navegador = None
        self.context = None
        self.pagina = None

        self.db_manager = db_manager
        self.dcConfig = dcConfig
        self.databasename = self.dcConfig['databasename']
        self.schema = self.dcConfig['dbschema']
        self.tabdocumentosged = self.dcConfig['tabdocumentosged']
        
        # Carregar variáveis de ambiente
        load_dotenv()


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

    
    def _inicializar_navegador(self):
        """Método privado para inicializar o navegador Playwright"""
        try:
            self.logger.log_info('_inicializar_navegador', "Abrindo o navegador")
            
            # inicia o playwright e mantém ativo
            self.playwright = sync_playwright().start()

            self.navegador = self.playwright.chromium.launch(
                headless=False,
                args=["--start-maximized"]
            )

            # contexto “limpo”, sem cache ou sessão de outro contexto.
            self.context = self.navegador.new_context(viewport=None)
            self.pagina = self.context.new_page()

            return True
        except Exception as e:
            self.logger.log_error('_inicializar_navegador', f"Erro ao inicializar navegador: {e}")
            return False

    
    def _obter_links_do_banco(self, status: str = None, limite: int = None):
        """Obtém lista de links da tabela documentos_ged"""
        if not self.confere_conexao_do_banco():
            return False

        try:
            query = f"SELECT link FROM {self.schema}.{self.tabdocumentosged}"
            params = []
            if status:
                self.logger.log_info('_obter_links_do_banco', f"Pesquisando pelo status: {status}")
                query += " WHERE status = %s"
                params.append(status)
            if limite:
                query += f" LIMIT {limite}"

            success, result = self.db_manager.execute_query(query, tuple(params) if params else None)
            if success:
                links = [row[0] for row in result if row and row[0]]
                self.logger.log_info('_obter_links_do_banco', f"{len(links)} links obtidos do banco")
                return links
            else:
                self.logger.log_error('_obter_links_do_banco', f"Falha na consulta de links: {result}")
                return []
        except Exception as e:
            self.logger.log_error('_obter_links_do_banco', f"Erro ao consultar links: {e}")
            return []

    
    def _acessar_sharepoint(self, url_sharepoint: str):
        """Método privado para acessar um link do SharePoint a partir do banco"""
        try:
            self.logger.log_info('_acessar_sharepoint', f"Acessando URL: {url_sharepoint}")
            if not url_sharepoint:
                raise ValueError("URL do SharePoint vazia")
            self.pagina.goto(url_sharepoint)
            return True
        except Exception as e:
            self.logger.log_error('_acessar_sharepoint', f"Erro ao acessar URL: {e}")
            return False

    
    def _efetuar_login(self):
        """Método privado para efetuar login no sharepoint"""
        try:
            self.logger.log_info('_efetuar_login', "Efetuando login")
            
            # Preencher email
            campo_email = self.pagina.get_by_role("textbox", name="someone@example.com")
            if campo_email:
                campo_email.fill("robo.rpa@fibrasa.com.br")
                
                # Clicar em avançar
                btn_avancar = self.pagina.get_by_role("button", name="Avançar")
                btn_avancar.click()
                
                # Preencher senha
                campo_senha = self.pagina.get_by_role("textbox", name="Insira a senha para robo.rpa@")
                campo_senha.fill("<wqXD0J3[3rw")
                
                # Clicar em entrar
                btn_entrar = self.pagina.get_by_role("button", name="Entrar")
                btn_entrar.click()
                
                # Configurações adicionais
                chk_nao_mostrar_novamente = self.pagina.get_by_role("checkbox", name="Não mostrar isso novamente")
                chk_nao_mostrar_novamente.click()
                
                btn_sim = self.pagina.get_by_role("button", name="Sim")
                btn_sim.click()
                
                return True
        except Exception as e:
            self.logger.log_error('_efetuar_login', f"Erro ao efetuar login: {e}")
            return False

    
    def executar_acesso_sharepoint(self, status: str = 'PENDENTE', limite: int = None, max_tentativas: int = 3):
        """Executa o acesso ao SharePoint para cada link armazenado na tabela documentos_ged"""
        links = self._obter_links_do_banco(status=status, limite=limite)
        if not links:
            self.logger.log_warning('executar_acesso_sharepoint', 'Nenhum link encontrado para processamento')
            return False

        for tentativa in range(max_tentativas):
            self.logger.log_info('executar_acesso_sharepoint', f"=== TENTATIVA {tentativa + 1} DE {max_tentativas} ===")
            
            try:
                # Inicializa o navegador
                if not self._inicializar_navegador():
                    continue
                
                # Acessa primeiro link e realiza login
                primeiro_link = links[0]
                if not self._acessar_sharepoint(primeiro_link):
                    raise Exception("Falha ao acessar primeiro link")
                
                if not self._efetuar_login():
                    raise Exception("Falha ao efetuar login")

                # Itera pelos demais links já com sessão autenticada
                for idx, link in enumerate(links):
                    try:
                        if idx > 0:
                            self.logger.log_info('executar_acesso_sharepoint', f"Acessando link {idx+1}/{len(links)}")
                            self._acessar_sharepoint(link)
                        # TODO: Etapas adicionais serão implementadas aqui
                        time.sleep(1)
                    except Exception as e_link:
                        self.logger.log_error('executar_acesso_sharepoint', f"Erro ao acessar link: {link} - {e_link}")

                # Finaliza navegador após processar todos os links
                self._finalizar_navegador()
                return True

            except Exception as e:
                self.logger.log_error('executar_acesso_sharepoint', f"Erro geral: {e}")
                self._finalizar_navegador()

                if tentativa < max_tentativas - 1:
                    self.logger.log_info('executar_acesso_sharepoint', "Tentando novamente...")
                    time.sleep(5)
                    continue
                else:
                    return None
        
        self.logger.log_error('executar_acesso_sharepoint', "=== TODAS AS TENTATIVAS FALHARAM ===")
        return None


    def _finalizar_navegador(self):
        """Método privado para finalizar o navegador"""
        try:
            if self.context:
                self.context.close()
            if self.navegador:
                self.navegador.close()
            if self.playwright:
                self.playwright.stop()
            self.logger.log_info('_finalizar_navegador', "Recursos finalizados com sucesso")
        except Exception as e:
            self.logger.log_error('_finalizar_navegador', f"Erro ao finalizar recursos: {e}")
