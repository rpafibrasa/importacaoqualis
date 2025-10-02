import time
import os

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright


class QualisAcessoPortal:
    def __init__(self, services, logger):
        """
        Inicializa a classe QualisAcessoPortal
        
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
        
        # Carregar variáveis de ambiente
        load_dotenv()

    
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

    
    def _acessar_portal(self):
        """Método privado para acessar o portal Qualis"""
        try:
            self.logger.log_info('_acessar_portal', "Acessando página inicial")

            url_qualis = os.getenv('URL_QUALIS')
            self.pagina.goto(url_qualis)

            self.pagina.get_by_text("MS-SymbolLockup Login com").click()
            return True
        except Exception as e:
            self.logger.log_error('_acessar_portal', f"Erro ao acessar portal: {e}")
            return False

    
    def _efetuar_login(self):
        """Método privado para efetuar login no portal"""
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

    
    def executar_acesso_portal_qualis(self, max_tentativas=3,):
        """Método público que executa todo o processo de acesso ao portal Qualis"""
        for tentativa in range(max_tentativas):
            self.logger.log_info('executar_acesso_portal_qualis', f"=== TENTATIVA {tentativa + 1} DE {max_tentativas} ===")
            
            try:
                # Executar métodos privados na sequência
                if not self._inicializar_navegador():
                    continue
                
                if not self._acessar_portal():
                    continue
                
                if not self._efetuar_login():
                    continue

                # Finalizar navegador
                self._finalizar_navegador()
                return True

            except Exception as e:
                self.logger.log_error('executar_acesso_portal_qualis', f"Erro geral: {e}")
                self._finalizar_navegador()

                if tentativa < max_tentativas - 1:
                    self.logger.log_info('executar_acesso_portal_qualis', "Tentando novamente...")
                    time.sleep(5)
                    continue
                else:
                    return None
        
        self.logger.log_error('executar_acesso_portal_qualis', "=== TODAS AS TENTATIVAS FALHARAM ===")
        return None
