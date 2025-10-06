import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright


class SharePointAcesso:
    def __init__(self, services, logger, dcConfig, dcParameter, db_manager):
        self.services = services
        self.logger = logger
        self.playwright = None
        self.navegador = None
        self.context = None
        self.pagina = None

        self.dcConfig = dcConfig
        self.dcParameter = dcParameter
        self.db_manager = db_manager

        self.databasename = self.dcConfig['databasename']
        self.schema = self.dcConfig['dbschema']
        self.tabdocumentosged = self.dcConfig['tabdocumentosged']

        self.folderdownloads = self.dcParameter['folderdownloads']

        # Carregar variáveis de ambiente
        load_dotenv()

    # -----------------------------
    # Métodos auxiliares
    # -----------------------------
    def _slugify(self, text: str) -> str:
        if text.startswith("https://"):
            text = text.replace("https://", "")
        s = text.lower()
        sanitized = [ch if ch.isalnum() else '-' for ch in s]
        slug = ''.join(sanitized)
        while '--' in slug:
            slug = slug.replace('--', '-')
        slug = slug.strip('-')
        return slug[:100]


    def _inicializar_navegador(self):
        try:
            self.logger.log_info('_inicializar_navegador', "Abrindo o navegador")
            self.playwright = sync_playwright().start()
            self.navegador = self.playwright.chromium.launch(headless=False)
            self.context = self.navegador.new_context(
                viewport={"width": 1280, "height": 1024},
                accept_downloads=True
            )
            self.pagina = self.context.new_page()
            return True
        except Exception as e:
            self.logger.log_error('_inicializar_navegador', f"Erro ao inicializar navegador: {e}")
            return False


    def _acessar_sharepoint(self, url_sharepoint: str):
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
        try:
            self.logger.log_info('_efetuar_login', "Efetuando login...")

            campo_email = self.pagina.get_by_role("textbox", name="someone@example.com")
            if campo_email:
                campo_email.fill("robo.rpa@fibrasa.com.br")
                self.pagina.get_by_role("button", name="Avançar").click()

                campo_senha = self.pagina.get_by_role("textbox", name="Insira a senha para robo.rpa@")
                campo_senha.fill("<wqXD0J3[3rw")
                self.pagina.get_by_role("button", name="Entrar").click()

                chk_nao_mostrar_novamente = self.pagina.get_by_role("checkbox", name="Não mostrar isso novamente")
                chk_nao_mostrar_novamente.click()
                self.pagina.get_by_role("button", name="Sim").click()

                self.logger.log_info('_efetuar_login', "Login efetuado com sucesso!")
                return True
            return False
        except Exception as e:
            self.logger.log_error('_efetuar_login', f"Erro ao efetuar login: {e}")
            return False


    def _finalizar_navegador(self):
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


    def _atualizar_status(self, link: str, status: str, mensagem: str = None):
        """Atualiza o status do documento no banco de dados."""
        try:
            self.services.tabela_documentos_ged.atualizar_status(link, status)
            if mensagem:
                self.logger.log_info('_atualizar_status', f"Status do link {link} atualizado para {status} ({mensagem})")
            else:
                self.logger.log_info('_atualizar_status', f"Status do link {link} atualizado para {status}")
        except Exception as e:
            self.logger.log_error('_atualizar_status', f"Erro ao atualizar status do link {link} para {status}: {e}")


    # -----------------------------
    # Processamento de cada link
    # -----------------------------
    def _processar_link(self, idx: int, link: str, total_links: int) -> bool:
        """Processa um único link: acessa, baixa, salva e extrai.
        Retorna True se concluir download e extração com sucesso, False caso contrário.
        """
        try:
            # Acessa link, se não for o primeiro
            if idx > 0:
                self.logger.log_info('_processar_link', f"Acessando link {idx+1}/{total_links}")
                if not self._acessar_sharepoint(link):
                    self._atualizar_status(link, "FALHOU", "Falha ao acessar SharePoint")
                    return False
                self.pagina.wait_for_load_state("networkidle", timeout=120000)

            # Selecionar todas as linhas
            seletor = self.pagina.get_by_role("gridcell", name="Selecionar todas as linhas")
            seletor.click()
            self.pagina.wait_for_load_state("domcontentloaded", timeout=60000)

            # Dispara o download
            with self.pagina.expect_download(timeout=120000) as download_info:
                self.logger.log_info('_processar_link', f"Realizando o download do link {idx+1}/{total_links}...")
                btn_baixar = self.pagina.get_by_role("menuitem", name="Baixar")
                btn_baixar.wait_for(state="visible", timeout=60000)
                btn_baixar.click()
            download = download_info.value

            # Cria pasta e salva arquivo
            link_slug = self._slugify(link)
            download_dir = os.path.join(self.folderdownloads, link_slug)
            os.makedirs(download_dir, exist_ok=True)
            destino_final = os.path.join(download_dir, download.suggested_filename)
            download.save_as(destino_final)

            # Log tamanho do arquivo
            try:
                file_size = os.path.getsize(destino_final)
                self.logger.log_info('_processar_link', f"Download salvo em: {destino_final} (tamanho: {file_size} bytes)")
            except Exception:
                self.logger.log_warning('_processar_link', f"Download salvo em: {destino_final}, mas não foi possível obter tamanho")

            # Extrai zips se houver
            try:
                self.services.extrair_zip.extrair_zips_em_pasta(download_dir)
                self.logger.log_info('_processar_link', f"Extração concluída na pasta: {download_dir}")
            except Exception as e_ext:
                self.logger.log_error('_processar_link', f"Erro ao extrair zips em {download_dir}: {e_ext}")

            # ✅ Atualiza status PROCESSADO após download e extração
            self._atualizar_status(link, "PROCESSADO")

            # Ações posteriores opcionais (waits, verificações)
            try:
                self.pagina.wait_for_load_state("networkidle", timeout=30000)
            except Exception as e_wait:
                # Apenas loga, não altera status
                self.logger.log_warning('_processar_link', f"Timeout ou atraso após download para link {idx+1}/{total_links}: {e_wait}")

            return True

        except Exception as e:
            # Se falhar antes do download, marca como FALHOU
            self.logger.log_error('_processar_link', f"Erro ao processar link {idx+1}/{total_links}: {link} - {e}")
            self._atualizar_status(link, "FALHOU", f"Erro: {str(e)}")
            return False

    # -----------------------------
    # Método principal
    # -----------------------------
    def executar_acesso_sharepoint(self, status: str = 'PENDENTE', limite: int = None, max_tentativas: int = 3, max_tentativas_link: int = 3):
        """Executa o acesso ao SharePoint para cada link armazenado na tabela documentos_ged"""
        for tentativa in range(max_tentativas):
            self.logger.log_info('executar_acesso_sharepoint', f"=== TENTATIVA {tentativa + 1} DE {max_tentativas} ===")
            try:
                if not self._inicializar_navegador():
                    continue

                links = self.services.tabela_documentos_ged.obter_links_do_banco(status=status, limite=limite)
                if not links:
                    self.logger.log_warning('executar_acesso_sharepoint', 'Nenhum link encontrado para processamento')
                    return False

                primeiro_link = links[0]
                if not self._acessar_sharepoint(primeiro_link):
                    raise Exception("Falha ao acessar primeiro link")
                self.pagina.wait_for_load_state("networkidle")

                if not self._efetuar_login():
                    raise Exception("Falha ao efetuar login")
                self.pagina.wait_for_load_state("networkidle")

                # Itera pelos links
                for idx, link in enumerate(links):
                    sucesso = False
                    for tentativa_link in range(max_tentativas_link):
                        self.logger.log_info(
                            'executar_acesso_sharepoint',
                            f"Processando link {idx+1}/{len(links)} (tentativa {tentativa_link+1}/{max_tentativas_link})"
                        )
                        if self._processar_link(idx, link, len(links)):
                            sucesso = True
                            break
                        else:
                            self.logger.log_warning(
                                'executar_acesso_sharepoint',
                                f"Tentativa {tentativa_link+1}/{max_tentativas_link} falhou para link {idx+1}/{len(links)}"
                            )
                            self.pagina.wait_for_load_state("networkidle")

                    if not sucesso:
                        self.logger.log_error(
                            'executar_acesso_sharepoint',
                            f"Link {idx+1}/{len(links)} ignorado após {max_tentativas_link} falhas: {link}"
                        )
                        # Atualiza status para FALHOU quando máximo de tentativas atingido
                        self._atualizar_status(link, "FALHOU", "Máximo de tentativas atingido")

                # Finaliza navegador após processar todos os links
                self._finalizar_navegador()
                return True

            except Exception as e:
                self.logger.log_error('executar_acesso_sharepoint',
                                    f"Erro geral na tentativa {tentativa+1}: {e}")
                self._finalizar_navegador()
                continue

        # Se todas as tentativas externas falharem
        return False
