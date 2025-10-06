from src.services.logsrc import EnhancedLogger
from src.services.databasesrc import DBManager
from src.services.configsrc import config

from src.tasks.executarhelloworld import ExecutarHelloWorld
from src.tasks.planilha_processar import PlanilhaProcessar
from src.tasks.tabela_documentos_ged import TabelaDocumentosGed
from src.tasks.sharepoint_acesso import SharePointAcesso
from src.tasks.extrair_zip import ExtrairZip
from src.tasks.qualis_acesso_portal import QualisAcessoPortal



class Services:
    def __init__(self):
        self.dcConfig, self.dcParameter = config.loadconfig()
        self.logger = EnhancedLogger(self.dcConfig, self.dcParameter)
        self.db_manager = DBManager(self.dcConfig, self.dcParameter, self.logger)

        self.executarhelloworld = ExecutarHelloWorld(self, self.logger)
        self.planilha_processar = PlanilhaProcessar(self.logger, self.dcParameter)
        self.tabela_documentos_ged = TabelaDocumentosGed(self.logger, self.dcConfig, self.dcParameter, self.db_manager)
        self.sharepoint_acesso = SharePointAcesso(self, self.logger, self.dcConfig, self.dcParameter, self.db_manager)
        self.extrair_zip = ExtrairZip(self.logger, self.dcParameter)
        self.qualis_acesso_portal = QualisAcessoPortal(self, self.logger)
