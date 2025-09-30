from src.services.logsrc import EnhancedLogger
from src.services.databasesrc import DBManager
from src.services.configsrc import config
from src.tasks.tabela_registros import TabelaRegistros
from src.tasks.tabela_controle_dados import TabelaControleDados
from src.tasks.planilha_processar import PlanilhaProcessar
from src.tasks.comprot import Comprot

class Services:
    def __init__(self):
        self.dcConfig, self.dcParameter = config.loadconfig()
        self.logger = EnhancedLogger(self.dcConfig, self.dcParameter)
        self.db_manager = DBManager(self.dcConfig, self.dcParameter, self.logger)
        self.tabela_registros = TabelaRegistros(self.dcConfig, self.dcParameter, self.logger, self.db_manager)
        self.tabela_controle_dados = TabelaControleDados(self.dcConfig, self.dcParameter, self.logger, self.db_manager)
        self.planilha_processar = PlanilhaProcessar(self.dcConfig, self.dcParameter, self.logger, self.db_manager)
        self.comprot = Comprot(self.dcConfig, self.dcParameter, self.logger, self.db_manager)
        
        
        
        
        
        