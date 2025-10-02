import logging
import psycopg2
from psycopg2 import sql
from typing import Optional, Tuple, Any
import traceback
import os
from urllib.parse import urlparse

class DBManager:
    """
    Singleton para gerenciar a conexão com o banco de dados.
    Responsável por estabelecer e manter a conexão com o SQL Server.
    """
    _instance: Optional['DBManager'] = None
    _connection: Optional[Any] = None
    _logger: Optional[Any] = None
    _dcConfig: Optional[dict] = None
    _dcParameter: Optional[dict] = None
    
    def __new__(cls, dcConfig: dict = None, dcParameter: dict = None, logger = None) -> 'DBManager':
        if cls._instance is None:
            cls._instance = super(DBManager, cls).__new__(cls)
            cls._instance._initialize(dcConfig, dcParameter, logger)
        return cls._instance
    
    def _initialize(self, dcConfig: dict = None, dcParameter: dict = None, logger = None) -> None:
        """Inicializa o gerenciador de banco de dados."""
        self._connection = None
        if dcConfig is not None:
            DBManager._dcConfig = dcConfig
        if dcParameter is not None:
            DBManager._dcParameter = dcParameter
        if logger is not None:
            DBManager._logger = logger
        
    def initialize_logging(self, logger = None) -> Any:
        """Inicializa o logger."""
        if logger is not None:
            DBManager._logger = logger
        return DBManager._logger
    
    def connect(self) -> bool:
        """Conecta ao banco de dados PostgreSQL usando configurações do config.json."""
        logger = DBManager._logger
        
        # Se já estiver conectado, verifica se a conexão ainda está ativa
        if self._connection:
            try:
                cursor = self._connection.cursor()
                cursor.execute('SELECT 1')
                cursor.close()
                if logger:
                    logger.log_info("connect", "Usando conexão existente com o banco de dados")
                return True
            except Exception:
                # Conexão inativa, fecha para reconectar
                self._connection = None
                if logger:
                    logger.log_warning("connect", "Conexão perdida, tentando reconectar")
        
        # Obtém configurações do banco de dados do config.json
        if not DBManager._dcConfig:
            if logger:
                logger.log_error("connect", "dcConfig não foi inicializado")
            return False
            
        # Debug: verificar estrutura do dcConfig
        if logger:
            logger.log_info("connect", f"dcConfig disponível: {list(DBManager._dcConfig.keys())}")
        
        # Verificar se existe a chave 'database' ou se as configurações estão diretamente no dcConfig
        if 'database' in DBManager._dcConfig:
            db_config = DBManager._dcConfig['database']
        elif 'host' in DBManager._dcConfig:
            # Configurações estão diretamente no dcConfig
            db_config = DBManager._dcConfig
        else:
            if logger:
                logger.log_error("connect", "Configurações de banco de dados não encontradas no config.json")
            return False
        
        try:
            host = db_config.get('host')
            port = db_config.get('port', 5432)
            database = db_config.get('database')
            username = db_config.get('username')
            password = db_config.get('password')
            sslmode = db_config.get('sslmode', 'require')
            
            if not all([host, database, username, password]):
                if logger:
                    logger.log_error("connect", "Configurações de banco de dados incompletas no config.json")
                return False
            
            if logger:
                logger.log_info("connect", ":::Processo de Conexão Iniciado:::")
            
            # Estabelece a conexão usando psycopg2
            self._connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password,
                sslmode=sslmode
            )
            
            # Testando a conexão
            cursor = self._connection.cursor()
            cursor.execute('SELECT version()')
            version = cursor.fetchone()[0]
            cursor.close()
            
            if logger:
                logger.log_success("connect", f"Conectado com sucesso ao PostgreSQL: {version[:50]}")
            
            return True
        except Exception as e:
            if logger: 
                logger.log_error("db_connect", f"Falha ao conectar ao banco de dados: {str(e)}", e)
            return False
    
    def get_connection(self) -> Optional[Any]:
        """Retorna a conexão ativa."""
        return self._connection
    
    def execute_query(self, query: str, params: Optional[Tuple] = None, commit: bool = False) -> Tuple[bool, Any]:
        """
        Executa uma query no banco de dados.
        
        Args:
            query (str): Query SQL a ser executada
            params (tuple, optional): Parâmetros para a query
            commit (bool, optional): Se deve fazer commit após a execução
            
        Returns:
            tuple: (success, result/error_message)
        """
        logger = DBManager._logger
        
        if not self._connection:
            return False, "Não há conexão ativa com o banco de dados"
        
        try:
            cursor = self._connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if commit:
                self._connection.commit()
                result = cursor.rowcount
            else:
                result = cursor.fetchall()
                
            cursor.close()
            return True, result
        except Exception as e:
            error_msg = str(e)
            if logger:
                logger.log_error("execute_query", f"Erro ao executar query: {error_msg}", e)
            return False, error_msg
    
    def close(self) -> bool:
        """Fecha a conexão com o banco de dados."""
        logger = DBManager._logger
        
        if self._connection:
            try:
                self._connection.close()
                self._connection = None
                return True
            except Exception as e:
                if logger:
                    logger.log_error("db_close", f"Erro ao fechar conexão: {str(e)}")
                return False
        return True

# Função para obter a instância singleton do gerenciador de banco de dados
def get_db_manager() -> DBManager:
    """Retorna a instância do gerenciador de banco de dados."""
    return DBManager()
