import logging
import os
import datetime
import inspect
import psutil
import pyodbc
import textwrap
import traceback
from enum import Enum
from pathlib import Path
from typing import Optional, Any


class ProcessType(str, Enum):
    ROBOTIC = "robotic"
    BUSINESS = "business"
    SYSTEM = "system"
    PROCESS = "process"

class LogStatus(str, Enum):
    FAILURE = "failure"
    SUCCESS = "success"
    WARNING = "warning"
    CRITICAL = "critical"
    INFO = "information"

class EnhancedLogger:
    """Logger avançado com suporte a arquivo, console e banco de dados."""
    
    def __init__(self, dcConfig: dict, dcParameter: dict):
        self.project_name = dcConfig['projectname']
        self.dcConfig = dcConfig
        self.dcParameter = dcParameter
        self.log_dir = dcConfig['folderlog']
        self.log_file: Optional[str] = None
        self.db_connection: Optional[Any] = None
        self.debug_mode = True
        self.bot_name = dcConfig['projectname']
        self.schema = dcConfig['dbschema']
        self.logtablename = dcConfig['logtablename']
        
        # Formato de colunas - definições de largura
        self.col_widths = {
            'timestamp': 25,    # TIMESTAMP
            'task': 15,         # TASK
            'function': 30,     # FUNCTION
            'file': 25,         # FILE
            'message': 50,      # MESSAGE
            'process_type': 15, # PROCESS_TYPE
            'status': 15        # STATUS
        }
        
        # Largura para wrap de mensagens
        self.message_width = self.col_widths['message']
        
        # Quantidade de espaço entre borda e conteúdo da coluna
        self.padding = 1
        
        # Setup inicial
        self.setup_logging()
        
        # Tentar conectar ao banco de dados
        self._try_connect_to_database()
    
    def setup_logging(self) -> None:
        try:
            """Configuração inicial do logging."""
            # Ensure log directory exists
            os.makedirs(self.log_dir, exist_ok=True)
            
            # Create log file with timestamp
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            self.log_file = os.path.join(self.log_dir, f"rpa_{self.project_name}_{timestamp}.log")
            
            # Define UTF-8 encoding for the log file
            with open(self.log_file, 'w', encoding='utf-8') as f:
                # Escreve o cabeçalho do arquivo de log
                f.write(self._create_header())
            
            # Set up standard Python logging
            logging.basicConfig(
                level=logging.DEBUG if self.debug_mode else logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.StreamHandler()
                ]
            )
            
            # Register function to add closing line on program exit
            import atexit
            atexit.register(self._add_closing_line)
        except Exception as e:
            print(e)
        
        
    def _create_separator_line(self) -> str:
        try:
            """Cria a linha separadora com + alinhado precisamente com as barras verticais."""
            separator = "+"
            for name, width in self.col_widths.items():
                # Adiciona dois traços extras para compensar o espaço de padding em cada lado
                separator += "-" * (width + self.padding * 2) + "+"
            return separator
        except Exception as e:
            print(e)

    def _create_header(self) -> str:
        try:
            """Cria o cabeçalho com alinhamento preciso e títulos melhor centralizados."""
            # Títulos das colunas com um espaço adicional em cada lado para melhor centralização
            headers = {
                'timestamp': " TIMESTAMP ",
                'task': " TASK ",
                'function': " FUNCTION ",
                'file': " FILE ",
                'message': " MESSAGE ",
                'process_type': " PROCESS_TYPE ",
                'status': " STATUS "
            }
            
            # Criar linha separadora
            separator = self._create_separator_line()
            
            # Criar linha de cabeçalho
            header_line = "|"
            for name, width in self.col_widths.items():
                # Centraliza o título na coluna
                title = headers[name]
                padding_left = self.padding + (width - len(title) + 1) // 2
                padding_right = width - (len(title) - 1) - padding_left + self.padding
                header_line += " " * padding_left + title + " " * padding_right + "|"
            
            # Monta o cabeçalho completo
            full_header = separator + "\n" + header_line + "\n" + separator + "\n"
            return full_header
        except Exception as e:
            print(e)

    def connect_to_db(self, connection) -> bool:
        """Connect to SQL Server database.
        
        Args:
            connection: Can be either a pyodbc connection object or a connection string
        """
        try:
            # Se for uma string de conexão, estabelecer conexão
            if isinstance(connection, str):
                self.db_connection = pyodbc.connect(connection) 
            else:
                # Se for um objeto de conexão, usar diretamente
                self.db_connection = connection
                
            # Criar tabela de logs se não existir
            self.create_log_table_if_not_exists()
            return True
        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            return False
        
    def create_log_table_if_not_exists(self) -> bool:
        if not self.db_connection:
            return False
        
        try:
            cursor = self.db_connection.cursor()
            
            # Create schema if it doesn't exist
            cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{self.schema}') EXEC('CREATE SCHEMA [{self.schema}]')")
            self.db_connection.commit()
            
            create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{self.logtablename}' AND xtype='U')
            CREATE TABLE [{self.schema}].[{self.logtablename}] (
                id INT IDENTITY(1,1) PRIMARY KEY,
                task_name NVARCHAR(255),
                function_name NVARCHAR(255),
                source_file NVARCHAR(255),
                cpu_usage FLOAT,
                memory_usage FLOAT,
                log_date DATE,
                log_time TIME,
                log_message NTEXT,
                process_type NVARCHAR(50),
                status NVARCHAR(50),
                created_at DATETIME DEFAULT GETDATE()
            )
            """
            cursor.execute(create_table_sql)
            self.db_connection.commit()
            cursor.close()
            return True
        except Exception as e:
            logging.error(f"Failed to create log table: {e}")
            return False

    def _get_caller_info(self, depth: int = 3) -> tuple[str, str]:
        """Get information about the caller (file, function).
        
        Args:
            depth (int): How far back in the stack to look for the caller
        
        Returns:
            tuple: (filename, function_name)
        """
        try:
            # Pega a stack de chamadas até o depth desejado
            frame = inspect.currentframe()
            for _ in range(depth):
                if frame.f_back is None:
                    break
                frame = frame.f_back
            
            # Extrai informações do frame
            if frame:
                frame_info = inspect.getframeinfo(frame)
                filename = os.path.basename(frame_info.filename)
                function_name = frame_info.function
                return filename, function_name
        except Exception:
            pass
        
        return "unknown.py", "unknown"
        
    def log_entry(self, function_name: str, log_message: str,
                process_type: ProcessType = ProcessType.SYSTEM,
                status: LogStatus = LogStatus.INFO,
                task_name: Optional[str] = None,
                extra_data: Optional[Any] = None) -> None:
        """Log a new entry to both file and database with precise alignment."""
        # Get system usage stats
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        
        # Get current date and time
        now = datetime.datetime.now()
        timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
        log_date = now.strftime('%Y-%m-%d')
        log_time = now.strftime('%H:%M:%S')
        
        # Use task_name from environment variable if not provided, otherwise get from execution service
        if task_name is None:
            try:
                # Import aqui para evitar dependência circular
                from .execution_service import get_execution_service
                execution_service = get_execution_service()
                task_identifier = execution_service.get_task_identifier()
                task_name = f"{self.bot_name}-{task_identifier}"
            except Exception:
                task_name = self.bot_name
        
        # Get caller info (file)
        source_file, _ = self._get_caller_info(depth=3)
        
        # Validate enum values
        if not isinstance(process_type, ProcessType):
            try:
                # Verifica se é uma string antes de tentar converter
                if isinstance(process_type, str):
                    process_type = ProcessType(process_type.lower())
                else:
                    # Se não for string nem ProcessType, usa o padrão
                    process_type = ProcessType.SYSTEM
            except (ValueError, AttributeError):
                process_type = ProcessType.SYSTEM
                
        if not isinstance(status, LogStatus):
            try:
                # Verifica se é uma string antes de tentar converter
                if isinstance(status, str):
                    status = LogStatus(status.lower())
                else:
                    # Se não for string nem LogStatus, usa o padrão
                    status = LogStatus.INFO
            except (ValueError, AttributeError):
                status = LogStatus.INFO
        
        # Format status
        status_str = status.value
        
        # Quebra a mensagem em múltiplas linhas se necessário
        message_lines = textwrap.wrap(log_message, width=self.message_width)
        if not message_lines:
            message_lines = [""]
            
        # Prepara valores para cada coluna (truncando se necessário)
        values = {
            'timestamp': timestamp[:self.col_widths['timestamp']],
            'task': task_name[:self.col_widths['task']],
            'function': function_name[:self.col_widths['function']],
            'file': source_file[:self.col_widths['file']],
            'message': message_lines[0],
            'process_type': process_type.value[:self.col_widths['process_type']],
            'status': status_str[:self.col_widths['status']]
        }
        
        # Formata a linha usando o mesmo padrão de padding que o cabeçalho
        log_line = "|"
        for name, width in self.col_widths.items():
            content = values[name]
            padding_right = width - len(content) + self.padding
            log_line += " " * self.padding + content + " " * padding_right + "|"
            
        # Escreve no arquivo de log com codificação UTF-8
        if self.log_file:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_line + "\n")
                
                # Se houver mais linhas na mensagem, adiciona-as com alinhamento preciso
                if len(message_lines) > 1:
                    # Cria linha de continuação para cada linha adicional da mensagem
                    for i in range(1, len(message_lines)):
                        cont_line = "|"
                        for name, width in self.col_widths.items():
                            if name == 'message':
                                # A coluna de mensagem tem conteúdo
                                content = message_lines[i]
                                padding_right = width - len(content) + self.padding
                                cont_line += " " * self.padding + content + " " * padding_right + "|"
                            else:
                                # Outras colunas ficam vazias
                                cont_line += " " * (width + self.padding * 2) + "|"
                        f.write(cont_line + "\n")
                
                # Adiciona separador após erros críticos para ênfase
                if status == LogStatus.CRITICAL:
                    f.write(self._create_separator_line() + "\n")
        
        # Log to console
        log_level = self._get_log_level(status)
        logging.log(log_level, f"{task_name} - {function_name}: {log_message}")
        
        # Save to database if connected
        self._log_to_database(task_name, function_name, source_file, cpu_usage,
                            memory_usage, log_date, log_time, log_message,
                            process_type, status, extra_data)
    
    def _add_closing_line(self) -> None:
        """Add closing line to log file on program exit with exact alignment."""
        if hasattr(self, 'log_file') and self.log_file and os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(self._create_separator_line() + "\n")
            except Exception:
                pass  # Silently fail if we can't write to the file
                
    def _get_log_level(self, status: LogStatus) -> int:
        """Map log status to Python logging level."""
        status_map = {
            LogStatus.CRITICAL: logging.CRITICAL,
            LogStatus.FAILURE: logging.ERROR,
            LogStatus.WARNING: logging.WARNING,
            LogStatus.INFO: logging.INFO,
            LogStatus.SUCCESS: logging.INFO
        }
        return status_map.get(status, logging.INFO)
    
    def _try_connect_to_database(self) -> None:
        """Tenta conectar ao banco de dados usando as credenciais corretas."""
        import pyodbc
        
        # Credenciais corretas
        server = r'server'
        database = 'rpa_prd'
        username = 'rpa'
        password = r'password'
        
        # Lista de drivers para tentar (baseado nos disponíveis)
        drivers = [
            '{ODBC Driver 17 for SQL Server}',
            '{SQL Server}',
            '{ODBC Driver 13 for SQL Server}',
            '{SQL Server Native Client 11.0}'
        ]
        
        for driver in drivers:
            try:
                connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;"
                print(f"Tentando conectar com driver: {driver}")
                print(f"String de conexão: {connection_string}")
                
                self.db_connection = pyodbc.connect(connection_string)
                print(f"✓ Conexão estabelecida com sucesso usando driver: {driver}")
                
                # Criar tabela se não existir
                result = self.create_log_table_if_not_exists()
                if result:
                    print("✓ Tabela de logs criada/verificada com sucesso")
                else:
                    print("⚠ Aviso: Problema ao criar/verificar tabela de logs")
                
                return
                
            except Exception as e:
                print(f"Falha com driver {driver}: {str(e)}")
                continue
        
        # Se chegou aqui, nenhum driver funcionou
        print("\n❌ Nenhum driver funcionou. Drivers ODBC disponíveis:")
        for driver in pyodbc.drivers():
            print(f"  - {driver}")
        
        self.log_warning("_try_connect_to_database", "Falha ao conectar ao banco para logs - logs serão salvos apenas em arquivo")
    
    def _log_to_database(self, task_name: str, function_name: str, source_file: str, 
                        cpu_usage: float, memory_usage: float, log_date: str, 
                        log_time: str, log_message: str, process_type: ProcessType, 
                        status: LogStatus, extra_data: Optional[Any]) -> bool:
        if not self.db_connection:
            return False
            
        try:
            cursor = self.db_connection.cursor()
            
            # Converter process_type e status para string se forem enum
            process_type_value = process_type.value if hasattr(process_type, 'value') else str(process_type)
            status_value = status.value if hasattr(status, 'value') else str(status)
            
            # Obter schema das configurações
            schema = self.dcConfig.get('dbschema', self.project_name)
            
            sql = f"""
            INSERT INTO [{schema}].[{self.logtablename}]
            (task_name, function_name, source_file, cpu_usage, memory_usage, log_date, log_time, 
            log_message, process_type, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor.execute(
                sql,
                (task_name, function_name, source_file, cpu_usage, memory_usage, log_date, log_time, 
                log_message, process_type_value, status_value)
            )
            
            self.db_connection.commit()
            cursor.close()
            return True
        except Exception as e:
            # Se falhar, tenta reconectar uma vez
            try:
                self._try_connect_to_database()
                if self.db_connection:
                    return self._log_to_database(task_name, function_name, source_file, cpu_usage, 
                                                memory_usage, log_date, log_time, log_message, 
                                                process_type, status, extra_data)
            except Exception:
                pass
            return False
    
    def log_info(self, function_name: str, message: str, process_type: ProcessType = ProcessType.SYSTEM) -> None:
        """Log information message."""
        self.log_entry(function_name, message, process_type, LogStatus.INFO)
    
    def log_success(self, function_name: str, message: str, process_type: ProcessType = ProcessType.SYSTEM) -> None:
        """Log success message."""
        self.log_entry(function_name, message, process_type, LogStatus.SUCCESS)
    
    def log_warning(self, function_name: str, message: str, process_type: ProcessType = ProcessType.SYSTEM) -> None:
        """Log warning message."""
        self.log_entry(function_name, message, process_type, LogStatus.WARNING)
    
    def log_error(self, function_name: str, message: str, exception: Exception = None, process_type: ProcessType = ProcessType.SYSTEM) -> None:
        """Log error message with traceback information."""
        # Captura informações do traceback da exceção
        tb_info = ""
        try:
            if exception and hasattr(exception, '__traceback__') and exception.__traceback__:
                # Usa o traceback da exceção fornecida
                line_number = traceback.extract_tb(exception.__traceback__)[0][1]
                filename = os.path.basename(traceback.extract_tb(exception.__traceback__)[0][0])
                tb_info = f" - LN {line_number} em {filename}"
            else:
                # Fallback para o comportamento original
                frame = inspect.currentframe()
                if frame and frame.f_back:
                    caller_frame = frame.f_back
                    filename = os.path.basename(caller_frame.f_code.co_filename)
                    line_number = caller_frame.f_lineno
                    tb_info = f" - LN {line_number} em {filename}"
        except Exception:
            pass
        
        # Adiciona informações de traceback à mensagem
        enhanced_message = f"{message}{tb_info}"
        self.log_entry(function_name, enhanced_message, process_type, LogStatus.FAILURE)
    
    def log_critical(self, function_name: str, message: str, process_type: ProcessType = ProcessType.SYSTEM) -> None:
        """Log critical error message with traceback information."""
        # Captura informações do traceback
        tb_info = ""
        try:
            # Obtém o frame atual e vai subindo na pilha para encontrar o erro
            frame = inspect.currentframe()
            if frame and frame.f_back:
                caller_frame = frame.f_back
                filename = os.path.basename(caller_frame.f_code.co_filename)
                line_number = caller_frame.f_lineno
                tb_info = f" - LN {line_number} em {filename}"
        except Exception:
            pass
        
        # Adiciona informações de traceback à mensagem
        enhanced_message = f"{message}{tb_info}"
        self.log_entry(function_name, enhanced_message, process_type, LogStatus.CRITICAL)