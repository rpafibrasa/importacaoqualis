import os, json
from dotenv import load_dotenv

class config:
    def __init__(self):
        self.dcConfig = {}
        self.dcParameter = {}
    
    @staticmethod
    def loadconfig():
        try:
            # Clear Console
            os.system('cls')
            
            # ===== Load Settings File =====
            # Load .env credentials
            load_dotenv()

            # Load json parameters and settings values
            with open('config.json', 'r', encoding='utf-8') as jsonfile:
                jsondata = json.load(jsonfile)
            
            # ===== Global Config =====
            # Get Parameters from .env and config.json
            dbconnstr = os.getenv('CONNECTION_SUPABASE') or os.getenv('DATABASE_CONNECTION_STRING')
            dbschema = jsondata['database']['schema']
            projectname = jsondata['project']['name']
            folderlog = jsondata['folders']['folderlog']
            logtablename = jsondata['database']['logtablename']
            databasename = jsondata['database']['databasename']
            tabdocumentosged = jsondata['database']['tabdocumentosged']
            url_inicial = os.getenv('URL_INICIAL')
            url_api_processo = os.getenv('URL_API_PROCESSO')
            
            # Set Parameters
            dcConfig = {
                'dbconnstr': dbconnstr,  # Connection String of Database
                'dbschema': dbschema,    # Control Schema used
                'projectname': projectname, # Project Name
                'folderlog': folderlog, # Folder Log
                'logtablename': logtablename, # Log Table Name
                'databasename': databasename, # Database Name
                'tabdocumentosged': tabdocumentosged, # Table Documentos GED
                'url_inicial': url_inicial, # URL Inicial
                'url_api_processo': url_api_processo, # URL API Processo
                # Adicionar configurações de banco de dados do config.json
                'database': jsondata['database']  # Configurações completas do banco
            }
            
            # ===== Global Parameters =====
            # Get Parameters from .env and config.json
            testmessage = jsondata['test']['message']
            testname = jsondata['test']['name']
            folderrede = jsondata['folders']['folderrede']
            foldercapturados = jsondata['folders']['foldercapturados']
            folderprocessados = jsondata['folders']['folderprocessados']
            folderdownloads = jsondata['folders']['folderdownloads']
            foldertemp = jsondata['folders']['foldertemp']

            # Set Parameters
            dcParameter = {
                'testmessage': testmessage,  # Test Message
                'testname': testname,        # Test Name
                'folderrede': folderrede, # folderrede 
                'foldercapturados': foldercapturados, # Folder Capturados
                'folderprocessados': folderprocessados, # Folder Processados
                'folderdownloads': folderdownloads, # Folder Downloads
                'foldertemp': foldertemp, # Folder Temp
            }
            
            return dcConfig, dcParameter
        except Exception as e:
            print(f"Erro ao carregar configurações: {e}")
            # Retornar configurações padrão em caso de erro
            return {}, {}
