class ExecutarHelloWorld:
    def __init__(self, services, logger):
        self.services = services
        self.logger = logger
    
    def run(self):
        self.services.logger.log_info('ExecutarHelloWorld', 'Executando Hello World!')        
        print("✓ Hello World!")