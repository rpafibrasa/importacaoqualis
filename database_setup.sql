-- =====================================================
-- SCRIPT DE CONFIGURAÇÃO DO BANCO DE DADOS SUPABASE
-- Projeto: IMP_DOC_QUALIS_R1
-- Data: 2024
-- =====================================================

-- =====================================================
-- 1. CRIAÇÃO DO SCHEMA
-- =====================================================

-- Criar schema público se não existir (geralmente já existe no Supabase)
CREATE SCHEMA IF NOT EXISTS public;

-- =====================================================
-- 2. CRIAÇÃO DA TABELA DE LOGS
-- =====================================================

-- Tabela para armazenar logs do sistema RPA
CREATE TABLE IF NOT EXISTS public.logrec (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(255),
    function_name VARCHAR(255),
    source_file VARCHAR(255),
    cpu_usage FLOAT,
    memory_usage FLOAT,
    log_date DATE,
    log_time TIME,
    log_message TEXT,
    process_type VARCHAR(50),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criar índices para melhor performance
CREATE INDEX IF NOT EXISTS idx_logrec_created_at ON public.logrec(created_at);
CREATE INDEX IF NOT EXISTS idx_logrec_task_name ON public.logrec(task_name);
CREATE INDEX IF NOT EXISTS idx_logrec_status ON public.logrec(status);
CREATE INDEX IF NOT EXISTS idx_logrec_process_type ON public.logrec(process_type);

-- =====================================================
-- 3. TABELAS ADICIONAIS PARA O PROJETO RPA
-- =====================================================

-- Tabela para controle de processos
CREATE TABLE IF NOT EXISTS public.rpa_processes (
    id SERIAL PRIMARY KEY,
    process_name VARCHAR(255) NOT NULL,
    process_description TEXT,
    status VARCHAR(50) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela para armazenar dados capturados
CREATE TABLE IF NOT EXISTS public.rpa_captured_data (
    id SERIAL PRIMARY KEY,
    process_id INTEGER REFERENCES public.rpa_processes(id),
    data_type VARCHAR(100),
    data_content JSONB,
    source_file VARCHAR(500),
    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Tabela para controle de execuções
CREATE TABLE IF NOT EXISTS public.rpa_executions (
    id SERIAL PRIMARY KEY,
    process_id INTEGER REFERENCES public.rpa_processes(id),
    execution_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_end TIMESTAMP,
    status VARCHAR(50) DEFAULT 'RUNNING',
    error_message TEXT,
    records_processed INTEGER DEFAULT 0
);

-- Tabela para configurações do sistema
CREATE TABLE IF NOT EXISTS public.rpa_config (
    id SERIAL PRIMARY KEY,
    config_key VARCHAR(255) UNIQUE NOT NULL,
    config_value TEXT,
    config_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 4. ÍNDICES ADICIONAIS PARA PERFORMANCE
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_rpa_captured_data_process_id ON public.rpa_captured_data(process_id);
CREATE INDEX IF NOT EXISTS idx_rpa_captured_data_captured_at ON public.rpa_captured_data(captured_at);
CREATE INDEX IF NOT EXISTS idx_rpa_captured_data_processed ON public.rpa_captured_data(processed);

CREATE INDEX IF NOT EXISTS idx_rpa_executions_process_id ON public.rpa_executions(process_id);
CREATE INDEX IF NOT EXISTS idx_rpa_executions_status ON public.rpa_executions(status);
CREATE INDEX IF NOT EXISTS idx_rpa_executions_start ON public.rpa_executions(execution_start);

-- =====================================================
-- 5. FUNÇÕES E TRIGGERS
-- =====================================================

-- Função para atualizar timestamp de updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers para atualizar updated_at automaticamente
CREATE TRIGGER update_rpa_processes_updated_at 
    BEFORE UPDATE ON public.rpa_processes 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_rpa_config_updated_at 
    BEFORE UPDATE ON public.rpa_config 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 6. DADOS INICIAIS
-- =====================================================

-- Inserir processo padrão
INSERT INTO public.rpa_processes (process_name, process_description) 
VALUES ('IMP_DOC_QUALIS_R1', 'Processo de importação de documentos de qualidade')
ON CONFLICT DO NOTHING;

-- Inserir configurações padrão
INSERT INTO public.rpa_config (config_key, config_value, config_description) VALUES
('system.version', '1.0.0', 'Versão do sistema RPA'),
('system.environment', 'production', 'Ambiente de execução'),
('log.retention_days', '30', 'Dias de retenção dos logs'),
('process.max_retries', '3', 'Número máximo de tentativas em caso de erro')
ON CONFLICT (config_key) DO NOTHING;

-- =====================================================
-- 7. POLÍTICAS DE SEGURANÇA (RLS - Row Level Security)
-- =====================================================

-- Habilitar RLS nas tabelas sensíveis (opcional)
-- ALTER TABLE public.rpa_config ENABLE ROW LEVEL SECURITY;

-- Exemplo de política (descomente se necessário)
-- CREATE POLICY "Users can view their own data" ON public.rpa_executions
--     FOR SELECT USING (auth.uid() = user_id);

-- =====================================================
-- 8. VIEWS ÚTEIS
-- =====================================================

-- View para estatísticas de logs
CREATE OR REPLACE VIEW public.log_statistics AS
SELECT 
    DATE(created_at) as log_date,
    status,
    process_type,
    COUNT(*) as count
FROM public.logrec
GROUP BY DATE(created_at), status, process_type
ORDER BY log_date DESC, status;

-- View para execuções recentes
CREATE OR REPLACE VIEW public.recent_executions AS
SELECT 
    e.id,
    p.process_name,
    e.execution_start,
    e.execution_end,
    e.status,
    e.records_processed,
    EXTRACT(EPOCH FROM (COALESCE(e.execution_end, CURRENT_TIMESTAMP) - e.execution_start)) as duration_seconds
FROM public.rpa_executions e
JOIN public.rpa_processes p ON e.process_id = p.id
ORDER BY e.execution_start DESC
LIMIT 100;

-- =====================================================
-- 9. COMANDOS DE LIMPEZA (MANUTENÇÃO)
-- =====================================================

-- Função para limpeza de logs antigos
CREATE OR REPLACE FUNCTION cleanup_old_logs(retention_days INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM public.logrec 
    WHERE created_at < CURRENT_DATE - INTERVAL '1 day' * retention_days;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 10. GRANTS E PERMISSÕES
-- =====================================================

-- Garantir permissões para o usuário postgres (já é superuser no Supabase)
-- Estas permissões são principalmente para documentação

-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
-- GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO postgres;

-- Para usuários específicos da aplicação (se necessário):
-- CREATE USER rpa_user WITH PASSWORD 'secure_password';
-- GRANT CONNECT ON DATABASE postgres TO rpa_user;
-- GRANT USAGE ON SCHEMA public TO rpa_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO rpa_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO rpa_user;

-- =====================================================
-- FIM DO SCRIPT
-- =====================================================

-- Verificar se todas as tabelas foram criadas
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE schemaname = 'public' 
    AND tablename IN ('logrec', 'rpa_processes', 'rpa_captured_data', 'rpa_executions', 'rpa_config')
ORDER BY tablename;

-- Verificar índices criados
SELECT 
    schemaname,
    tablename,
    indexname
FROM pg_indexes 
WHERE schemaname = 'public' 
    AND tablename IN ('logrec', 'rpa_processes', 'rpa_captured_data', 'rpa_executions', 'rpa_config')
ORDER BY tablename, indexname;