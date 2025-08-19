-- Создание таблиц для системы поставщиков
-- База данных: data (существующая)

-- 1. Таблица поставщиков (основная информация)
CREATE TABLE IF NOT EXISTS suppliers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    code VARCHAR(100) UNIQUE NOT NULL,
    inn VARCHAR(12),
    main_manager VARCHAR(255),
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Таблица контактов поставщиков
CREATE TABLE IF NOT EXISTS supplier_contacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_id UUID NOT NULL REFERENCES suppliers(id) ON DELETE CASCADE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    is_reconciliation_responsible BOOLEAN DEFAULT false,
    contact_code VARCHAR(100),
    position VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Таблица взаимодействий с поставщиками
CREATE TABLE IF NOT EXISTS supplier_interactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_id UUID NOT NULL REFERENCES suppliers(id) ON DELETE CASCADE,
    contact_id UUID REFERENCES supplier_contacts(id) ON DELETE SET NULL,
    interaction_type VARCHAR(100) NOT NULL, -- 'request_sent', 'reminder', 'response_received', 'act_received'
    month_cycle VARCHAR(7), -- формат: '2024-01'
    message_subject VARCHAR(500),
    message_body TEXT,
    attachments_meta JSONB, -- метаданные вложений
    email_id VARCHAR(255), -- ID письма для отслеживания
    status VARCHAR(50) DEFAULT 'pending', -- 'pending', 'sent', 'received', 'completed'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Таблица задач по сверке
CREATE TABLE IF NOT EXISTS supplier_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_id UUID NOT NULL REFERENCES suppliers(id) ON DELETE CASCADE,
    month_cycle VARCHAR(7) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending', -- 'pending', 'in_progress', 'completed', 'overdue'
    priority VARCHAR(20) DEFAULT 'normal', -- 'low', 'normal', 'high', 'urgent'
    request_sent_at TIMESTAMP WITH TIME ZONE,
    reminder_sent_at TIMESTAMP WITH TIME ZONE,
    deadline DATE,
    days_remaining INTEGER,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. Таблица месячных циклов
CREATE TABLE IF NOT EXISTS monthly_cycles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    month VARCHAR(7) UNIQUE NOT NULL, -- формат: '2024-01'
    status VARCHAR(50) DEFAULT 'pending', -- 'pending', 'active', 'completed', 'archived'
    tasks_created INTEGER DEFAULT 0,
    tasks_completed INTEGER DEFAULT 0,
    total_tasks INTEGER DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для производительности
CREATE INDEX IF NOT EXISTS idx_suppliers_code ON suppliers(code);
CREATE INDEX IF NOT EXISTS idx_suppliers_status ON suppliers(status);
CREATE INDEX IF NOT EXISTS idx_suppliers_inn ON suppliers(inn);

CREATE INDEX IF NOT EXISTS idx_supplier_contacts_supplier_id ON supplier_contacts(supplier_id);
CREATE INDEX IF NOT EXISTS idx_supplier_contacts_email ON supplier_contacts(email);
CREATE INDEX IF NOT EXISTS idx_supplier_contacts_reconciliation ON supplier_contacts(is_reconciliation_responsible);
CREATE INDEX IF NOT EXISTS idx_supplier_contacts_contact_code ON supplier_contacts(contact_code);

CREATE INDEX IF NOT EXISTS idx_supplier_interactions_supplier_id ON supplier_interactions(supplier_id);
CREATE INDEX IF NOT EXISTS idx_supplier_interactions_month_cycle ON supplier_interactions(month_cycle);
CREATE INDEX IF NOT EXISTS idx_supplier_interactions_type ON supplier_interactions(interaction_type);
CREATE INDEX IF NOT EXISTS idx_supplier_interactions_status ON supplier_interactions(status);
CREATE INDEX IF NOT EXISTS idx_supplier_interactions_created_at ON supplier_interactions(created_at);

CREATE INDEX IF NOT EXISTS idx_supplier_tasks_supplier_id ON supplier_tasks(supplier_id);
CREATE INDEX IF NOT EXISTS idx_supplier_tasks_month_cycle ON supplier_tasks(month_cycle);
CREATE INDEX IF NOT EXISTS idx_supplier_tasks_status ON supplier_tasks(status);
CREATE INDEX IF NOT EXISTS idx_supplier_tasks_deadline ON supplier_tasks(deadline);
CREATE UNIQUE INDEX IF NOT EXISTS idx_supplier_tasks_supplier_month ON supplier_tasks(supplier_id, month_cycle);

CREATE INDEX IF NOT EXISTS idx_monthly_cycles_month ON monthly_cycles(month);
CREATE INDEX IF NOT EXISTS idx_monthly_cycles_status ON monthly_cycles(status);

-- Создание триггеров для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Применение триггеров ко всем таблицам
CREATE TRIGGER update_suppliers_updated_at 
    BEFORE UPDATE ON suppliers FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_supplier_contacts_updated_at 
    BEFORE UPDATE ON supplier_contacts FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_supplier_interactions_updated_at 
    BEFORE UPDATE ON supplier_interactions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_supplier_tasks_updated_at 
    BEFORE UPDATE ON supplier_tasks FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_monthly_cycles_updated_at 
    BEFORE UPDATE ON monthly_cycles FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Комментарии к таблицам
COMMENT ON TABLE suppliers IS 'Основная информация о поставщиках';
COMMENT ON TABLE supplier_contacts IS 'Контакты поставщиков';
COMMENT ON TABLE supplier_interactions IS 'История взаимодействий с поставщиками';
COMMENT ON TABLE supplier_tasks IS 'Задачи по сверке с поставщиками';
COMMENT ON TABLE monthly_cycles IS 'Месячные циклы сверки';

-- Тестовые данные (опционально)
-- INSERT INTO suppliers (name, code, inn, main_manager) VALUES
-- ('ООО Поставщик А', 'SUP001', '1234567890', 'Иванов И.И.'),
-- ('ООО Поставщик Б', 'SUP002', '0987654321', 'Петров П.П.');
