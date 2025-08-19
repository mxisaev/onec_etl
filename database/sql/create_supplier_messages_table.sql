-- Добавление таблицы supplier_messages для истории сообщений
-- База данных: data (существующая)

-- Таблица истории сообщений с поставщиками
CREATE TABLE IF NOT EXISTS supplier_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_id UUID NOT NULL REFERENCES suppliers(id) ON DELETE CASCADE,
    contact_id UUID REFERENCES supplier_contacts(id) ON DELETE SET NULL,
    message_type VARCHAR(100) NOT NULL, -- 'email_sent', 'email_received', 'sms_sent', 'sms_received', 'call_made', 'call_received'
    direction VARCHAR(20) NOT NULL, -- 'outbound', 'inbound'
    subject VARCHAR(500),
    body TEXT,
    sender_email VARCHAR(255),
    recipient_email VARCHAR(255),
    message_id VARCHAR(255), -- уникальный ID сообщения (например, email message-id)
    thread_id VARCHAR(255), -- ID цепочки сообщений
    status VARCHAR(50) DEFAULT 'sent', -- 'sent', 'delivered', 'read', 'failed', 'bounced'
    priority VARCHAR(20) DEFAULT 'normal', -- 'low', 'normal', 'high', 'urgent'
    attachments_count INTEGER DEFAULT 0,
    attachments_meta JSONB, -- метаданные вложений
    delivery_timestamp TIMESTAMP WITH TIME ZONE,
    read_timestamp TIMESTAMP WITH TIME ZONE,
    related_task_id UUID REFERENCES supplier_tasks(id) ON DELETE SET NULL,
    related_interaction_id UUID REFERENCES supplier_interactions(id) ON DELETE SET NULL,
    tags TEXT[], -- массив тегов для категоризации
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для производительности
CREATE INDEX IF NOT EXISTS idx_supplier_messages_supplier_id ON supplier_messages(supplier_id);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_contact_id ON supplier_messages(contact_id);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_message_type ON supplier_messages(message_type);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_direction ON supplier_messages(direction);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_status ON supplier_messages(status);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_created_at ON supplier_messages(created_at);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_message_id ON supplier_messages(message_id);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_thread_id ON supplier_messages(thread_id);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_related_task ON supplier_messages(related_task_id);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_related_interaction ON supplier_messages(related_interaction_id);

-- Триггер для автоматического обновления updated_at
CREATE TRIGGER update_supplier_messages_updated_at 
    BEFORE UPDATE ON supplier_messages FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Комментарий к таблице
COMMENT ON TABLE supplier_messages IS 'История сообщений с поставщиками (email, SMS, звонки)';
