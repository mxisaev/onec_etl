-- Создание таблицы product_properties для key-value хранения характеристик
CREATE TABLE IF NOT EXISTS product_properties (
    id SERIAL PRIMARY KEY,
    product_id UUID REFERENCES companyproducts(id) ON DELETE CASCADE,
    prop_key VARCHAR(255) NOT NULL,
    prop_value TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_product_properties_product_id ON product_properties(product_id);
CREATE INDEX IF NOT EXISTS idx_product_properties_key ON product_properties(prop_key);
CREATE INDEX IF NOT EXISTS idx_product_properties_key_value ON product_properties(prop_key, prop_value);

-- Уникальный индекс для предотвращения дублирования характеристик
CREATE UNIQUE INDEX IF NOT EXISTS idx_product_properties_unique ON product_properties(product_id, prop_key);

-- Триггер для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_product_properties_updated_at 
    BEFORE UPDATE ON product_properties 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Комментарии к таблице
COMMENT ON TABLE product_properties IS 'Характеристики продуктов в формате key-value';
COMMENT ON COLUMN product_properties.product_id IS 'ID продукта из таблицы companyproducts';
COMMENT ON COLUMN product_properties.prop_key IS 'Название характеристики (например: Цвет, Бренд, Материал)';
COMMENT ON COLUMN product_properties.prop_value IS 'Значение характеристики'; 