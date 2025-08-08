
-- Миграция для добавления недостающих колонок в таблицу companyproducts
-- Дата: $(date)

-- Добавляем колонку product_properties если её нет
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'companyproducts' 
        AND column_name = 'product_properties'
    ) THEN
        ALTER TABLE public.companyproducts ADD COLUMN product_properties TEXT;
        RAISE NOTICE 'Колонка product_properties добавлена';
    ELSE
        RAISE NOTICE 'Колонка product_properties уже существует';
    END IF;
END $$;

-- Проверяем структуру таблицы
SELECT column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_schema = 'public' 
AND table_name = 'companyproducts'
ORDER BY ordinal_position;
