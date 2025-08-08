-- === Ensure upload_timestamp is always updated on INSERT and UPDATE ===

-- 1. Создаём функцию для обновления upload_timestamp
CREATE OR REPLACE FUNCTION set_upload_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.upload_timestamp := CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Удаляем старый триггер, если он был
DROP TRIGGER IF EXISTS trg_set_upload_timestamp ON companyproducts;

-- 3. Создаём новый триггер на INSERT и UPDATE
CREATE TRIGGER trg_set_upload_timestamp
BEFORE INSERT OR UPDATE ON companyproducts
FOR EACH ROW EXECUTE FUNCTION set_upload_timestamp();

-- === END upload_timestamp trigger === 