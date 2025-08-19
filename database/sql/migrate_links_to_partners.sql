-- Migrate existing tables to link by 1C identifiers instead of UUIDs
-- We will use VARCHAR(255) for partner and contact identifiers

-- supplier_interactions
ALTER TABLE supplier_interactions 
	ALTER COLUMN supplier_id TYPE VARCHAR(255),
	ALTER COLUMN contact_id TYPE VARCHAR(255);

-- supplier_tasks
ALTER TABLE supplier_tasks 
	ALTER COLUMN supplier_id TYPE VARCHAR(255);

-- supplier_messages
ALTER TABLE supplier_messages 
	ALTER COLUMN supplier_id TYPE VARCHAR(255),
	ALTER COLUMN contact_id TYPE VARCHAR(255);

-- Indexes to speed up joins with partners
CREATE INDEX IF NOT EXISTS idx_supplier_interactions_supplier_id_varchar ON supplier_interactions(supplier_id);
CREATE INDEX IF NOT EXISTS idx_supplier_interactions_contact_id_varchar ON supplier_interactions(contact_id);
CREATE INDEX IF NOT EXISTS idx_supplier_tasks_supplier_id_varchar ON supplier_tasks(supplier_id);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_supplier_id_varchar ON supplier_messages(supplier_id);
CREATE INDEX IF NOT EXISTS idx_supplier_messages_contact_id_varchar ON supplier_messages(contact_id);
