-- Create partners table to store unified partners (clients and suppliers)
-- Database: data (existing)

-- Ensure helper function exists (was created earlier in project). If not, uncomment next line.
-- CREATE OR REPLACE FUNCTION update_updated_at_column() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = CURRENT_TIMESTAMP; RETURN NEW; END; $$ language 'plpgsql';

CREATE TABLE IF NOT EXISTS partners (
	partner_uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	id_1c_partner VARCHAR(255) NOT NULL,
	partner VARCHAR(255) NOT NULL,
	contact VARCHAR(255),
	id_1c_contact VARCHAR(255),
	contact_email VARCHAR(255),
	responsible_manager VARCHAR(255),
	is_client BOOLEAN,
	is_supplier BOOLEAN,
	role VARCHAR(255),
	created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_partners_id_1c_partner ON partners(id_1c_partner);
CREATE INDEX IF NOT EXISTS idx_partners_id_1c_contact ON partners(id_1c_contact);
CREATE INDEX IF NOT EXISTS idx_partners_is_supplier ON partners(is_supplier);
CREATE INDEX IF NOT EXISTS idx_partners_is_client ON partners(is_client);
CREATE INDEX IF NOT EXISTS idx_partners_contact_email ON partners(contact_email);

-- Optional composite index to speed up supplier filtering by contact
CREATE INDEX IF NOT EXISTS idx_partners_supplier_contact ON partners(is_supplier, contact_email);

-- Suppliers view
CREATE OR REPLACE VIEW suppliers_view AS
SELECT * FROM partners WHERE is_supplier = TRUE;

-- Trigger to auto-update updated_at
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM pg_trigger WHERE tgname = 'update_partners_updated_at'
	) THEN
		CREATE TRIGGER update_partners_updated_at
			BEFORE UPDATE ON partners FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
	END IF;
END $$;
