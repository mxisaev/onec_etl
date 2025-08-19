-- Cleanup old tables and check new structure
DROP TABLE IF EXISTS suppliers CASCADE;
DROP TABLE IF EXISTS supplier_contacts CASCADE;

-- Check partners table structure
\d partners

-- Check if view exists
\dv suppliers_view

-- List all tables
\dt
