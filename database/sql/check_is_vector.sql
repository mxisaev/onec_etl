SELECT 
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE is_vector = false) as needs_embedding
FROM companyproducts; 