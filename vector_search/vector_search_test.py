import os
import psycopg2
import numpy as np
from dotenv import load_dotenv
from pgvector.psycopg2 import register_vector

# Load environment variables
load_dotenv()

# Database connection parameters
db_params = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

def find_products_by_description(conn, search_text, limit=5):
    """
    Find products by text description using LIKE query
    Args:
        conn: database connection
        search_text: text to search for in description
        limit: number of products to return
    """
    with conn.cursor() as cur:
        # Search for products with similar description
        cur.execute('''
            SELECT id, description, brand, category
            FROM companyproducts
            WHERE description ILIKE %s
            AND is_vector = true
            LIMIT %s;
        ''', (f'%{search_text}%', limit))
        
        results = cur.fetchall()
        if not results:
            print(f"No products found matching '{search_text}'")
            return None
            
        print(f"\nFound {len(results)} products matching '{search_text}':")
        print("----------------------")
        for i, (id, description, brand, category) in enumerate(results, 1):
            print(f"{i}. ID: {id}")
            print(f"   Description: {description}")
            print(f"   Brand: {brand}")
            print(f"   Category: {category}")
            print("----------------------")
        
        # If multiple products found, ask user to select one
        if len(results) > 1:
            while True:
                try:
                    choice = int(input("\nEnter the number of the product to find similar items (1-{}): ".format(len(results))))
                    if 1 <= choice <= len(results):
                        return results[choice-1][0]  # Return the selected product ID
                    print("Invalid choice. Please try again.")
                except ValueError:
                    print("Please enter a valid number.")
        else:
            return results[0][0]  # Return the only product ID found

def find_similar_products(conn, product_id, limit=4, exclude_same_brand=False, category_weight=1.0):
    """
    Find similar products based on a reference product ID with weighted category similarity
    Args:
        conn: database connection
        product_id: UUID of the reference product
        limit: number of similar products to return
        exclude_same_brand: if True, exclude products from the same brand
        category_weight: weight for category similarity (default 1.0)
    """
    with conn.cursor() as cur:
        # First, get the reference product details
        cur.execute('''
            SELECT id, description, brand, category, vector 
            FROM companyproducts 
            WHERE id = %s AND is_vector = true;
        ''', (product_id,))
        
        reference = cur.fetchone()
        if not reference:
            print(f"Product with ID {product_id} not found or has no vector")
            return
            
        ref_id, ref_desc, ref_brand, ref_category, ref_vector = reference
        
        print("\nReference Product:")
        print("----------------------")
        print(f"ID: {ref_id}")
        print(f"Description: {ref_desc}")
        print(f"Brand: {ref_brand}")
        print(f"Category: {ref_category}")
        print("\nSimilar Products:")
        print("----------------------")
        
        # Find similar products with weighted category similarity
        query = '''
            WITH similarity_scores AS (
                SELECT 
                    id,
                    description,
                    brand,
                    category,
                    vector <=> %s::vector as vector_distance,
                    CASE 
                        WHEN category = %s THEN 1.0
                        ELSE 0.0
                    END as category_match
                FROM companyproducts
                WHERE is_vector = true 
                AND id != %s
        '''
        
        if exclude_same_brand:
            query += " AND brand != %s"
            params = (ref_vector, ref_category, product_id, ref_brand)
        else:
            params = (ref_vector, ref_category, product_id)
            
        query += '''
            )
            SELECT 
                id,
                description,
                brand,
                category,
                vector_distance,
                category_match,
                (1 - vector_distance) * (1 - %s) + category_match * %s as combined_score
            FROM similarity_scores
            ORDER BY combined_score DESC
            LIMIT %s;
        '''
        
        cur.execute(query, params + (category_weight, category_weight, limit))
        
        results = cur.fetchall()
        for id, description, brand, category, vector_distance, category_match, combined_score in results:
            print(f"ID: {id}")
            print(f"Description: {description}")
            print(f"Brand: {brand}")
            print(f"Category: {category}")
            print(f"Vector Similarity: {(1 - vector_distance):.2%}")
            print(f"Category Match: {'Yes' if category_match == 1.0 else 'No'}")
            print(f"Combined Score: {combined_score:.2%}")
            print("----------------------")

def main():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        register_vector(conn)
        
        print("Connected to PostgreSQL database!")
        
        while True:
            # Get search text from user
            search_text = input("\nEnter product description to search (or 'q' to quit): ")
            if search_text.lower() == 'q':
                break
                
            # Find products matching the description
            product_id = find_products_by_description(conn, search_text)
            
            if product_id:
                # Ask if user wants to exclude same brand
                exclude_same_brand = input("\nExclude products from the same brand? (y/n): ").lower() == 'y'
                
                # Get category weight from user
                while True:
                    try:
                        category_weight = float(input("\nEnter category weight (0.0 to 1.0, default 1.0): ") or "1.0")
                        if 0 <= category_weight <= 1:
                            break
                        print("Weight must be between 0.0 and 1.0")
                    except ValueError:
                        print("Please enter a valid number")
                
                # Find similar products
                find_similar_products(conn, product_id, exclude_same_brand=exclude_same_brand, category_weight=category_weight)
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main() 