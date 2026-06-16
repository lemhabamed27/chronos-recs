import time
import psycopg2

# Database Connection for Cart and Inventory simulation
PG_CONN = psycopg2.connect(
    host="localhost",
    port=5432,
    database="chronos_db",
    user="admin",
    password="admin123"
)
PG_CURSOR = PG_CONN.cursor()

# Mocking Inventory Stock Database
inventory_db = {
    "product_101": 5,  # In stock
    "product_202": 0   # Out of stock (Will trigger rollback)
}

def fix_and_prepare_tables():
    # Safely ensure the cart table has the correct schema for SAGA tracking
    try:
        PG_CURSOR.execute("DROP TABLE IF EXISTS cart CASCADE;")
        PG_CURSOR.execute("""
            CREATE TABLE cart (
                id SERIAL PRIMARY KEY,
                user_id INT,
                product_id VARCHAR(50),
                status VARCHAR(50)
            );
        """)
        PG_CONN.commit()
    except Exception as e:
        PG_CONN.rollback()
        print(f"Schema preparation fallback: {e}")

def step_1_create_cart_order(user_id, product_id):
    print(f"   [Step 1] Reserving item in Cart table for User {user_id}...")
    PG_CURSOR.execute(
        "INSERT INTO cart (user_id, product_id, status) VALUES (%s, %s, 'PENDING') RETURNING id;",
        (user_id, product_id)
    )
    cart_id = PG_CURSOR.fetchone()[0]
    PG_CONN.commit()
    return cart_id

def step_2_deduct_inventory(product_id):
    print(f"   [Step 2] Checking and deducting inventory for {product_id}...")
    if inventory_db.get(product_id, 0) > 0:
        inventory_db[product_id] -= 1
        print(f"            Success! Remaining stock: {inventory_db[product_id]}")
        return True
    else:
        print(f"            FAILED! {product_id} is out of stock.")
        return False

def confirm_saga(cart_id):
    print(f"   [Success Path] Confirming Order! Updating Cart ID {cart_id} status to 'CONFIRMED'...")
    PG_CURSOR.execute("UPDATE cart SET status = 'CONFIRMED' WHERE id = %s;", (cart_id,))
    PG_CONN.commit()

def rollback_saga(cart_id, product_id, user_id):
    print("\n   [Compensating Path] SAGA TRIGGERED ROLLBACK! Reverting transactions...")
    # Compensating action 1: Cancel cart status
    PG_CURSOR.execute("UPDATE cart SET status = 'CANCELLED' WHERE id = %s;", (cart_id,))
    PG_CONN.commit()
    print(f"            Cart status for order {cart_id} updated to 'CANCELLED'.")
    print("   [Status] System returned safely to consistent state. Saga execution complete.")

def run_saga_pipeline(user_id, product_id):
    print(f"\n--- Starting Saga Transaction for Product: {product_id} ---")
    cart_id = None
    try:
        # Local Transaction 1
        cart_id = step_1_create_cart_order(user_id, product_id)
        time.sleep(0.5)
        
        # Local Transaction 2 (Distributed Tier)
        success = step_2_deduct_inventory(product_id)
        time.sleep(0.5)
        
        if success:
            confirm_saga(cart_id)
            print("--- Saga Transaction Completed Successfully ---")
        else:
            # Trigger compensation if any tier fails
            rollback_saga(cart_id, product_id, user_id)
            
    except Exception as e:
        PG_CONN.rollback() # Clear any aborted transaction block state
        if cart_id:
            rollback_saga(cart_id, product_id, user_id)
        print(f"Unexpected Infrastructure Error: {e}")

# Main Execution Flow
if __name__ == "__main__":
    fix_and_prepare_tables()
    
    # Test 1: Successful Saga (Product 101 is available)
    run_saga_pipeline(user_id=77, product_id="product_101")
    
    # Test 2: Failed Saga triggering Compensation (Product 202 is empty)
    run_saga_pipeline(user_id=88, product_id="product_202")

    PG_CURSOR.close()
    PG_CONN.close()