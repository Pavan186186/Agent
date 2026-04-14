import pandas as pd
import hashlib

# 1. Define your 5 core Demo Personas
personas = [
    {"user_id": "U001", "name": "PAVAN TEJA TALLAPALLI", "username": "pavan_tallapalli", "pin": "1234", "routing_number": "111000111", "account_number": "1000000001", "card_number": "4111222233334441", "balance": 8500.50},
    {"user_id": "U002", "name": "NITHIN KUMAR SURINENI", "username": "nithin_surineni", "pin": "2345", "routing_number": "111000111", "account_number": "1000000002", "card_number": "4111222233334442", "balance": 45000.00},
    {"user_id": "U003", "name": "SAHITHI KATOORI", "username": "sahithi_katoori", "pin": "3456", "routing_number": "222000222", "account_number": "2000000001", "card_number": "5111222233334443", "balance": 12400.75},
    {"user_id": "U004", "name": "NIKHILESH GOUD", "username": "nikhilesh_goud", "pin": "4567", "routing_number": "333000333", "account_number": "3000000001", "card_number": "4555222233334444", "balance": 850.20},
    {"user_id": "U005", "name": "UDAY REDDY", "username": "uday_reddy", "pin": "5678", "routing_number": "111000111", "account_number": "1000000003", "card_number": "6011222233334445", "balance": 105000.00}
]

# Convert to DataFrame and secure the PINs
users_df = pd.DataFrame(personas)
users_df['pin_hash'] = users_df['pin'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
users_df.drop(columns=['pin'], inplace=True)

print("Reading raw Kaggle data (memory-safe mode)...")
# 2. Load the Kaggle Dataset efficiently
# We only read the first 50,000 rows. This avoids loading a 2.6GB file into memory.
# Note: Ensure your file is named 'kaggle_data.csv' or change the string below.
df = pd.read_csv('../synthetic_fraud_data.csv', nrows=50000)

# 3. Map the original customer IDs to our 5 specific personas
original_customers = df['customer_id'].unique()

customer_mapping = {
    old_id: personas[i % 5]['user_id'] 
    for i, old_id in enumerate(original_customers)
}

df['user_id'] = df['customer_id'].map(customer_mapping)

# 4. Limit the dataset to exactly 10,000 random transactions
print("Sampling down to 10,000 transactions...")
df = df.sample(n=10000, random_state=42).reset_index(drop=True)

# 5. Inject the specific account and card numbers
account_map = {p['user_id']: p['account_number'] for p in personas}
card_map = {p['user_id']: p['card_number'] for p in personas}

df['account_number'] = df['user_id'].map(account_map)
df['card_number'] = df['user_id'].map(card_map)

# Clean up columns we no longer need to keep the database light
# Adjust 'original_name_column' / 'original_card_column' based on actual Kaggle headers
columns_to_drop = ['customer_id', 'original_name_column', 'original_card_column'] 
df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

# 6. Export the final clean data
print("Exporting clean data to the data/ directory...")
users_df.to_csv('../demo_users.csv', index=False)
df.to_csv('../demo_transactions.csv', index=False)

print("Success! Created demo_users.csv and 10,000 rows in demo_transactions.csv.")