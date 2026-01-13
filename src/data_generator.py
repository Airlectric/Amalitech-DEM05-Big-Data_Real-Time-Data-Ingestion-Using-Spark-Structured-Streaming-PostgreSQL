import os
import time
import random
import pandas as pd
from datetime import datetime

# Configurable parameters
OUTPUT_DIR = '/opt/spark/app/data/events'
NUM_BATCHES = 20
BATCH_SIZE = 100
SLEEP_INTERVAL = 5

os.makedirs(OUTPUT_DIR, exist_ok=True)

PRODUCTS = [
    'Laptop', 'Phone', 'Headphones', 'Book', 'Shoes', 'Bags', 'Watch', 
    'Tablet', 'Monitor', 'Keyboard'
]

def generate_fake_event(batch_num):
    """Generate event with some realistic data quality issues"""
    timestamp = datetime.now()
    
    # ~5% chance of missing/invalid timestamp format
    if random.random() < 0.05:
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")  # wrong format (no T)
    elif random.random() < 0.03:
        timestamp_str = ""  # empty string
    elif random.random() < 0.02:
        timestamp_str = "INVALID_DATE"
    else:
        timestamp_str = timestamp.isoformat()

    # ~4% chance of negative or zero price
    price = round(random.uniform(-15.0, 800.0), 2)
    if random.random() < 0.96:
        price = max(0.0, price)  # most are positive

    # ~3% chance of null/empty action
    action = random.choice(['view', 'purchase', 'add_to_cart', 'remove_from_cart', '', None][:-2])
    if random.random() < 0.03:
        action = random.choice(['', None, 'VIEW', 'Purchase'])  # case issues + empty

    return {
        'user_id': random.randint(1, 1200) if random.random() > 0.02 else None,           # occasional null
        'action': action,
        'product_id': random.randint(1001, 2500) if random.random() > 0.015 else -1,     # occasional invalid id
        'product_name': random.choice(PRODUCTS) if random.random() > 0.01 else '',       # occasional empty
        'price': price,
        'timestamp': timestamp_str,

        'session_id': str(random.randint(10000, 99999)) if random.random() < 0.9 else None
    }


print("Starting fake e-commerce event generation...\n")

for batch in range(NUM_BATCHES):
    events = []
    for _ in range(BATCH_SIZE):
        event = generate_fake_event(batch)
        events.append(event)

    df = pd.DataFrame(events)

    # Sometimes drop session_id column to simulate inconsistent schema
    if batch % 4 != 0:
        if 'session_id' in df.columns:
            df = df.drop(columns=['session_id'])

    file_path = os.path.join(OUTPUT_DIR, f'events_batch_{batch:03d}.csv')
    df.to_csv(file_path, index=False)
    print(f"Generated: {file_path}  ({len(df)} rows)")
    
    time.sleep(SLEEP_INTERVAL)

print("\nGeneration complete.")