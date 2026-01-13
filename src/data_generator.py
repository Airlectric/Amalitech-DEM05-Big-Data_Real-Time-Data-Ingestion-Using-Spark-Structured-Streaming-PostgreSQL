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
    timestamp = datetime.now()
    
    if random.random() < 0.05:
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    elif random.random() < 0.03:
        timestamp_str = ""
    elif random.random() < 0.02:
        timestamp_str = "INVALID_DATE"
    else:
        timestamp_str = timestamp.isoformat()

    price = round(random.uniform(-15.0, 800.0), 2)
    if random.random() < 0.96:
        price = max(0.0, price)

    action_choices = ['view', 'purchase', 'add_to_cart', 'remove_from_cart', '', None]
    action = random.choice(action_choices[:-2])
    if random.random() < 0.03:
        action = random.choice(['', None, 'VIEW', 'Purchase'])

    return {
        'user_id': random.randint(1, 1200) if random.random() > 0.02 else None,
        'action': action,
        'product_id': random.randint(1001, 2500) if random.random() > 0.015 else -1,
        'product_name': random.choice(PRODUCTS) if random.random() > 0.01 else '',
        'price': price,
        'timestamp': timestamp_str,
        'session_id': str(random.randint(10000, 99999)) if random.random() < 0.9 else None
    }


print("Starting fake e-commerce event generation...\n")

start_total = time.time()
total_events = 0
batch_times = []

for batch in range(NUM_BATCHES):
    batch_start = time.time()
    
    events = [generate_fake_event(batch) for _ in range(BATCH_SIZE)]
    df = pd.DataFrame(events)
    
    if batch % 4 != 0:
        if 'session_id' in df.columns:
            df = df.drop(columns=['session_id'])
    
    file_path = os.path.join(OUTPUT_DIR, f'events_batch_{batch:03d}.csv')
    df.to_csv(file_path, index=False)
    
    batch_duration = time.time() - batch_start
    events_this_batch = len(df)
    throughput = events_this_batch / batch_duration if batch_duration > 0 else 0
    
    print(f"Batch {batch:3d} | {events_this_batch:4d} rows | "
          f"Time: {batch_duration:5.2f}s | "
          f"{throughput:6.1f} rows/sec | {file_path}")
    
    total_events += events_this_batch
    batch_times.append(batch_duration)
    
    time.sleep(SLEEP_INTERVAL)

total_time = time.time() - start_total
avg_batch_time = sum(batch_times) / len(batch_times) if batch_times else 0
overall_throughput = total_events / total_time if total_time > 0 else 0

print("\n" + "="*70)
print("GENERATION COMPLETE")
print(f"Total events generated:    {total_events:,}")
print(f"Total time:                {total_time:.2f} seconds")
print(f"Average time per batch:    {avg_batch_time:.2f} seconds")
print(f"Overall throughput:        {overall_throughput:.1f} events/second")
print(f"Effective rate (incl. sleep): {total_events / (total_time + (NUM_BATCHES * SLEEP_INTERVAL)):.1f} events/second")
print("="*70)