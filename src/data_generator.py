import os
import time
import random
import pandas as pd
from datetime import datetime

# Configurable parameters for efficiency
OUTPUT_DIR = '../data/events/'  # Relative to src/
NUM_BATCHES = 10  # Number of files to generate (simulate streaming)
BATCH_SIZE = 100  # Events per file (small for low memory)
SLEEP_INTERVAL = 5  # Seconds between files (simulate real-time)

# Ensure output dir exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_fake_event():
    """Generate a single fake event dict - efficient random generation."""
    return {
        'user_id': random.randint(1, 1000),
        'action': random.choice(['view', 'purchase']),
        'product_id': random.randint(1001, 2000),
        'product_name': random.choice(['Laptop', 'Phone', 'Headphones', 'Book', 'Shoes','Bags', 'Watch']),
        'price': round(random.uniform(10.0, 500.0), 2),
        'timestamp': datetime.now().isoformat()
    }

# Generate batches
for batch in range(NUM_BATCHES):
    events = [generate_fake_event() for _ in range(BATCH_SIZE)]  # List comprehension for speed
    df = pd.DataFrame(events)  # Vectorized DataFrame creation
    file_path = os.path.join(OUTPUT_DIR, f'events_batch_{batch}.csv')
    df.to_csv(file_path, index=False)  # Efficient CSV write
    print(f'Generated: {file_path}')
    time.sleep(SLEEP_INTERVAL)  # Simulate real-time arrival