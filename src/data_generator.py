import time
import json
import random
import argparse
from kafka import KafkaProducer
from sklearn.datasets import load_digits

def generate_mnist_data():
    digits = load_digits()
    n_samples = len(digits.images)
    
    while True:
        idx = random.randint(0, n_samples - 1)
        image_flat = digits.data[idx].tolist()
        label = int(digits.target[idx])
        
        # Create feature dictionary: pixel_0 to pixel_63
        data = {f"pixel_{i}": val for i, val in enumerate(image_flat)}
        data["label"] = label
        yield data

def main(topic, bootstrap_servers, interval, limit=None):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    gen = generate_mnist_data()

    print(f"Starting MNIST data generation to topic '{topic}' on {bootstrap_servers}...")
    try:
        count = 0
        for data in gen:
            producer.send(topic, data)
            count += 1
            if limit and count >= limit:
                print(f"Reached limit of {limit} messages.")
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopping data generation.")
    finally:
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Data Generator")
    parser.add_argument("--topic", default="input_data", help="Kafka topic name")
    parser.add_argument("--bootstrap-servers", default="kafka:29092", help="Kafka bootstrap servers")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between messages in seconds")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of messages to send")
    args = parser.parse_args()

    main(args.topic, args.bootstrap_servers, args.interval, args.limit)
