from kafka import KafkaConsumer
import json
from datetime import datetime
import sys
import time

class PositionConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic='vault_positions', max_retries=5):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None
        self.max_retries = max_retries
        self.connect_to_kafka()

    def connect_to_kafka(self):
        retries = 0
        while retries < self.max_retries:
            try:
                print(f"Attempting to connect to Kafka (attempt {retries + 1}/{self.max_retries})...")
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    auto_offset_reset='latest',  # Start reading from latest messages
                    enable_auto_commit=True,
                    group_id='position_tracker_group'  # Consumer group ID
                )
                print(f"Successfully connected to Kafka topic: {self.topic}")
                return
            except Exception as e:
                retries += 1
                if retries == self.max_retries:
                    print("\nError: Could not connect to Kafka. Please ensure Kafka is running.")
                    print("\nTo start Kafka using Docker:")
                    print("1. Install Docker if not already installed")
                    print("2. Run the following commands:")
                    print("   docker run -d --name kafka-container \\")
                    print("   -p 2181:2181 -p 9092:9092 \\")
                    print("   -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \\")
                    print("   -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \\")
                    print("   -e KAFKA_ZOOKEEPER_CONNECT=localhost:2181 \\")
                    print("   wurstmeister/kafka")
                    sys.exit(1)
                print(f"Kafka not available, retrying in 5 seconds... (Error: {str(e)})")
                time.sleep(5)

    def format_message(self, message):
        """Format the message for display"""
        data = message.value
        event_type = data['data']['event_type']
        asset = data['data']['asset']
        
        if event_type == 'POSITION_OPENED':
            return (
                f"🟢 OPENED: {asset}\n"
                f"    Direction: {data['data']['direction']}\n"
                f"    Size: {data['data']['size']}\n"
                f"    Leverage: {data['data']['leverage']}x\n"
                f"    Entry Price: {data['data']['entry_price']}\n"
                f"    Notional Value: {data['data']['notional_value']}"
            )
        elif event_type == 'POSITION_CLOSED':
            return (
                f"🔴 CLOSED: {asset}\n"
                f"    Final PnL: {data['data']['pnl']}\n"
                f"    Last Size: {data['data']['size']}\n"
                f"    Direction: {data['data']['direction']}"
            )
        else:
            return f"ℹ️ {event_type}: {asset}"

    def run(self):
        """Start consuming messages"""
        print(f"\nStarting position monitor for topic: {self.topic}")
        print("Waiting for position updates...\n")
        
        try:
            for message in self.consumer:
                try:
                    formatted_message = self.format_message(message)
                    print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print(formatted_message)
                    print("-" * 50)
                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                    print(f"Raw message: {message.value}")
        
        except KeyboardInterrupt:
            print("\nShutting down position monitor...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    try:
        # Initialize and run the consumer
        consumer = PositionConsumer(
            bootstrap_servers=['localhost:9092'],
            topic='vault_positions',
            max_retries=5
        )
        
        consumer.run()
    except Exception as e:
        print(f"\nError: {str(e)}") 