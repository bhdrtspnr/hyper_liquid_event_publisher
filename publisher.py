##testing publishing to kafka topics, messages can be consumed from the topic
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
from scraper import scrape_positions
import pandas as pd
from datetime import datetime
import sys
from position_tracker import PositionTracker

class PositionPublisher:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic='vault_positions', max_retries=5):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.max_retries = max_retries
        self.position_tracker = PositionTracker()  # Initialize the tracker
        self.connect_to_kafka()

    def connect_to_kafka(self):
        retries = 0
        while retries < self.max_retries:
            try:
                print(f"Attempting to connect to Kafka (attempt {retries + 1}/{self.max_retries})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print("Successfully connected to Kafka!")
                return
            except NoBrokersAvailable:
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
                print(f"Kafka not available, retrying in 5 seconds...")
                time.sleep(5)

    def publish_changes(self, changes):
        for change in changes:
            # Add timestamp and format message
            message = {
                'timestamp': datetime.utcnow().isoformat(),
                'vault_address': '0x8fc7c0442e582bca195978c5a4fdec2e7c5bb0f7',
                'data': change
            }
            
            try:
                self.producer.send(self.topic, value=message)
                print(f"Published {change['event_type']} event for {change['asset']}")
                print(f"Details: {json.dumps(message, indent=2)}")
            except Exception as e:
                print(f"Error publishing message: {str(e)}")
        
        # Ensure messages are sent
        self.producer.flush()
    
    def run(self, vault_url, interval_seconds=5):
        print(f"Starting position monitoring for {vault_url}")
        print(f"Publishing to topic: {self.topic}")
        
        while True:
            try:
                # Fetch current positions
                current_positions = scrape_positions(vault_url)
                
                # Detect changes using the shared tracker
                closed_positions, opened_positions = self.position_tracker.detect_position_changes(current_positions)
                
                # Publish all changes
                all_changes = closed_positions + opened_positions
                if all_changes:
                    self.publish_changes(all_changes)
                else:
                    print(f"No changes detected at {datetime.utcnow().isoformat()}")
                
                time.sleep(interval_seconds)
                
            except Exception as e:
                print(f"Error in monitoring loop: {str(e)}")
                time.sleep(interval_seconds)

if __name__ == "__main__":
    vault_url = "https://app.hyperliquid.xyz/vaults/0x8fc7c0442e582bca195978c5a4fdec2e7c5bb0f7"
    
    try:
        # Initialize and run the publisher
        publisher = PositionPublisher(
            bootstrap_servers=['localhost:9092'],
            topic='vault_positions',
            max_retries=5
        )
        
        publisher.run(vault_url)
    except KeyboardInterrupt:
        print("\nShutting down position monitor...")
    except Exception as e:
        print(f"\nError: {str(e)}")
