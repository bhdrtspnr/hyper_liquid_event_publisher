from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from typing import Optional, Literal
import json
from datetime import datetime
import time

app = FastAPI(title="Position Publisher Test API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Kafka configuration
KAFKA_TOPIC = 'vault_positions'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

class MockPosition(BaseModel):
    asset: str
    event_type: Literal['POSITION_OPENED', 'POSITION_CLOSED']
    direction: Optional[str] = "LONG"
    size: Optional[str] = "1.0000"
    leverage: Optional[str] = "10"
    entry_price: Optional[str] = "50000"
    notional_value: Optional[str] = "$50,000"
    mark_price: Optional[str] = "50100"
    pnl: Optional[str] = "$100 (2%)"
    liq_price: Optional[str] = "45000"
    margin: Optional[str] = "$5,000"
    funding: Optional[str] = "$0.50"

class KafkaManager:
    def __init__(self):
        self.producer = None
        self.connect_to_kafka()

    def connect_to_kafka(self):
        retries = 0
        max_retries = 5
        while retries < max_retries:
            try:
                print(f"Attempting to connect to Kafka (attempt {retries + 1}/{max_retries})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print("Successfully connected to Kafka!")
                return
            except NoBrokersAvailable:
                retries += 1
                if retries == max_retries:
                    raise HTTPException(
                        status_code=500,
                        detail="Could not connect to Kafka. Please ensure Kafka is running."
                    )
                print(f"Kafka not available, retrying in 5 seconds...")
                time.sleep(5)

    def publish_message(self, message):
        if not self.producer:
            self.connect_to_kafka()
        try:
            self.producer.send(KAFKA_TOPIC, value=message)
            self.producer.flush()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to publish message: {str(e)}")

kafka_manager = KafkaManager()

@app.post("/publish/position")
async def publish_position(position: MockPosition):
    """
    Publish a mock position event to Kafka.
    """
    message = {
        'timestamp': datetime.utcnow().isoformat(),
        'vault_address': '0x8fc7c0442e582bca195978c5a4fdec2e7c5bb0f7',
        'data': position.dict()
    }
    
    kafka_manager.publish_message(message)
    return {"status": "success", "message": f"Published {position.event_type} event for {position.asset}"}

@app.post("/publish/sample")
async def publish_sample():
    """
    Publish a sample position event (BTC long position opened).
    """
    sample_position = MockPosition(
        asset="BTC",
        event_type="POSITION_OPENED",
        direction="LONG",
        size="2.5000",
        leverage="25",
        entry_price="42000",
        notional_value="$105,000",
        mark_price="42100",
        pnl="$250 (0.24%)",
        liq_price="38000",
        margin="$4,200",
        funding="$1.25"
    )
    
    message = {
        'timestamp': datetime.utcnow().isoformat(),
        'vault_address': '0x8fc7c0442e582bca195978c5a4fdec2e7c5bb0f7',
        'data': sample_position.dict()
    }
    
    kafka_manager.publish_message(message)
    return {"status": "success", "message": "Published sample BTC position"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 