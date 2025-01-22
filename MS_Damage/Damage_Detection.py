from fastapi import FastAPI
from pydantic import BaseModel
import asyncio
import aio_pika
import json

app = FastAPI()

RABBITMQ_URL = "amqp://localhost"

class ClaimMessage(BaseModel):
    claimId: str

async def consume_and_process_messages():
    """
    Consumes messages from the 'claim_queue', processes them,
    and publishes the result to the 'fraud_detection_queue'.
    """
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()

            # Ensure the queue declaration matches the existing setup
            queue = await channel.declare_queue(
                "claim_queue", durable=True  # Match the configuration of the existing queue
            )

            async for message in queue:
                async with message.process():
                    # Decode the message
                    claim_data = json.loads(message.body)
                    print(f"Received message from claim_queue: {claim_data}")

                    # Simulate damage detection logic
                    await asyncio.sleep(20)
                    print(f"Damage detection completed for claim {claim_data['claimId']}")

                    # Forward message to fraud_detection_queue
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps({"claimId": claim_data["claimId"], "status": "Damage Detected"}).encode("utf-8")
                        ),
                        routing_key="fraud_detection_queue",
                    )
                    print(f"Message forwarded to fraud_detection_queue: {claim_data['claimId']}")

    except Exception as e:
        print(f"Error in consume_and_process_messages: {e}")


@app.on_event("startup")
async def startup_event():
    """
    On application startup, start the consumer in the background.
    """
    asyncio.create_task(consume_and_process_messages())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=4001)
