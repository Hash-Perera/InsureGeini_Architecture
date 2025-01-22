from fastapi import FastAPI, HTTPException
import asyncio
import aio_pika
import json

app = FastAPI()

RABBITMQ_URL = "amqp://localhost"

async def consume_and_process_messages():
    """
    Consumes messages from 'fraud_detection_queue',
    processes them, and publishes the result to 'sentiment_analysis_queue'.
    """
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue("fraud_detection_queue", durable=True)

            async for message in queue:
                async with message.process():
                    claim_data = json.loads(message.body)
                    print(f"Received message from fraud_detection_queue: {claim_data}")

                    # Simulate fraud detection logic
                    await asyncio.sleep(20)
                    print(f"Fraud detection completed for claim {claim_data['claimId']}")

                    # Forward message to report_generation_queue
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps({"claimId": claim_data["claimId"], "status": "Fraud Checked"}).encode("utf-8")
                        ),
                        routing_key="report_generation_queue",
                    )
                    print(f"Message forwarded to report_generation_queue: {claim_data['claimId']}")

    except Exception as e:
        print(f"Error in consume_and_process_messages: {e}")


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_and_process_messages())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=4002)
