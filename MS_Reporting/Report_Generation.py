from fastapi import FastAPI, HTTPException
import asyncio
import aio_pika
import json

app = FastAPI()

RABBITMQ_URL = "amqp://localhost"

async def consume_and_process_messages():
    """
    Consumes messages from 'report_generation_queue' and processes them.
    """
    try:
        # Connect to RabbitMQ
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()
            
            # Declare the queue
            queue = await channel.declare_queue("report_generation_queue", durable=True)

            # Consume messages
            async for message in queue:
                async with message.process():
                    # Decode the message
                    claim_data = json.loads(message.body)
                    print(f"Received message from report_generation_queue: {claim_data}")

                    # Simulate report generation logic
                    await asyncio.sleep(10)  # Simulate processing time
                    print(f"Report generated for claim {claim_data['claimId']} with status: {claim_data['status']}")

                    # Final processing step (no further queue in this case)
                    print(f"Finalized claim processing for: {claim_data['claimId']}")

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
    uvicorn.run(app, host="0.0.0.0", port=4003)
