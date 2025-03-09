from aiokafka import AIOKafkaProducer
from hybrid_message_logger import HybridMessageLogger
from bluesky_web_socket_client_handler import BlueSkyWebSocketClientHandler
import json
import uuid
import asyncio


class PostProducer:

    def __init__(self):
        
        self.message_topic = "bluesky-raw-posts"
        self.hybrid_logger = HybridMessageLogger()
        self.client = BlueSkyWebSocketClientHandler(uri="wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post")

    async def initialize(self):
        """Initialize the Kafka producer with specific configuration."""
        
        self.producer = AIOKafkaProducer(
            key_serializer=lambda key: str(key).encode(),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        await self.hybrid_logger.initialize()
        await self.producer.start()
        await self.start()
        

    async def start(self):
        """Connect to the WebSocket server and handle incoming messages."""
        try:

            async for message in self.client.messages():
            
                message_key = str(uuid.uuid4())
                message_data = json.dumps(message)

                # IMPORTANT: await the async database call
                await self.hybrid_logger.add_event(
                    message_key,
                    message_data.encode('utf-8')
                )

                # If post_producer.send_message is sync, this is fine;
                # otherwise, you may also need to await it if it's async.
                await self.send_message(message_key, message_data)

        except Exception as err:
            print(f"An error occurred connectiing to socket: {err}")

    def parse_response(self, response):
        """Parse the response from the server into JSON format."""
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            print("Failed to parse response as JSON")
            return None

    async def send_message(self, message_key, message):
        """
        Send a message with a specific key to the Kafka topic, awaiting the result.
        """
        try:
            record_meta = await self.producer.send_and_wait(
                self.message_topic,
                key=message_key,
                value=message
            )
            print(f"Message with key '{message_key}' sent successfully. Offset={record_meta.offset}")

            # If successful and you want to remove the event from the transient DB:
            # (Assuming your logger’s remove_event is async)
            try:
                await self.hybrid_logger.remove_event(message_key)
            except Exception as e:
                print(f"Failed to remove event for '{message_key}': {e}")

        except Exception as e:
            print(f"Send failed for key '{message_key}': {e}")

            # If you want to move the event to 'failed' on error:
            try:
                await self.hybrid_logger.move_to_failed(message_key)
            except Exception as me:
                print(f"Failed to move the event for key '{message_key}': {me}")

    async def close(self):
        """Close the Kafka producer asynchronously."""
        await self.producer.stop()

if __name__ == "__main__":
    producer = PostProducer()
    asyncio.run(producer.initialize())
    