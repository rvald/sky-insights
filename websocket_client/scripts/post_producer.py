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

                target_keys = [
                    'did',
                    'time_us',
                    'kind',
                    'cid',
                    'operation',
                    'created_at',
                    'createdAt',
                    'langs',
                    'text'
                ]

                extracted_values = await self.find_keys(message, target_keys)

                if extracted_values["kind"][0] == "commit" and extracted_values["operation"] is not None and extracted_values["operation"][0] == "create":
                    
                    if len(extracted_values["created_at"]) == 0:
                        created_at = extracted_values["createdAt"][0]
                    else:
                        created_at = extracted_values["created_at"][0]

                    post_dict = {
                        'did':        extracted_values["did"][0] if extracted_values.get("did") and len(extracted_values["did"]) > 0 and extracted_values["did"][0] != "" else None,
                        'time_us':    extracted_values["time_us"][0] if extracted_values.get("time_us") and len(extracted_values["time_us"]) > 0 and extracted_values["time_us"][0] != "" else None,
                        'kind':       extracted_values["kind"][0] if extracted_values.get("kind") and len(extracted_values["kind"]) > 0 and extracted_values["kind"][0] != "" else None,
                        'cid':        extracted_values["cid"][0] if extracted_values.get("cid") and len(extracted_values["cid"]) > 0 and extracted_values["cid"][0] != "" else None,
                        'operation':  extracted_values["operation"][0] if extracted_values.get("operation") and len(extracted_values["operation"]) > 0 and extracted_values["operation"][0] != "" else None,
                        'created_at': created_at,
                        'langs':      extracted_values["langs"][0] if extracted_values.get("langs") and len(extracted_values["langs"]) > 0 and extracted_values["langs"][0] != "" else None,
                        'text':       extracted_values["text"][0] if extracted_values.get("text") and len(extracted_values["text"]) > 0 and extracted_values["text"][0] != "" else None,
                    }

                    message_key = str(uuid.uuid4())
                    message_data = json.dumps(post_dict)

                    # IMPORTANT: await the async database call
                    await self.hybrid_logger.add_event(
                        message_key,
                        message_data.encode('utf-8')
                    )

                    # If post_producer.send_message is sync, this is fine;
                    # otherwise, you may also need to await it if it's async.
                    await self.send_message(message_key, post_dict)

                
        except Exception as err:
            print(f"An error occurred connecting to socket: {err} for value {message} and extracted - {extracted_values}")

    async def find_keys(self, data, target_keys):
        """
        Recursively search the JSON-like dict (or list) for keys in target_keys.
        Returns a dictionary mapping each target key to a list of all found values.
        
        :param data: JSON object as a dict (or list) to search.
        :param target_keys: A list of keys to search for.
        :return: A dict {key: [found_value1, found_value2, ...], ...}
        """
        # Initialize the result dictionary, one empty list for each target key.
        found = {key: [] for key in target_keys}
        
        def recursive_search(current):
            if isinstance(current, dict):
                for k, v in current.items():
                    if k in target_keys:
                        found[k].append(v)
                    # Recurse into the value
                    recursive_search(v)
            elif isinstance(current, list):
                for item in current:
                    recursive_search(item)
        
        recursive_search(data)
        return found

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
    