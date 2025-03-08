import asyncio
from hybrid_message_logger import HybridMessageLogger
from bluesky_web_socket_client_handler import BlueSkyWebSocketClientHandler


class CollectionServiceWebSocketClient:

    def __init__(self):
        try:
            logger = HybridMessageLogger()
            logger.initialize()
        except Exception as err:
            print("Could not initialize HybridMessageLogger!")
            raise 

        # Define the WebSocket server URI
        uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

        # Create an instance of WebSocketClient
        self.client = BlueSkyWebSocketClientHandler(uri)

    async def start(self):
         await self.client.connect()

if __name__ == "__main__":
        collection_service_web_socket_client = CollectionServiceWebSocketClient()

        # Run the client's connection coroutine
        asyncio.get_event_loop().run_until_complete(collection_service_web_socket_client.start())



    