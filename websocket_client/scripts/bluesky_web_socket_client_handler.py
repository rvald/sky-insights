import websockets
import json


class BlueSkyWebSocketClientHandler:
    def __init__(self, uri):
        """Initialize the WebSocket client with a server URI and Kafka Producer."""
        self.uri = uri

    async def messages(self):
        """Connect to the WebSocket server and handle incoming messages."""
        async with websockets.connect(self.uri) as websocket:
            print(f"Connected to the server at {self.uri}")
            try:
                while True:
                    # Receive response message from the server
                    response = await websocket.recv()
                    yield self.parse_response(response)

            except Exception as e:
                print(f"An error occurred: {e}")

    def parse_response(self, response):
        """Parse the response from the server into JSON format."""
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            print("Failed to parse response as JSON")
            return None
