import asyncio
import websockets
import json


# Function to handle the chat client
async def connect_to_server():
    uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"  # The WebSocket server URI

    async with websockets.connect(uri) as websocket:
        print(f"Connected to the server at {uri}.")

        try:
            while True:
                # Receive the response message from the server
                response = await websocket.recv()

                # Parse the text into json
                parsed_data = json.loads(response)
                print(f"json response {parsed_data}")
                
        except Exception as e:
            print(f"An error occurred: {e}")

# Run the client
if __name__ == "__main__":
    # Run the client
    asyncio.get_event_loop().run_until_complete(connect_to_server())