import asyncio
import websockets

# Function to handle the chat client
async def connect_to_server():
    uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"  # The WebSocket server URI

    async with websockets.connect(uri) as websocket:
        print(f"Connected to the server at {uri}.")

        try:
            while True:
                # Receive the reversed message from the server
                reversed_message = await websocket.recv()
                print(f"Received reversed message: {reversed_message}")

        except Exception as e:
            print(f"An error occurred: {e}")

# Run the client
if __name__ == "__main__":
    # Run the client
    asyncio.get_event_loop().run_until_complete(connect_to_server())