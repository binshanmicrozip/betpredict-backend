import asyncio
import json
import websockets
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Test the historical data WebSocket endpoint"

    def add_arguments(self, parser):
        parser.add_argument("--host", default="localhost", help="WebSocket host")
        parser.add_argument("--port", type=int, default=8000, help="WebSocket port")
        parser.add_argument("--test-type", choices=["cricket", "market", "combined"], default="cricket",
                          help="Type of data to test")

    def handle(self, *args, **options):
        host = options["host"]
        port = options["port"]
        test_type = options["test_type"]

        uri = f"ws://{host}:{port}/ws/bet-signals/"

        async def test_websocket():
            try:
                async with websockets.connect(uri) as websocket:
                    self.stdout.write(f"Connected to {uri}")

                    # Wait for connection established message
                    response = await websocket.recv()
                    self.stdout.write(f"Connection response: {response}")

                    # Send test request based on type
                    if test_type == "cricket":
                        request = {
                            "type": "get_cricket_history",
                            "match_id": "151807",
                            "source_match_id": "151807",
                            "limit": 10
                        }
                    elif test_type == "market":
                        request = {
                            "type": "get_market_history",
                            "market_id": "1.256856948",
                            "runner_id": "509959",
                            "limit": 10,
                            "hours_back": 1
                        }
                    elif test_type == "combined":
                        request = {
                            "type": "get_combined_history",
                            "match_id": "151807",
                            "source_match_id": "151807",
                            "market_id": "1.256856948",
                            "runner_id": "509959",
                            "limit": 20
                        }

                    await websocket.send(json.dumps(request))
                    self.stdout.write(f"Sent request: {json.dumps(request, indent=2)}")

                    # Receive response
                    response = await websocket.recv()
                    response_data = json.loads(response)

                    self.stdout.write(f"Response type: {response_data.get('type')}")

                    if response_data.get("type") == "cricket_history_response":
                        self.stdout.write(f"Cricket snapshots: {response_data.get('total_snapshots')}")
                        self.stdout.write(f"Cricket commentary: {response_data.get('total_commentary')}")

                        # Show last few cricket snapshots
                        snapshots = response_data.get("cricket_snapshots", [])
                        if snapshots:
                            self.stdout.write("Last cricket snapshot:")
                            self.stdout.write(json.dumps(snapshots[-1], indent=2))

                    elif response_data.get("type") == "market_history_response":
                        self.stdout.write(f"Market ticks: {response_data.get('total_ticks')}")

                        # Show last few market ticks
                        ticks = response_data.get("market_ticks", [])
                        if ticks:
                            self.stdout.write("Last market tick:")
                            self.stdout.write(json.dumps(ticks[-1], indent=2))

                    elif response_data.get("type") == "combined_history_response":
                        self.stdout.write(f"Combined cricket: {response_data.get('total_cricket')}")
                        self.stdout.write(f"Combined market: {response_data.get('total_market')}")

                        # Show summary
                        cricket = response_data.get("cricket_snapshots", [])
                        market = response_data.get("market_ticks", [])
                        if cricket and market:
                            self.stdout.write("Sample combined data:")
                            self.stdout.write(f"Cricket: {cricket[-1]['score']}/{cricket[-1]['wickets']} ({cricket[-1]['overs']} ov)")
                            self.stdout.write(f"Market: LTP {market[-1]['ltp']}")

                    elif response_data.get("type") == "error":
                        self.stdout.write(self.style.ERROR(f"Error: {response_data.get('message')}"))
                    else:
                        self.stdout.write(f"Raw response: {json.dumps(response_data, indent=2)}")

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"WebSocket error: {e}"))

        asyncio.run(test_websocket())