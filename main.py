import requests
import os
import json
import threading
import time
import zlib
from typing import Dict, List, Optional

# --- Azure Web PubSub Client ---
from azure.messaging.webpubsubclient import WebPubSubClient
from azure.messaging.webpubsubclient.models import (
    OnConnectedArgs,
    OnDisconnectedArgs,
    OnGroupDataMessageArgs,
    CallbackType,
)

# --- Protobuf Imports ---
from google.protobuf import any_pb2

# --- Local Decompression Imports ---
# Import the functions from your new file
from decompression_utils import (
    decompress_gex_message,
    decompress_greek_message,
    decompress_orderflow_message
)
# -----------------------------------

# --- Configuration ---

# Get API key from an environment variable for security.
# Using your provided key as default
API_KEY = os.environ.get("GEXBOT_API_KEY", "")

BASE_URL = "https://api.gexbot.com"
NEGOTIATE_URL = f"{BASE_URL}/negotiate"

# --- USER SELECTION: Uncomment the feeds you want to subscribe to ---

# Select Tickers (used for all hubs)
ACTIVE_TICKERS = [
    # "SPX",
    "ES_SPX",
    # "NDX",
    # "NQ_NDX",
    # "RUT",
    # "SPY",
    # "QQQ",
    # "TQQQ",
    # "UVXY",
    # "AAPL",
    # "TSLA",
    # "MSFT",
    # "AMZN",
    # "NVDA",
    # "META",
    # "NFLX",
    # "AVGO",
    # "MSTR",
    # "VIX",
    # "GOOG",
    # "IWM",
    # "TLT",
    # "GLD",
    # "USO",
    # "GOOGL",
    # "AMD",
    # "SMCI",
    # "COIN",
    # "PLTR",
    # "APP",
    # "BABA",
    # "SNOW",
    # "IONQ",
    # "HOOD",
    # "CRWD",
    # "MU",
    # "CRWV",
    # "INTC",
    # "UNH",
    # "VALE",
    # "IBIT",
    # "SLV",
    # "HYG",
    # "SOFI",
    # "GME",
    # "TSM",
    # "ORCL",
    # "RDDT",
]

# Select categories for the 'classic' hub
ACTIVE_CLASSIC_CATEGORIES = [
    # "gex_full",
    # "gex_zero",
    # "gex_one",
]

# Select categories for the 'state_gex' hub
ACTIVE_STATE_GEX_CATEGORIES = [
    # "gex_full",
    # "gex_zero",
    # "gex_one",
]

# Select categories for the 'state_greeks_zero' hub
ACTIVE_STATE_GREEKS_ZERO_CATEGORIES = [
    "volume_zero",
    # "delta_zero",
    # "gamma_zero",
    # "vanna_zero",
    # "charm_zero",
]

# Select categories for the 'state_greeks_one' hub
ACTIVE_STATE_GREEKS_ONE_CATEGORIES = [
    # "volume_one",
    # "delta_one",
    # "gamma_one",
    # "vanna_one",
    # "charm_one",
]

# Select categories for the 'orderflow' hub
ACTIVE_ORDERFLOW_CATEGORIES = [
    # "orderflow",
]

# --- End of USER SELECTION ---


# --- Group Name Generation ---

def _generate_group_names(tickers: List[str], package: str, categories: List[str]) -> List[str]:
    """Helper to create all combinations for group names."""
    groups = []
    for ticker in tickers:
        for category in categories:
            groups.append(f"blue_{ticker}_{package}_{category}")
    return groups


# Dynamically build GROUP_CONFIG from the active selections
GROUP_CONFIG = {
    "classic": _generate_group_names(
        ACTIVE_TICKERS, "classic", ACTIVE_CLASSIC_CATEGORIES
    ),
    "state_gex": _generate_group_names(
        ACTIVE_TICKERS, "state", ACTIVE_STATE_GEX_CATEGORIES
    ),
    "state_greeks_zero": _generate_group_names(
        ACTIVE_TICKERS, "state", ACTIVE_STATE_GREEKS_ZERO_CATEGORIES
    ),
    "state_greeks_one": _generate_group_names(
        ACTIVE_TICKERS, "state", ACTIVE_STATE_GREEKS_ONE_CATEGORIES
    ),
    "orderflow": _generate_group_names(
        ACTIVE_TICKERS, "orderflow", ACTIVE_ORDERFLOW_CATEGORIES
    ),
}
# ------------------------------


# --- Web PubSub Client Manager ---


class WebPubSubClientManager:
    """
    Manages a single Web PubSub client connection, event handling,
    and group joins in a separate thread.
    """

    def __init__(self, hub_key: str, connection_url: str, groups_to_join: List[str]):
        self.hub_key = hub_key
        self.groups_to_join = groups_to_join
        self.client = WebPubSubClient(connection_url)
        self.thread: Optional[threading.Thread] = None

        # Register event handlers
        # Use .subscribe() with CallbackType enum instead of .on() with strings
        self.client.subscribe(CallbackType.CONNECTED, self.on_connected)
        self.client.subscribe(CallbackType.DISCONNECTED, self.on_disconnected)
        self.client.subscribe(CallbackType.GROUP_MESSAGE,
                              self.on_group_message)

    def start(self):
        """Starts the client in a new daemon thread."""
        print(f"[{self.hub_key}] Starting client thread...")
        # Use client.open() which is the correct method to start the connection
        self.thread = threading.Thread(target=self.client.open, daemon=True)
        self.thread.start()

    def stop(self):
        """Stops the client."""
        print(f"[{self.hub_key}] Stopping client...")
        # Use client.close() which is the correct method to stop
        self.client.close()

    def on_connected(self, event: OnConnectedArgs):
        """Handle connection event and join groups."""
        print(f"[{self.hub_key}] ✅ Web PubSub connected (ID: {event.connection_id})")
        for group in self.groups_to_join:
            try:
                self.client.join_group(group)
                print(f"[{self.hub_key}] 📢 Joined group: {group}")
            except Exception as e:
                print(f"[{self.hub_key}] ❌ Failed to join group {group}: {e}")

    def on_disconnected(self, event: OnDisconnectedArgs):
        """Handle disconnection event."""
        print(f"[{self.hub_key}] ❌ Web PubSub disconnected: {event.message}")

    def on_group_message(self, event: OnGroupDataMessageArgs):
        """Handle incoming group messages."""
        # print(f"[{self.hub_key}] 📩 Message from group {event.group} ({len(event.data)} bytes)")

        try:
            # 1. Decode the outer Any wrapper
            any_message = any_pb2.Any()
            any_message.ParseFromString(event.data)

            message_type_url = any_message.type_url
            # print(f"  Type URL: {message_type_url}")

            # 2. ROBUST Category Extraction
            # The group format is: blue_{ticker}_{package}_{category}
            # We split by the known package names to isolate the category,
            # regardless of how many underscores the ticker has.
            current_category = ""
            known_packages = ["classic", "state", "orderflow"]

            for pkg in known_packages:
                # We look for '_{pkg}_' to ensure we don't match partial ticker names
                separator = f"_{pkg}_"
                if separator in event.group:
                    # Take everything AFTER the package name
                    current_category = event.group.split(separator)[-1]
                    break

            if not current_category:
                # Fallback/Debug if pattern doesn't match
                print(
                    f"  ⚠️ Could not extract category from group: {event.group}")
                return

            # print(f"  Extracted Category: {current_category}")

            # 3. Route based on type_url
            if "proto.gex" in message_type_url:
                gex_data = decompress_gex_message(any_message)
                if gex_data:
                    print(
                        f"[{self.hub_key}] GEX: {gex_data.get('ticker')} @ {gex_data.get('spot')}")

            elif "proto.greek" in message_type_url:
                # Now 'current_category' will be cleanly 'volume_zero', matching your utils check
                greek_data = decompress_greek_message(
                    any_message, current_category)
                if greek_data:
                    # Differentiate output based on what we got back
                    if "mini_contracts" in greek_data:
                        print(
                            f"[{self.hub_key}] {current_category}: {greek_data.get('ticker')} (JSON path)")
                    else:
                        print(
                            f"[{self.hub_key}] {current_category}: {greek_data.get('ticker')} (Proto path)")

            elif "proto.orderflow" in message_type_url:
                orderflow_data = decompress_orderflow_message(any_message)
                if orderflow_data:
                    print(
                        f"[{self.hub_key}] Orderflow: {orderflow_data.get('ticker')}")

            else:
                print(f"  Unknown message type_url: {message_type_url}")

        except Exception as e:
            print(f"  Failed to parse protobuf message: {e}")
            # print(f"  Raw data (first 50 bytes): {event.data[:50]!r}...")


# --- Negotiation Function (from your script) ---

def get_negotiate_response(api_key: str) -> Optional[Dict]:
    """
    Hits the /negotiate endpoint using an API key for authentication.
    """
    if not api_key:
        print("Error: GEXBOT_API_KEY is not set.")
        return None

    params = {"key": api_key}
    print(f"Connecting to {NEGOTIATE_URL}...")

    try:
        response = requests.get(NEGOTIATE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response body: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")
    return None


# --- Main Script ---
if __name__ == "__main__":
    negotiate_data = get_negotiate_response(API_KEY)

    if not (negotiate_data and 'websocket_urls' in negotiate_data):
        print("\n--- Negotiation Failed or 'websocket_urls' key missing ---")
        if negotiate_data:
            print(json.dumps(negotiate_data, indent=2))
        exit()

    print("\n--- Successfully Negotiated ---")
    websocket_urls_dict = negotiate_data['websocket_urls']
    # print(websocket_urls_dict)
    # print(json.dumps(websocket_urls_dict, indent=2))

    # --- Start Clients ---
    print("\n--- Initializing WebSocket Clients based on GROUP_CONFIG ---")
    client_managers: List[WebPubSubClientManager] = []

    for hub_key, url in websocket_urls_dict.items():
        groups_to_join = GROUP_CONFIG.get(hub_key)

        if groups_to_join:
            print(
                f"[Main] Found {len(groups_to_join)} groups for hub: {hub_key}")
            manager = WebPubSubClientManager(hub_key, url, groups_to_join)
            manager.start()
            client_managers.append(manager)
        else:
            print(
                f"[Main] Skipping {hub_key}: No groups defined in GROUP_CONFIG")

    if not client_managers:
        print("\nNo clients were started. Check your GROUP_CONFIG.")
        exit()

    # Keep the main thread alive to let daemon threads run
    print("\nClients are running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n--- Stopping Clients ---")
        for manager in client_managers:
            manager.stop()
        print("All clients stopped. Exiting.")
