import os
import json
import logging
import configparser
from datetime import datetime
import time
from threading import Thread, Lock, Event
from urllib.parse import parse_qs, urlparse
import pickle
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# Initialize logging
logger = logging.getLogger('trading_strategy')

class FyersWebSocket:
    """Handles WebSocket connection to Fyers for market data."""

    def __init__(self, client_id, access_token):
        """Initialize FyersWebSocket."""
        self.client_id = client_id
        self.access_token = access_token
        self.ws = None
        self.running = True
        self.connected = False
        self.last_message_time = time.time()
        self.reconnect_delay = 5
        self.max_reconnect_delay = 300
        self.connection_timeout = 30
        self.lock = Lock()
        self.connection_event = Event()
        self.symbols = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]  # Default symbols, can be moved to config

    def connect(self):
        """Initialize and connect WebSocket."""
        try:
            logger.info("Initializing WebSocket connection...")
            self.ws = data_ws.FyersDataSocket(
                access_token=f"{self.client_id}:{self.access_token}",
                log_path="logs/",
                litemode=False,
                write_to_file=False
            )

            self.ws.on_connect = self._on_connect
            self.ws.on_message = self._on_message
            self.ws.on_error = self._on_error
            self.ws.on_close = self._on_close

            self.ws.connect()

            if not self.connection_event.wait(timeout=10):
                raise Exception("Connection timeout")

            logger.info("WebSocket connection established")

            self._start_connection_monitor()

            while self.running:
                time.sleep(1)

        except Exception as e:
            logger.error(f"Error in WebSocket connection: {e}")
            self._reconnect()

    def _on_connect(self):
        """Callback when the WebSocket connection is established."""
        logger.info("WebSocket connected")
        self.connected = True
        self.connection_event.set()
        self.ws.subscribe(symbols=self.symbols)

    def _on_message(self, message):
        """Callback when a message is received from the WebSocket."""
        try:
            self.last_message_time = time.time()
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message

            if isinstance(data, dict):
                if data.get('s') == 'ok':
                    logger.info(f"Message: {data}")
                elif data.get('s') == 'error':
                    logger.error(f"WebSocket error: {data.get('message', '')}")
            elif isinstance(data, list):
                for item in data:
                    self._process_market_data(item)

        except Exception as e:
            logger.error(f"Error in _on_message: {e}")

    def _on_error(self, error):
        """Callback when an error occurs on the WebSocket."""
        logger.error(f"WebSocket error: {error}")
        self.connected = False
        self.connection_event.clear()
        self._reconnect()

    def _on_close(self, message=""):
        """Callback when the WebSocket connection is closed."""
        logger.warning(f"WebSocket connection closed: {message}")
        self.connected = False
        self.connection_event.clear()
        if self.running:
          self._reconnect()

    def _process_market_data(self, data):
        """Process market data messages."""
        try:
            symbol = data.get('symbol', '')
            if symbol in self.symbols:
                ltp = data.get('ltp', 0)
                timestamp = data.get('timestamp', datetime.now().timestamp())
                dt = datetime.fromtimestamp(timestamp)
                logger.info(f"[{dt}] {symbol}: LTP={ltp}")
        except Exception as e:
            logger.error(f"Error processing market data: {e}")

    def _reconnect(self):
        """Reconnect to the WebSocket with exponential backoff."""
        if not self.running:
            return

        delay = self.reconnect_delay
        while self.running and not self.connected:
            try:
                logger.info(f"Attempting to reconnect in {delay} seconds...")
                time.sleep(delay)

                if self.ws:
                    try:
                        self.ws.close()
                    except Exception:
                        pass

                self.connection_event.clear()
                self.connect()

                if self.connected:
                    logger.info("Reconnection successful")
                    break

            except Exception as e:
                logger.error(f"Reconnection attempt failed: {e}")
                delay = min(delay * 2, self.max_reconnect_delay)

    def _start_connection_monitor(self):
        """Start a thread to monitor the connection."""
        def monitor():
            while self.running:
                try:
                    current_time = time.time()
                    if current_time - self.last_message_time > self.connection_timeout:
                        logger.warning("Connection timeout detected")
                        self.connected = False
                        self.connection_event.clear()
                        self._reconnect()
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Error in connection monitor: {e}")

        monitor_thread = Thread(target=monitor, daemon=True)
        monitor_thread.start()
        logger.info("Connection monitor started")

    def close(self):
        """Close the WebSocket connection."""
        self.running = False
        if self.ws:
            try:
                self.ws.close()
                logger.info("WebSocket connection closed")
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")

    def _generate_access_token(self):
        """Generate access token using Fyers API."""
        try:
            session = fyersModel.SessionModel(
                client_id=self.client_id,
                secret_key=self.secret_key,
                redirect_uri=self.redirect_uri,
                response_type="code",
                grant_type="authorization_code"
            )

            auth_url = session.generate_authcode()
            print(f"\nVisit this URL to get authorization code:\n{auth_url}")
            webbrowser.open(auth_url)
            callback_url = input("\nEnter the auth code from the redirect URL: ").strip()
            parsed_url = urlparse(callback_url)
            query_params = parse_qs(parsed_url.query)
            auth_code = query_params.get("auth_code", [None])[0]

            if not auth_code:
                raise Exception("No auth code found in callback URL")

            session.set_token(auth_code)
            response = session.generate_token()

            if response.get('s') == 'ok':
                self.access_token = response.get('access_token')
                logger.info("Access token generated successfully")
                return True
            else:
                logger.error(f"Failed to generate access token: {response.get('message')}")
                return False

        except Exception as e:
            logger.error(f"Error generating access token: {e}")
            return False

if __name__ == "__main__":
    """Main function to run the FyersWebSocket client."""
    try:
        # Load configuration
        config = configparser.ConfigParser()
        config.read('config.ini')
        client_id = config.get('Credentials', 'CLIENT_ID')
        secret_key = config.get('Credentials', 'SECRET_KEY')
        redirect_uri = config.get('Credentials', 'REDIRECT_URI')

        # Create WebSocket client
        ws_client = FyersWebSocket(client_id, secret_key, redirect_uri)
        try:
            # Connect and start processing messages
            ws_client.connect()
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, closing connection...")
        finally:
            # Ensure cleanup on exit
            ws_client.close()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        traceback.print_exc()
