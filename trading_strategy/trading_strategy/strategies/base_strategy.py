import logging
from datetime import datetime

class BaseStrategy:
    """
    Base class for trading strategies.  Provides common functionality
    such as logging, order management abstractions, and helper methods.
    """

    def __init__(self, trading_session, symbol):
        """
        Initialize the base strategy.

        Args:
            trading_session (TradingSession):  The main trading session instance.
            symbol (str): The symbol this strategy will trade.
        """
        self.ts = trading_session
        self.logger = logging.getLogger(f'trading_strategy.{self.__class__.__name__}')
        self.symbol = symbol
        self.position = None  # Keep track of the current position
        self.order_id = None

    def get_historical_data(self, days=30):
        """
        Fetch historical data for the strategy's symbol.

        Args:
            days (int): The number of days of historical data to retrieve.
        """
        return self.ts.get_historical_data(self.symbol, days=days)

    def get_live_data(self):
        """
        Get the latest live market data for the strategy's symbol.
        """
        return self.ts.live_data.get(self.symbol)

    def is_market_open(self):
        """
        Check if the market is currently open.
        """
        return self.ts.is_market_open()

    def place_order(self, side, qty, order_type="MARKET", price=0, retries=3):
        """
        Place an order using the trading session's order placement method.

        Args:
            side (str): 'buy' or 'sell'.
            qty (int): Quantity to trade.
            order_type (str): 'MARKET', 'LIMIT', 'SL', or 'SL-M'.
            price (float):  Limit or stop price, if applicable.
            retries (int): Number of times to retry the order.
        """
        order_id = self.ts.place_order(self.symbol, side, qty, order_type, price, retries)
        if order_id:
            self.order_id = order_id # store the order id.
            return True
        return False

    def close_position(self):
        """Close the current position, if any."""
        if self.position:
            self.logger.info(f"Closing position for {self.symbol}")
            if self.ts.close_position(self.symbol):
                self.position = None  # Clear the position
                self.order_id = None
                return True
            else:
                return False
        else:
            self.logger.info(f"No position to close for {self.symbol}")
            return True # No position to close

    def on_tick(self, tick_data):
        """
        Called whenever new market data (tick) is received for the symbol.
        This is where the strategy's core logic resides.  This method *must* be overridden
        by any class that inherits from BaseStrategy

        Args:
            tick_data (dict): The market data for the symbol.
        """
        raise NotImplementedError("on_tick() must be implemented in a derived strategy class.")

    def run(self):
        """
        Runs the strategy.  This sets up the data feed and calls the on_tick
        method.
        """
        if not self.ts.running:
            self.logger.error("Trading session is not running.")
            return

        self.logger.info(f"Running strategy for {self.symbol}")
        # Get initial historical data
        try:
            self.historical_data = self.get_historical_data()
        except Exception as e:
            self.logger.error(f"Failed to get initial historical data: {e}")
            return

        # Main loop: Process incoming ticks
        while self.ts.running:
            live_data = self.get_live_data()
            if live_data:
                self.on_tick(live_data)
            time.sleep(1)  #  Consider making this configurable

        self.logger.info(f"Strategy for {self.symbol} stopped.")
