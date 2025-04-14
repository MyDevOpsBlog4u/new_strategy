import logging
import time
from datetime import datetime, timedelta
import pandas as pd
import ta  # Import the ta-lib library
import numpy as np
from scipy.stats import norm  # For Black-Scholes
from typing import Optional, Dict

from .base_strategy import BaseStrategy  # Import BaseStrategy

class OptionChainStrategy(BaseStrategy):
    """
    A strategy that combines option chain analysis, greeks, VIX, mean reversion,
    and technical indicators to trade Nifty and Bank Nifty options.
    """

    def __init__(self, trading_session, symbol, strategy_name="OptionChainStrategy"):
        super().__init__(trading_session, symbol, strategy_name)
        self.logger.info(f"OptionChainStrategy initialized for {symbol}")
        self.option_chain = None
        self.vix = None
        self.atm_strike = None
        self.max_pain_strike = None
        self.previous_close = None

        # Strategy Parameters (These could be in a config file)
        self.vix_threshold = 25
        self.delta_threshold = 0.3
        self.mean_reversion_period = 20  # For Bollinger Bands on option prices
        self.stoch_rsi_period = 14
        self.stoch_rsi_k = 3
        self.stoch_rsi_d = 3
        self.adx_period = 14
        self.atr_period = 14
        self.cpr_period = 14  # Periods to calculate pivot points
        self.max_open_positions = 2
        self.position_sizing_method = "fixed"  # "fixed" or "percent"
        self.position_size_fixed = 50  # Fixed quantity
        self.position_size_percent = 0.02  # % of account equity per trade
        self.max_account_risk_per_trade = 0.02 # Maximum % of account value to risk per trade
        self.trailing_stop_loss_pct = 0.1
        self.profit_target_multiplier = 1.1  # Example: 10% profit target
        self.order_type = "LIMIT" # Or "MARKET"
        self.slippage_bps = 2  # Basis points of slippage (0.02%)
        self.commission_per_lot = 20  # Example: ₹20 per lot
        self.use_option_greeks = True
        self.greek_exposure_limit = {  # Example limits
            "delta": 10,
            "gamma": 5,
            "theta": 1000,
            "vega": 5000
        }
        self.current_greek_exposure = {  # Track current exposure.
            "delta": 0,
            "gamma": 0,
            "theta": 0,
            "vega": 0
        }

    def calculate_cpr(self, df):
        """
        Calculates Central Pivot Range (CPR)
        """
        if len(df) < self.cpr_period:
            return df
        
        pivot = (df['high'].iloc[-1] + df['low'].iloc[-1] + df['close'].iloc[-1]) / 3
        bc = (df['high'].iloc[-1] + df['low'].iloc[-1]) / 2
        tc = 2 * pivot - bc
        
        if tc > bc:
            df['cpr_top'] = tc
            df['cpr_bottom'] = bc
        else:
            df['cpr_top'] = bc
            df['cpr_bottom'] = tc
        df['pivot'] = pivot
        return df

    def calculate_indicators(self, df):
        """
        Calculate technical indicators, including those relevant to options and volatility.
        """
        df = super().calculate_indicators(df) #call the parent

        if len(df) < 50:
            return df
        try:
            df["ema_fast"] = ta.EMA(df["close"], timeperiod=int(self.ts.config['EMA_FAST']))
            df["ema_slow"] = ta.EMA(df["close"], timeperiod=int(self.ts.config['EMA_SLOW']))
            df["rsi"] = ta.RSI(df["close"], timeperiod=int(self.ts.config['RSI_PERIOD']))
            df["atr"] = ta.ATR(df["high"], df["low"], df["close"], timeperiod=int(self.ts.config['ATR_PERIOD']))
            df["bb_upper"], df["bb_middle"], df["bb_lower"] = ta.BBANDS(
                df["close"],
                timeperiod=int(self.ts.config['BB_PERIOD']),
                nbdevup=float(self.ts.config['BB_STD_DEV']),
                nbdevdn=float(self.ts.config['BB_STD_DEV'])
            )
            if "volume" in df.columns:
                df["obv"] = ta.OBV(df["close"], df["volume"])
                df["volume_ema"] = ta.EMA(df["volume"], timeperiod=20)
                df["volume_ratio"] = df["volume"] / df["volume_ema"]
                df["vwap"] = (df["volume"] * (df["high"] + df["low"] + df["close"]) / 3).cumsum() / df["volume"].cumsum()

            # Stochastic RSI
            df['stoch_k'] = ta.STOCH(df['high'], df['low'], df['close'],
                                        fastk_period=self.stoch_rsi_period,
                                        slowk_period=self.stoch_rsi_k,
                                        slowd_period=self.stoch_rsi_d)
            df['stoch_d'] = ta.STOCHD(df['high'], df['low'], df['close'],
                                        fastk_period=self.stoch_rsi_period,
                                        slowd_period=self.stoch_rsi_d)

            # MACD
            macd = ta.MACD(df['close'])
            df['macd'] = macd
            df['macd_signal'] = macd.macd_signal
            df['macd_hist'] = macd.macd_diff
            df = self.calculate_cpr(df)
            
        except Exception as e:
            self.logger.error(f"Error calculating indicators: {e}")
            # Consider re-raising or returning a specific error DataFrame/value
        return df

    def get_option_chain_data(self):
        """
        Gets and filters option chain data, and calculates Greeks.
        """
        option_chain_response = self.ts.fyers.option_chain(self.symbol)  # Changed according to the docs
        if not option_chain_response or option_chain_response.get('s') != "ok":  # Added error check
            self.logger.warning(
                f"Could not retrieve option chain data for {self.symbol}: {option_chain_response.get('message')}"
            )
            return None

        option_chain = option_chain_response.get('d', {})  # Extract data

        self.option_chain = option_chain
        self.atm_strike = option_chain['nearest_strike']

        # Calculate max pain
        max_pain = None
        min_pain_value = float('inf')
        for strike in option_chain['calls']:
            call_oi = option_chain['calls'][strike]['oi']
            put_oi = option_chain['puts'][strike]['oi'] if strike in option_chain['puts'] else 0
            pain_value = abs(call_oi - put_oi)
            if pain_value < min_pain_value:
                min_pain_value = pain_value
                max_pain = strike
        self.max_pain_strike = max_pain

        # Basic Greek Calculation (Replace with a proper option pricing model if needed)
        spot_price = option_chain['spot_price']
        time_to_expiry = (datetime.fromisoformat(option_chain['expiry_date']) - datetime.now()).days / 365
        risk_free_rate = 0.05  # Example risk-free rate
        
        for option_type in ['calls', 'puts']:
            for strike, option in option_chain[option_type].items():
                price = option['ltp']
                if price <= 0:
                  option_chain[option_type][strike]['delta'] = 0
                  option_chain[option_type][strike]['gamma'] = 0
                  option_chain[option_type][strike]['theta'] = 0
                  option_chain[option_type][strike]['vega'] = 0
                  continue
                
                try:
                    d1 = (np.log(spot_price / strike) + (risk_free_rate + 0.5 * self.vix**2) * time_to_expiry) / (self.vix * np.sqrt(time_to_expiry))
                    d2 = d1 - self.vix * np.sqrt(time_to_expiry)
                    
                    if option_type == 'calls':
                        delta = norm.cdf(d1)
                        gamma = norm.pdf(d1) / (spot_price * self.vix * np.sqrt(time_to_expiry))
                        theta = spot_price * norm.pdf(d1) * self.vix / (2 * np.sqrt(time_to_expiry)) - risk_free_rate * strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(d2)
                        vega = spot_price * norm.pdf(d1) * np.sqrt(time_to_expiry)
                    else:  # Puts
                        delta = norm.cdf(d1) - 1
                        gamma = norm.pdf(d1) / (spot_price * self.vix * np.sqrt(time_to_expiry))
                        theta = spot_price * norm.pdf(d1) * self.vix / (2 * np.sqrt(time_to_expiry)) + risk_free_rate * strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(-d2)
                        vega = spot_price * norm.pdf(d1) * np.sqrt(time_to_expiry)

                    option_chain[option_type][strike]['delta'] = delta
                    option_chain[option_type][strike]['gamma'] = gamma
                    option_chain[option_type][strike]['theta'] = theta
                    option_chain[option_type][strike]['vega'] = vega
                except Exception as e:
                    self.logger.error(f"Error calculating Greeks for {option_type} at strike {strike}: {e}")
                    option_chain[option_type][strike]['delta'] = 0
                    option_chain[option_type][strike]['gamma'] = 0
                    option_chain[option_type][strike]['theta'] = 0
                    option_chain[option_type][strike]['vega'] = 0

        return option_chain

    def analyze_market_conditions(self):
        """
        Analyzes market conditions using VIX and other factors.
        """
        try:
            vix_data = self.ts.live_data.get(VIX_SYMBOL)
            self.vix = vix_data['lp'] if vix_data and 'lp' in vix_data else None #added check
            high_volatility = self.vix > self.vix_threshold if self.vix is not None else False

            # Get previous day's close
            if not self.historical_data.empty:
                self.previous_close = self.historical_data['close'].iloc[-1]
            else:
                self.previous_close = None

            return {
                "high_volatility": high_volatility,
                "trend": self.strategy_state.get(self.symbol, {}).get("trend", "neutral"),
                "previous_close": self.previous_close,
                "vix": self.vix
            }
        except Exception as e:
            self.logger.error(f"Error analyzing market conditions: {e}")
            return {
                "high_volatility": False,
                "trend": "neutral",
                "previous_close": None,
                "vix": None
            }

    def should_trade(self, market_conditions, latest_data):
        """
        Determine if a trade should be taken based on market conditions, option chain, and indicators.
        """

        if not self.is_market_open():
            self.logger.info("Market is closed, no trade")
            return False, None

        if len(self.historical_data) < 50:
            self.logger.info("Not enough historical data to trade")
            return False, None

        if self.open_positions_count >= self.max_open_positions:
            self.logger.info("Max open positions reached")
            return False, None

        option_chain = self.get_option_chain_data()
        if not option_chain:
            return False, None

        high_volatility = market_conditions["high_volatility"]
        trend = market_conditions["trend"]
        vix = market_conditions["vix"]
        previous_close = market_conditions["previous_close"]


        latest_price = latest_data['lp']
        df = self.historical_data.copy()
        latest_row = pd.DataFrame([{'timestamp': datetime.now(), 'close': latest_price, 'high': latest_price, 'low': latest_price, 'volume': latest_data.get('v',0)}])
        latest_row["timestamp"] = pd.to_datetime(latest_row["timestamp"])
        df = self.calculate_indicators(df)
        current_candle = df.iloc[-1]

        # Example strategy logic (Highly simplified, you'll need to refine this)
        signal = None
        order_details = None

        if high_volatility:
            self.logger.info(f"High volatility (VIX = {vix:.2f}), be cautious")

        # Mean Reversion with Bollinger Bands on Underlying with Option Overlay
        if current_candle['close'] < current_candle['bb_lower']:  # Mean Reversion on underlying
            if trend == "bullish":
                put_strike = self.atm_strike - 2 * 100  # Example: Buy OTM put
                if put_strike in option_chain['puts'] and option_chain['puts'][put_strike]['delta'] < self.delta_threshold:
                    signal = "buy_option"
                    order_details = {
                        "symbol": self.symbol, # Pass the symbol
                        "option_type": "PE",
                        "strike": put_strike,
                        "side": "buy",
                        "qty": self.calculate_position_size(latest_price),  # Use calculated position size
                        "price": option_chain['puts'][put_strike]['ltp'],
                        "delta": option_chain['puts'][put_strike]['delta'],
                        "gamma": option_chain['puts'][put_strike]['gamma'],
                        "theta": option_chain['puts'][put_strike]['theta'],
                        "vega": option_chain['puts'][put_strike]['vega'],
                    }
        elif current_candle['close'] > current_candle['bb_upper']:
            if trend == "bearish":
                call_strike = self.atm_strike + 2 * 100
                if call_strike in option_chain['calls'] and option_chain['calls'][call_strike]['delta'] < self.delta_threshold:
                    signal = "buy_option"
                    order_details = {
                        "symbol": self.symbol,
                        "option_type": "CE",
                        "strike": call_strike,
                        "side": "buy",
                        "qty": self.calculate_position_size(latest_price),
                        "price": option_chain['calls'][call_strike]['ltp'],
                        "delta": option_chain['calls'][call_strike]['delta'],
                        "gamma": option_chain['calls'][call_strike]['gamma'],
                        "theta": option_chain['calls'][call_strike]['theta'],
                        "vega": option_chain['calls'][call_strike]['vega'],
                    }

        # CPR and Stochastic RSI
        if current_candle['close'] > current_candle['cpr_top'] and current_candle['stoch_k'] > 80:
            signal = "sell"
            order_details = {
                "symbol": self.symbol,
                "side": "sell",
                "qty": self.calculate_position_size(latest_price),
            }
        elif current_candle['close'] < current_candle['cpr_bottom'] and current_candle['stoch_k'] < 20:
            signal = "buy"
            order_details = {
                "symbol": self.symbol,
                "side": "buy",
                "qty": self.calculate_position_size(latest_price),
            }

        if signal:
            self.logger.info(f"Trade signal: {signal}, {order_details}")
            return True, order_details
        return False, None

    def calculate_position_size(self, price):
        """
        Calculates position size based on account equity and risk tolerance.
        """
        if price <= 0:
            self.logger.error("Price must be positive in calculate_position_size")
            return 0  # Or raise an exception
        if self.position_sizing_method == "fixed":
            return self.position_size_fixed
        elif self.position_sizing_method == "percent":
            account_value = self.ts.account_value  # Get account value from trading session
            position_size = int(account_value * self.position_size_percent / price)
            return position_size
        else:
            self.logger.warning(f"Invalid position sizing method: {self.position_sizing_method}.  Defaulting to 1 lot")
            return 50 # 1 lot of NIFTY

    def place_order_with_risk_management(self, order_details: Dict) -> Optional[str]:
        """
        Places an order with risk management checks.
        """
        if self.use_option_greeks and 'delta' in order_details:
            # Check Greek exposure limits
            for greek in ['delta', 'gamma', 'theta', 'vega']:
                if greek in order_details:
                    if abs(self.current_greek_exposure[greek] + order_details[greek]) > self.greek_exposure_limit[greek]:
                        self.logger.warning(f"Order rejected: {greek.upper()} exposure limit exceeded.")
                        return None  # Do not place the order

        # Calculate slippage
        slippage = order_details['price'] * self.slippage_bps / 10000
        if order_details['side'] == 'buy':
            order_details['price'] += slippage
        else:
            order_details['price'] -= slippage

        # Account for commission
        order_details['price'] += self.commission_per_lot / order_details['qty']

        try: # added try catch
            order_id = self.place_order(
                symbol=order_details["symbol"],
                side=order_details["side"],
                qty=order_details["qty"],
                order_type=self.order_type,
                price=order_details['price']
            )
        except Exception as e:
            self.logger.error(f"Error placing order: {e}")
            return None

        if order_id:
            # Update Greek exposure
            if self.use_option_greeks and 'delta' in order_details:
                for greek in ['delta', 'gamma', 'theta', 'vega']:
                    self.current_greek_exposure[greek] += order_details[greek] * order_details['qty']
            self.open_positions_count += 1
            return order_id
        return None

    def manage_existing_positions(self):
        """
        Manage existing positions (e.g., trailing stop loss, profit taking).
        """
        if self.position:
            # Get the latest price
            latest_price = self.get_live_data()['lp']
            if latest_price is None:
                self.logger.warning("No live price to manage existing position")
                return

            if self.position == 'long':
                # Trailing stop loss
                new_stop_loss = latest_price * (1 - self.trailing_stop_loss_pct)
                if 'trailing_stop' not in self.strategy_state:
                    self.strategy_state['trailing_stop'] = new_stop_loss
                else:
                    self.strategy_state['trailing_stop'] = max(self.strategy_state['trailing_stop'], new_stop_loss)
                if latest_price <= self.strategy_state['trailing_stop']:
                    self.logger.info(f"Trailing stop hit. Closing long position for {self.symbol} at {latest_price:.2f}")
                    if self.close_position():
                        self.open_positions_count -= 1
                        del self.strategy_state['trailing_stop']

                # Profit taking
                if latest_price >= self.entry_price * self.profit_target_multiplier:
                    self.logger.info(f"Profit target reached. Closing long position for {self.symbol} at {latest_price:.2f}")
                    if self.close_position():
                         self.open_positions_count -= 1

            elif self.position == 'short':
                # Trailing stop loss
                new_stop_loss = latest_price * (1 + self.trailing_stop_loss_pct)
                if 'trailing_stop' not in self.strategy_state:
                    self.strategy_state['trailing_stop'] = new_stop_loss
                else:
                    self.strategy_state['trailing_stop'] = min(self.strategy_state['trailing_stop'], new_stop_loss)
                if latest_price >= self.strategy_state['trailing_stop']:
                    self.logger.info(f"Trailing stop hit. Closing short position for {self.symbol} at {latest_price:.2f}")
                    if self.close_position():
                        self.open_positions_count -= 1
                        del self.strategy_state['trailing_stop']
                # Profit taking
                if latest_price <= self.entry_price / self.profit_target_multiplier:
                    self.logger.info(f"Profit target reached. Closing short position for {self.symbol} at {latest_price:.2f}")
                    if self.close_position():
                        self.open_positions_count -= 1

    def on_tick(self, tick_data):
        """
        Process incoming market data and execute the option trading strategy.
        """
        try:
            # 1. Update Live Data
            self.live_data[self.symbol] = tick_data

            # 2. Analyze Market Conditions
            market_conditions = self.analyze_market_conditions()

            # 3. Get Trading Signal
            should_trade, order_details = self.should_trade(market_conditions, tick_data)

            # 4. Execute Trade if Signal is generated
            if should_trade and order_details:
                order_id = self.place_order_with_risk_management(order_details)
                if order_id:
                    self.logger.info(f"Order placed: {order_details}")
                    self.entry_price = order_details["price"]
                    if order_details["option_type"]:
                        self.position = order_details["side"]
                    else:
                        self.position = order_details["side"]
                else:
                    self.logger.warning("Order was not placed due to risk management rules.")

            # 5. Manage Existing Positions
            self.manage_existing_positions()

        except Exception as e:
            self.logger.error(f"Error in on_tick: {e}")
