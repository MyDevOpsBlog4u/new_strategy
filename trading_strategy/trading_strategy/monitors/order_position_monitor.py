import os
import json
import time
import logging
import pickle
import signal
import queue
import sys
import configparser
from datetime import datetime, timedelta
from threading import Thread, Lock, Event
from urllib.parse import parse_qs, urlparse
from logging.handlers import RotatingFileHandler
import pandas as pd
import ta
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
from tabulate import tabulate
from dotenv import load_dotenv

class OrderPositionMonitor:
    """Monitors orders, positions, and generates reports."""

    def __init__(self, trading_session):
        self.logger = logging.getLogger('trading_strategy.monitor')
        self.ts = trading_session
        self.orders = {}
        self.active_orders = {}
        self.positions = {}
        self.position_history = {}
        self.pnl_history = []
        self.lock = Lock()
        self.running = False
        self.order_poll_interval = 5
        self.position_poll_interval = 15
        self.health_poll_interval = 30
        self.trade_metrics = {
            'win_count': 0,
            'loss_count': 0,
            'total_trades': 0,
            'profitable_trades': 0,
            'unprofitable_trades': 0,
            'max_profit': 0,
            'max_loss': 0,
            'avg_profit': 0,
            'avg_loss': 0,
            'win_rate': 0,
            'profit_factor': 0,
            'avg_trade_duration': 0,
        }
        self.alerts = []
        self.alert_levels = {
            'INFO': 0,
            'WARNING': 1,
            'ERROR': 2,
            'CRITICAL': 3
        }
        self.min_alert_level = self.alert_levels['WARNING']
        self.alert_callbacks = []
        self.status = {
            'api_connected': False,
            'websocket_connected': False,
            'strategy_running': False,
            'last_order_check': None,
            'last_position_check': None,
            'last_health_check': None,
            'errors': []
        }
        self.order_updates = Event()
        self.position_updates = Event()

    def start(self):
        """Start the order and position monitoring threads."""

        if self.running:
            return
        self.running = True
        self.logger.info("Starting Order and Position Monitor")

        self.order_thread = Thread(target=self._monitor_orders, daemon=True)
        self.order_thread.start()

        self.position_thread = Thread(target=self._monitor_positions, daemon=True)
        self.position_thread.start()

        self.health_thread = Thread(target=self._monitor_health, daemon=True)
        self.health_thread.start()

        self.report_thread = Thread(target=self._generate_periodic_reports, daemon=True)
        self.report_thread.start()

    def stop(self):
        """Stop the order and position monitoring threads."""

        if not self.running:
            return
        self.running = False
        self.logger.info("Stopping Order and Position Monitor")
        self._save_monitor_state()

    def add_alert_callback(self, callback):
        """Add a callback function to be triggered when an alert is raised."""

        if callable(callback):
            self.alert_callbacks.append(callback)

    def set_min_alert_level(self, level):
        """Set the minimum alert level to be processed."""

        if level in self.alert_levels:
            self.min_alert_level = self.alert_levels[level]

    def _trigger_alert(self, level, message):
        """Trigger an alert and log it."""

        if level not in self.alert_levels:
            level = 'INFO'
        alert = {
            'level': level,
            'message': message,
            'timestamp': datetime.now()
        }
        with self.lock:
            self.alerts.append(alert)
            if len(self.alerts) > 100:
                self.alerts = self.alerts[-100:]
        self.logger.log(
            getattr(logging, level),
            f"ALERT: {message}"
        )
        if self.alert_levels[level] >= self.min_alert_level:
            for callback in self.alert_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    self.logger.error(f"Error in alert callback: {e}")

    def _monitor_orders(self):
        """Monitor order status."""

        while self.running:
            try:
                if not self.ts.is_market_open() and not self.ts.positions:
                    time.sleep(self.order_poll_interval * 4)
                    continue

                response = self.ts.fyers.orderbook()
                if response.get('s') != "ok":
                    self.logger.error(f"Failed to fetch orderbook: {response.get('message', 'Unknown error')}")
                    time.sleep(self.order_poll_interval)
                    continue

                orders = response.get('orderBook', [])
                new_orders = {}
                active_orders = {}

                for order in orders:
                    order_id = order.get('id')
                    if not order_id:
                        continue
                    status_map = {
                        1: "Pending", 2: "Filled", 3: "Cancelled",
                        4: "Rejected", 5: "FutureCancelled", 6: "Expired"
                    }
                    status_code = order.get('status')
                    status = status_map.get(status_code, f"Unknown({status_code})")
                    symbol = order.get('symbol', '')
                    side = "Buy" if order.get('side') == 1 else "Sell"
                    qty = order.get('qty', 0)
                    filled_qty = order.get('filledQty', 0)
                    avg_price = order.get('tradedPrice', 0)
                    order_type = order.get('type', 0)
                    order_type_map = {1: "Limit", 2: "Market", 3: "SL-M", 4: "SL"}
                    order_type_str = order_type_map.get(order_type, f"Unknown({order_type})")

                    order_record = {
                        'id': order_id,
                        'symbol': symbol,
                        'side': side,
                        'qty': qty,
                        'filled_qty': filled_qty,
                        'avg_price': avg_price,
                        'status': status,
                        'status_code': status_code,
                        'order_type': order_type_str,
                        'timestamp': datetime.now(),
                        'message': order.get('message', ''),
                        'placed_at': order.get('orderDateTime', ''),
                    }
                    new_orders[order_id] = order_record
                    if status_code in [1]:
                        active_orders[order_id] = order_record

                    with self.lock:
                        if order_id in self.orders:
                            old_status = self.orders[order_id]['status_code']
                            if old_status != status_code:
                                self.logger.info(
                                    f"Order {order_id} status changed: {status_map.get(old_status)} -> {status}"
                                )
                                if status_code == 2 and old_status == 1:
                                    self._trigger_alert(
                                        'INFO',
                                        f"Order {order_id} filled: {symbol} {side} {filled_qty} @ ₹{avg_price}"
                                    )
                                elif status_code in [3, 4, 5, 6] and old_status in [1]:
                                    self._trigger_alert(
                                        'WARNING',
                                        f"Order {order_id} {status}: {symbol} {side} - {order.get('message', 'No reason')}"
                                    )
                        else:
                            self.logger.info(f"New order detected: {order_id} - {symbol} {side} {qty}")
                            if status_code == 2:
                                self._trigger_alert(
                                    'INFO',
                                    f"Order {order_id} filled: {symbol} {side} {filled_qty} @ ₹{avg_price}"
                                )

                with self.lock:
                    self.orders = new_orders
                    self.active_orders = active_orders
                    self.status['last_order_check'] = datetime.now()
                self.order_updates.set()
                self.order_updates.clear()

            except Exception as e:
                self.logger.error(f"Error in order monitoring: {e}")
                with self.lock:
                    self.status['errors'].append({
                        'timestamp': datetime.now(),
                        'source': 'order_monitor',
                        'error': str(e),
                        'traceback': traceback.format_exc()
                    })
            time.sleep(self.order_poll_interval)

    def _monitor_positions(self):
        """Monitor position status."""

        while self.running:
            try:
                response = self.ts.fyers.positions()
                if response.get('s') != "ok":
                    self.logger.error(f"Failed to fetch positions: {response.get('message', 'Unknown error')}")
                    time.sleep(self.position_poll_interval)
                    continue

                net_positions = response.get('netPositions', [])
                day_positions = response.get('dayWisePositions', [])
                new_positions = {}
                realized_pnl = 0
                unrealized_pnl = 0

                for pos in net_positions:
                    symbol = pos.get('symbol')
                    if not symbol:
                        continue
                    if symbol not in self.ts.SYMBOLS and not any(
                        symbol.startswith(f"NSE:OPTIDX{code}") for code in ["N", "B"]
                    ):
                        continue
                    side = "Buy" if pos.get('side') == 1 else "Sell"
                    qty = abs(pos.get('qty', 0))
                    if qty == 0:
                        continue
                    entry_price = pos.get('avgPrice', 0)
                    current_price = self.ts.live_data.get(symbol, {}).get("lp", entry_price)
                    if current_price == entry_price:
                        if side == "Buy":
                            current_price = pos.get('sellAvg', entry_price) or entry_price
                        else:
                            current_price = pos.get('buyAvg', entry_price) or entry_price
                    price_diff = (current_price - entry_price) * (1 if side == "Buy" else -1)
                    pos_pnl = price_diff * qty
                    pos_pnl_pct = (price_diff / entry_price) * 100 if entry_price > 0 else 0
                    cost = 0
                    with self.lock:
                        if symbol in self.positions:
                            cost = self.positions[symbol].get('cost', 0)
                        elif symbol in self.ts.positions:
                            cost = self.ts.positions[symbol].get('cost', 0)
                        else:
                            cost = self.ts.calculate_transaction_cost(entry_price, qty)

                    position_record = {
                        'symbol': symbol,
                        'side': side,
                        'qty': qty,
                        'entry_price': entry_price,
                        'current_price': current_price,
                        'unrealized_pnl': pos_pnl,
                        'unrealized_pnl_pct': pos_pnl_pct,
                        'entry_time': pos.get('openTime', datetime.now().isoformat()),
                        'cost': cost,
                        'net_pnl': pos_pnl - cost,
                        'last_updated': datetime.now(),
                    }
                    new_positions[symbol] = position_record
                    unrealized_pnl += pos_pnl - cost

                for pos in day_positions:
                    if pos.get('side') == 0:
                        realized_pnl += pos.get('pl', 0)

                with self.lock:
                    for symbol, position in new_positions.items():
                        if symbol not in self.positions:
                            self.logger.info(f"New position detected: {symbol} {position['side']} {position['qty']} @ ₹{position['entry_price']}")
                            self._trigger_alert(
                                'INFO',
                                f"New position: {symbol} {position['side']} {position['qty']} @ ₹{position['entry_price']}"
                            )

                    for symbol, position in self.positions.items():
                        if symbol not in new_positions:
                            close_pnl = position.get('net_pnl', 0)
                            self.logger.info(f"Position closed: {symbol} P&L: ₹{close_pnl:.2f}")
                            if symbol not in self.position_history:
                                self.position_history[symbol] = []
                            position['close_time'] = datetime.now()
                            position['status'] = 'CLOSED'
                            self.position_history[symbol].append(position.copy())
                            self._update_trade_metrics(position)
                            self.pnl_history.append({
                                'symbol': symbol,
                                'side': position['side'],
                                'entry_price': position['entry_price'],
                                'exit_price': position['current_price'],
                                'qty': position['qty'],
                                'entry_time': position['entry_time'],
                                'exit_time': datetime.now().isoformat(),
                                'pnl': close_pnl,
                                'pnl_pct': position['unrealized_pnl_pct'],
                            })
                            level = 'INFO' if close_pnl >= 0 else 'WARNING'
                            self._trigger_alert(
                                level,
                                f"Position closed: {symbol} {position['side']} {position['qty']} P&L: ₹{close_pnl:.2f} ({position['unrealized_pnl_pct']:.2f}%)"
                            )

                    self.positions = new_positions
                    self.status['last_position_check'] = datetime.now()
                    self.ts.daily_pnl = realized_pnl + unrealized_pnl
                self.position_updates.set()
                self.position_updates.clear()

            except Exception as e:
                self.logger.error(f"Error in position monitoring: {e}")
                with self.lock:
                    self.status['errors'].append({
                        'timestamp': datetime.now(),
                        'source': 'position_monitor',
                        'error': str(e),
                        'traceback': traceback.format_exc()
                    })
            time.sleep(self.position_poll_interval)

    def _update_trade_metrics(self, closed_position):
        """Update trade metrics based on closed position."""

        pnl = closed_position.get('net_pnl', 0)
        self.trade_metrics['total_trades'] += 1
        if pnl >= 0:
            self.trade_metrics['win_count'] += 1
            self.trade_metrics['profitable_trades'] += 1
            self.trade_metrics['max_profit'] = max(self.trade_metrics['max_profit'], pnl)
            self.trade_metrics['avg_profit'] = (
                (self.trade_metrics['avg_profit'] * (self.trade_metrics['win_count'] - 1) + pnl) /
                self.trade_metrics['win_count'] if self.trade_metrics['win_count'] > 0 else 0
            )
        else:
            self.trade_metrics['loss_count'] += 1
            self.trade_metrics['unprofitable_trades'] += 1
            self.trade_metrics['max_loss'] = min(self.trade_metrics['max_loss'], pnl)
            self.trade_metrics['avg_loss'] = (
                (self.trade_metrics['avg_loss'] * (self.trade_metrics['loss_count'] - 1) + pnl) /
                self.trade_metrics['loss_count'] if self.trade_metrics['loss_count'] > 0 else 0
            )
        if self.trade_metrics['total_trades'] > 0:
            self.trade_metrics['win_rate'] = (self.trade_metrics['win_count'] / self.trade_metrics['total_trades']) * 100
        if self.trade_metrics['loss_count'] > 0 and self.trade_metrics['avg_loss'] < 0:
            profit_factor = (self.trade_metrics['win_count'] * self.trade_metrics['avg_profit']) / (
                self.trade_metrics['loss_count'] * abs(self.trade_metrics['avg_loss'])
            )
            self.trade_metrics['profit_factor'] = profit_factor
        entry_time = datetime.fromisoformat(closed_position['entry_time'].replace('Z', '+00:00'))
        close_time = closed_position['close_time']
        duration = (close_time - entry_time).total_seconds() / 60
        if self.trade_metrics['total_trades'] == 1:
            self.trade_metrics['avg_trade_duration'] = duration
        else:
            self.trade_metrics['avg_trade_duration'] = (
                (self.trade_metrics['avg_trade_duration'] * (self.trade_metrics['total_trades'] - 1) + duration) /
                self.trade_metrics['total_trades']
            )

    def _monitor_health(self):
        """Monitor system health."""

        while self.running:
            try:
                api_connected = False
                try:
                    profile = self.ts.fyers.get_profile()
                    api_connected = profile.get('s') == 'ok'
                except Exception:
                    api_connected = False

                ws_connected = False
                try:
                    last_message_age = time.time() - self.ts.last_message_time
                    ws_connected = last_message_age < self.ts.connection_timeout
                except Exception:
                    ws_connected = False

                with self.lock:
                    prev_api = self.status['api_connected']
                    prev_ws = self.status['websocket_connected']
                    self.status['api_connected'] = api_connected
                    self.status['websocket_connected'] = ws_connected
                    self.status['strategy_running'] = self.ts.running
                    self.status['last_health_check'] = datetime.now()
                    if len(self.status['errors']) > 50:
                        self.status['errors'] = self.status['errors'][-50:]

                if prev_api and not api_connected:
                    self._trigger_alert('ERROR', 'API connection lost')
                elif not prev_api and api_connected:
                    self._trigger_alert('INFO', 'API connection restored')

                if prev_ws and not ws_connected:
                    self._trigger_alert('WARNING', 'WebSocket connection lost')
                elif not prev_ws and ws_connected:
                    self._trigger_alert('INFO', 'WebSocket connection restored')

                for symbol, position in self.positions.items():
                    pnl_pct = position.get('unrealized_pnl_pct', 0)
                    if pnl_pct < -5:
                        self._trigger_alert(
                            'WARNING',
                            f"Position {symbol} down {pnl_pct:.2f}% (₹{position['unrealized_pnl']:.2f})"
                        )

                if self.ts.daily_pnl < self.ts.config['MAX_DAILY_LOSS'] * 0.8:
                    self._trigger_alert(
                        'WARNING',
                        f"Approaching daily loss limit: ₹{self.ts.daily_pnl:.2f} (limit: ₹{self.ts.config['MAX_DAILY_LOSS']:.2f})"
                    )

            except Exception as e:
                self.logger.error(f"Error in health monitoring: {e}")
            time.sleep(self.health_poll_interval)

    def _save_monitor_state(self):
        """Save the monitor's state to a file."""

        try:
            state = {
                'positions': self.positions,
                'position_history': self.position_history,
                'pnl_history': self.pnl_history,
                'trade_metrics': self.trade_metrics,
                'timestamp': datetime.now().isoformat()
            }
            filename = os.path.join(self.ts.BASE_DIR, 'monitor_state.json')
            with open(filename, 'w') as f:
                json.dump(state, f, indent=2)
            self.logger.info("Monitor state saved successfully")
        except Exception as e:
            self.logger.error(f"Failed to save monitor state: {e}")

    def _generate_periodic_reports(self):
        """Generate periodic trading reports."""

        last_daily_report = datetime.now().date() - timedelta(days=1)
        while self.running:
            now = datetime.now()
            if (now.date() > last_daily_report and
                    now.time() > datetime.strptime(self.ts.config['SQUARE_OFF_TIME'], "%H:%M:%S").time()):
                try:
                    self._generate_daily_report()
                    last_daily_report = now.date()
                except Exception as e:
                    self.logger.error(f"Error generating daily report: {e}")

            if now.minute % 30 == 0 and now.second < 10:
                self._save_monitor_state()
            time.sleep(60)

    def _generate_daily_report(self):
        """Generate a daily trading report."""

        today = datetime.now().date()
        report_path = os.path.join(REPORT_DIR, f"trading_report_{today.strftime('%Y%m%d')}.txt")
        realized_pnl = sum(trade['pnl'] for trade in self.pnl_history if
                                        datetime.fromisoformat(trade['exit_time'].replace('Z', '+00:00')).date() == today)
        unrealized_pnl = sum(pos['unrealized_pnl'] for pos in self.positions.values())
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"Trading Report for {today.strftime('%Y-%m-%d')}\n")
            f.write("=" * 50 + "\n\n")
            f.write("Performance Summary:\n")
            f.write("-" * 50 + "\n")
            f.write(f"Realized P&L: ₹{realized_pnl:.2f}\n")
            f.write(f"Unrealized P&L: ₹{unrealized_pnl:.2f}\n")
            f.write(f"Total P&L: ₹{(realized_pnl + unrealized_pnl):.2f}\n")
            f.write(f"Day's Trades: {sum(1 for trade in self.pnl_history if datetime.fromisoformat(trade['exit_time'].replace('Z', '+00:00')).date() == today)}\n\n")
            f.write("Trade Metrics:\n")
            f.write("-" * 50 + "\n")
            f.write(f"Win Rate: {self.trade_metrics['win_rate']:.2f}%\n")
            f.write(f"Profit Factor: {self.trade_metrics['profit_factor']:.2f}\n")
            f.write(f"Average Winning Trade: ₹{self.trade_metrics['avg_profit']:.2f}\n")
            f.write(f"Average Losing Trade: ₹{self.trade_metrics['avg_loss']:.2f}\n")
            f.write(f"Largest Win: ₹{self.trade_metrics['max_profit']:.2f}\n")
            f.write(f"Largest Loss: ₹{self.trade_metrics['max_loss']:.2f}\n")
            f.write(f"Average Trade Duration: {self.trade_metrics['avg_trade_duration']:.2f} minutes\n\n")

            today_trades = [trade for trade in self.pnl_history if
                            datetime.fromisoformat(trade['exit_time'].replace('Z', '+00:00')).date() == today]
            if today_trades:
                f.write("Today's Completed Trades:\n")
                f.write("-" * 50 + "\n")
                table_data = []
                for trade in today_trades:
                    entry_time = datetime.fromisoformat(trade['entry_time'].replace('Z', '+00:00'))
                    exit_time = datetime.fromisoformat(trade['exit_time'].replace('Z', '+00:00'))
                    duration = (exit_time - entry_time).total_seconds() / 60
                    table_data.append([
                        trade['symbol'],
                        trade['side'],
                        trade['qty'],
                        f"₹{trade['entry_price']:.2f}",
                        f"₹{trade['exit_price']:.2f}",
                        f"{entry_time.strftime('%H:%M:%S')}",
                        f"{exit_time.strftime('%H:%M:%S')}",
                        f"{duration:.1f}m",
                        f"₹{trade['pnl']:.2f}",
                        f"{trade['pnl_pct']:.2f}%"
                    ])
                f.write(tabulate(
                    table_data,
                    headers=["Symbol", "Side", "Qty", "Entry", "Exit", "Entry Time", "Exit Time", "Duration", "P&L", "P&L %"],
                    tablefmt="simple"
                ))
                f.write("\n\n")

            if self.positions:
                f.write("Current Open Positions:\n")
                f.write("-" * 50 + "\n")
                table_data = []
                for symbol, pos in self.positions.items():
                    entry_time = datetime.fromisoformat(pos['entry_time'].replace('Z', '+00:00'))
                    now = datetime.now()
                    duration = (now - entry_time).total_seconds() / 60
                    table_data.append([
                        symbol,
                        pos['side'],
                        pos['qty'],
                        f"₹{pos['entry_price']:.2f}",
                        f"₹{pos['current_price']:.2f}",
                        f"{entry_time.strftime('%H:%M:%S')}",
                        f"{duration:.1f}m",
                        f"₹{pos['unrealized_pnl']:.2f}",
                        f"{pos['unrealized_pnl_pct']:.2f}%"
                    ])
                f.write(tabulate(
                    table_data,
                    headers=["Symbol", "Side", "Qty", "Entry", "Current", "Entry Time", "Duration", "Unrl P&L", "P&L %"],
                    tablefmt="simple"
                ))
                f.write("\n\n")

            f.write("System Status:\n")
            f.write("-" * 50 + "\n")
            f.write(f"API Connected: {self.status['api_connected']}\n")
            f.write(f"WebSocket Connected: {self.status['websocket_connected']}\n")
            f.write(f"Strategy Running: {self.status['strategy_running']}\n")
            f.write(f"Last Order Check: {self.status['last_order_check']}\n")
            f.write(f"Last Position Check: {self.status['last_position_check']}\n")
            f.write(f"Last Health Check: {self.status['last_health_check']}\n")
            f.write(f"Error Count: {len(self.status['errors'])}\n")
            recent_errors = sorted(self.status['errors'], key=lambda x: x['timestamp'], reverse=True)[:10]
            if recent_errors:
                f.write("\nRecent Errors:\n")
                f.write("-" * 50 + "\n")
                for error in recent_errors:
                    f.write(f"{error['timestamp']} - {error['source']}: {error['error']}\n")
        self.logger.info("Daily trading report generated: %s", report_path)

    def get_position_summary(self):
        """Get a summary of current positions."""

        with self.lock:
            total_unrealized_pnl = sum(pos['unrealized_pnl'] for pos in self.positions.values())
            total_realized_pnl = sum(trade['pnl'] for trade in self.pnl_history if
                                        datetime.fromisoformat(trade['exit_time'].replace('Z', '+00:00')).date() == datetime.now().date())
            summary = {
                'position_count': len(self.positions),
                'unrealized_pnl': total_unrealized_pnl,
                'realized_pnl': total_realized_pnl,
                'total_pnl': total_unrealized_pnl + total_realized_pnl,
                'positions': self.positions,
                'trade_metrics': self.trade_metrics,
            }
            return summary

    def get_status(self):
        """Get the current system status."""

        with self.lock:
            return self.status.copy()

    def get_alerts(self, level=None):
        """Get alerts, optionally filtered by level."""

        with self.lock:
            if level is None:
                return self.alerts.copy()
            level_value = self.alert_levels.get(level, 0)
            return [alert for alert in self.alerts if self.alert_levels.get(alert['level'], 0) >= level_value]

    def get_trade_metrics(self):
        """Get the current trade metrics."""

        with self.lock:
            return self.trade_metrics.copy()

    def wait_for_order_update(self, timeout=None):
        """Wait for an order update event."""
        return self.order_updates.wait(timeout)

    def wait_for_position_update(self, timeout=None):
        """Wait for a position update event."""
        return self.position_updates.wait(timeout)

    def get_position(self, symbol):
        """Get a specific position by symbol."""

        with self.lock:
            return self.positions.get(symbol)

    def get_order(self, order_id):
        """Get a specific order by order ID."""

        with self.lock:
            return self.orders.get(order_id)

    def get_active_orders(self):
        """Get all active orders."""

        with self.lock:
            return self.active_orders.copy()

    def generate_snapshot_report(self):
        """Generate a snapshot report of the current trading state."""

        now = datetime.now()
        today_realized_pnl = sum(trade['pnl'] for trade in self.pnl_history if
                                    datetime.fromisoformat(trade['exit_time'].replace('Z', '+00:00')).date() == now.date())
        unrealized_pnl = sum(pos['unrealized_pnl'] for pos in self.positions.values())
        total_pnl = today_realized_pnl + unrealized_pnl
        report = []
        report.append(f"Trading Snapshot - {now.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 50)
        report.append("")
        report.append("Performance:")
        report.append(f"Today's P&L: ₹{total_pnl:.2f} (Realized: ₹{today_realized_pnl:.2f}, Unrealized: ₹{unrealized_pnl:.2f})")
        report.append(f"Win Rate: {self.trade_metrics['win_rate']:.2f}%")
        report.append(f"Profit Factor: {self.trade_metrics['profit_factor']:.2f}")
        report.append("")

        if self.positions:
            report.append("Open Positions:")
            position_data = []
            for symbol, pos in sorted(self.positions.items(), key=lambda x: x[1]['unrealized_pnl'], reverse=True):
                position_data.append([
                    symbol,
                    pos['side'],
                    pos['qty'],
                    f"₹{pos['entry_price']:.2f}",
                    f"₹{pos['current_price']:.2f}",
                    f"₹{pos['unrealized_pnl']:.2f}",
                    f"{pos['unrealized_pnl_pct']:.2f}%"
                ])
            report.append(tabulate(
                position_data,
                headers=["Symbol", "Side", "Qty", "Entry", "Current", "P&L", "P&L %"],
                tablefmt="simple"
            ))
            report.append("")

        if self.active_orders:
            report.append("Active Orders:")
            order_data = []
            for order_id, order in self.active_orders.items():
                order_data.append([
                    order_id,
                    order['symbol'],
                    order['side'],
                    order['qty'],
                    order['order_type'],
                    order['status']
                ])
            report.append(tabulate(
                order_data,
                headers=["Order ID", "Symbol", "Side", "Qty", "Type", "Status"],
                tablefmt="simple"
            ))
            report.append("")

        today_trades = [trade for trade in self.pnl_history if
                        datetime.fromisoformat(trade['exit_time'].replace('Z', '+00:00')).date() == now.date()]
        if today_trades:
            report.append("Today's Completed Trades:")
            trade_data = []
            for trade in sorted(today_trades, key=lambda x: x['exit_time'], reverse=True):
                trade_data.append([
                    trade['symbol'],
                    trade['side'],
                    trade['qty'],
                    f"₹{trade['entry_price']:.2f}",
                    f"₹{trade['exit_price']:.2f}",
                    f"₹{trade['pnl']:.2f}",
                    f"{trade['pnl_pct']:.2f}%"
                ])
            report.append(tabulate(
                trade_data,
                headers=["Symbol", "Side", "Qty", "Entry", "Exit", "P&L", "P&L %"],
                tablefmt="simple"
            ))
            report.append("")

        report.append("System Status:")
        report.append(f"API: {'✓' if self.status['api_connected'] else '✗'} | " +
                      f"WebSocket: {'✓' if self.status['websocket_connected'] else '✗'} | " +
                      f"Strategy: {'Running' if self.status['strategy_running'] else 'Stopped'}")
        recent_alerts = sorted(self.alerts, key=lambda x: x['timestamp'], reverse=True)[:5]
        if recent_alerts:
            report.append("\nRecent Alerts:")
            for alert in recent_alerts:
                report.append(f"{alert['timestamp'].strftime('%H:%M:%S')} [{alert['level']}] {alert['message']}")
        return "\n".join(report)

    def load_monitor_state(self):
        """Load the monitor's state from a file."""

        try:
            filename = os.path.join(self.ts.BASE_DIR, 'monitor_state.json')
            if not os.path.exists(filename):
                self.logger.info("No saved monitor state found")
                return False
            with open(filename, 'r') as f:
                state = json.load(f)
            for symbol, position in state.get('positions', {}).items():
                if 'last_updated' in position:
                    position['last_updated'] = datetime.fromisoformat(position['last_updated'])
            for symbol, history in state.get('position_history', {}).items():
                for pos in history:
                    if 'close_time' in pos:
                        pos['close_time'] = datetime.fromisoformat(pos['close_time'])
                    if 'last_updated' in pos:
                        pos['last_updated'] = datetime.fromisoformat(pos['last_updated'])
            with self.lock:
                self.positions = state.get('positions', {})
                self.position_history = state.get('position_history', {})
                self.pnl_history = state.get('pnl_history', [])
                self.trade_metrics = state.get('trade_metrics', self.trade_metrics)
            self.logger.info("Monitor state loaded successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to load monitor state: {e}")
            return False

    def export_pnl_history(self, filename=None):
        """Export P&L history to a CSV file."""

        if filename is None:
            filename = os.path.join(REPORT_DIR, f"pnl_history_{datetime.now().strftime('%Y%m%d')}.csv")
        try:
            df = pd.DataFrame(self.pnl_history)
            if not df.empty:
                df.to_csv(filename, index=False)
                self.logger.info(f"P&L history exported to {filename}")
                return True
            else:
                self.logger.warning("No P&L history to export")
                return False
        except Exception as e:
            self.logger.error(f"Failed to export P&L history: {e}")
            return False

# --- Trading Session ---
class TradingSession:
    """Manages the overall trading session, including authentication, data handling, and strategy execution."""

    def __init__(self):
        self.logger = logging.getLogger('trading_strategy')
        self.config = load_config()
        self.client_id = self.config['CLIENT_ID']
        self.secret_key = self.config['SECRET_KEY']  # Add this line
        self.redirect_uri = self.config['REDIRECT_URI']
        self.fyers = FyersModelWrapper(self.client_id, None)  # Initialize with None, will update later
        self.access_token = None
        self.ws = None
        self.running = False
        self.positions = {}
        self.live_data = {}
        self.historical_data = {sym: pd.DataFrame() for sym in SYMBOLS}
        self.option_chain_cache = {}
        self.order_id_to_symbol = {}
        self.strategy_state = {}
        self.account_value = 100000
        self.initial_account_value = 100000
        self.daily_pnl = 0
        self.trade_count = 0
        self.lock = Lock()
        self.tick_queue = queue.Queue()
        self.last_message_time = time.time()
        self.last_chain_update = datetime.now() - timedelta(hours=1)
        self.history_loaded = False
        self.connection_timeout = 30
        self.monitor = None
        self.BASE_DIR = BASE_DIR
        self.SYMBOLS = SYMBOLS

    def _load_token(self):
        """Load the access token from file."""
        try:
            if os.path.exists(TOKEN_PATH):
                with open(TOKEN_PATH, 'rb') as file:
                    token_dict = pickle.load(file)
                if self._is_token_valid(token_dict):
                    self.logger.info("Loaded valid access token")
                    return token_dict['access_token']
            except Exception as e:
                self.logger.error(f"Error loading token: {e}")
        return None

    def _is_token_valid(self, token_dict=None):
        """Check if the access token is still valid."""
        if token_dict and 'generated_at' in token_dict:
            generated_at = token_dict['generated_at']
            now = datetime.now()
            if (now - generated_at).total_seconds() >= 86400:
                return False
        try:
            temp_fyers = fyersModel.FyersModel(
                client_id=self.client_id,
                is_async=False,
                token=self.access_token if self.access_token else token_dict.get('access_token'),
                log_path=LOG_DIR
            )
            response = temp_fyers.get_profile()
            return response.get('s') == 'ok'
        except Exception as e:
            self.logger.error(f"Token validation failed: {e}")
            return False

    def _authenticate(self):
        """Authenticate with Fyers and obtain an access token."""
        self.logger.info("Starting authentication process")
        try:
            session = fyersModel.SessionModel(
                client_id=self.client_id,
                secret_key=self.secret_key,
                redirect_uri=self.redirect_uri,
                response_type="code",
                grant_type="authorization_code"
            )
            auth_link = session.generate_authcode()
            webbrowser.open(auth_link, new=2)
            auth_code = input("Enter the auth code from the redirect URL: ")
            session.set_token(auth_code)
            response = session.generate_token()
            if response['s'] != 'ok':
                raise Exception(f"Error generating token: {response['message']}")
            token_dict = {
                'access_token': response['access_token'],
                'generated_at': datetime.now()
            }
            with open(os.path.join(BASE_DIR, 'token_dict.pickle'), 'wb') as file:
                pickle.dump(token_dict, file)
            return response['access_token']
        except Exception as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            raise

    def generate_access_token(self):
        """Generate a new access token (using the corrected function)."""
        return generate_access_token(self.client_id, self.secret_key, self.redirect_uri)

    def refresh_token(self):
        """Refresh the access token."""
        self.logger.info("Refreshing access token")
        new_token = self.generate_access_token()
        if new_token:
            self.access_token = new_token
            self.fyers = FyersModelWrapper(self.client_id, new_token)
            if self.ws:
                try:
                    self.ws.close()
                except Exception:
                    pass
                self.init_websocket()
            self.logger.info("Token refreshed successfully")
            return True
        return False

    def start_token_refresh_scheduler(self):
        """Start a scheduler to refresh the token periodically."""
        def scheduler():
            while self.running:
                time.sleep(23 * 3600)  # Refresh every 23 hours
                if not self.running:
                    return
                self.logger.info("Scheduled token refresh triggered")
                self.refresh_token()
        Thread(target=scheduler, daemon=True).start()
        self.logger.info("Token refresh scheduler started")

    def init_websocket(self):
        """Initialize the WebSocket connection."""
        try:
            self.ws = FyersWebSocket(self.client_id, self.access_token)
            Thread(target=self.ws.connect, daemon=True).start()
            return True
        except Exception as e:
            self.logger.error(f"Error initializing WebSocket: {e}")
            return False

    def monitor_websocket(self):
        """Monitor the WebSocket connection and reconnect if necessary."""
        while self.running:
            try:
                if not self.is_market_open():
                    self.logger.info("Market closed, skipping WebSocket check")
                    time.sleep(60)
                    continue
                current_time = time.time()
                if current_time - self.last_message_time > self.connection_timeout:
                    self.logger.warning("WebSocket timeout, reconnecting...")
                    self.init_websocket()
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Error monitoring WebSocket: {e}")
                time.sleep(5)

    def get_profile(self):
        """Get the user's profile from Fyers API."""
        try:
            response = self.fyers.get_profile()
            return response
        except Exception as e:
            self.logger.error(f"Error getting profile: {str(e)}")
            return None

    def get_market_data(self, symbols):
        """Get market data for the given symbols."""
        try:
            data = {"symbols": ",".join(symbols)}
            response = self.fyers.quotes(data)
            return response
        except Exception as e:
            self.logger.error(f"Error getting market data: {str(e)}")
            return None

    def get_historical_data(self, symbol, timeframe='1D', days=30):
        """Get historical data for a symbol."""

        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            data = {
                "symbol": symbol,
                "resolution": timeframe,
                "date_format": "1",
                "range_from": start_date.strftime("%Y-%m-%d"),
                "range_to": end_date.strftime("%Y-%m-%d"),
                "cont_flag": "1"
            }
            response = self.fyers.history(data)
            if response.get('s') != "ok":
                raise Exception(f"Failed to get historical data: {response.get('message', 'Unknown error')}")
            return pd.DataFrame(response['candles'],
                                 columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        except Exception as e:
            self.logger.error(f"Error getting historical data: {str(e)}")
            raise

    def place_order(self, symbol, side, qty, order_type="MARKET", price=0, retries=3):
        """Place an order."""
        try:
            if not self.is_market_open():
                self.logger.warning(f"Market closed, skipping order: {symbol}, {side}, {qty}")
                return None
            if not all([symbol, side, qty]):
                self.logger.error("Missing required order parameters")
                return None
            order_type_map = {"MARKET": 2, "LIMIT": 1, "SL": 4, "SL-M": 3}
            side_value = 1 if side.lower() == "buy" else -1
            order_data = {
                "symbol": symbol,
                "qty": qty,
                "type": order_type_map.get(order_type, 2),
                "side": side_value,
                "productType": "INTRADAY",
                "validity": "DAY",
                "offlineOrder": "False",
                "limitPrice": 0,
                "stopPrice": 0,
                "disclosedQty": 0
            }
            if order_type == "LIMIT":
                current_price = self.live_data.get(symbol, {}).get("lp", price)
                if side.lower() == "buy":
                    limit_price = current_price * 1.002
                else:
                    limit_price = current_price * 0.998
                order_data["limitPrice"] = round(limit_price, 1)
            if order_type in ["SL", "SL-M"]:
                current_price = self.live_data.get(symbol, {}).get("lp", price)
                if side.lower() == "buy":
                    stop_price = current_price * 0.995
                else:
                    stop_price = current_price * 1.005
                order_data["stopPrice"] = round(stop_price, 1)
                if order_type == "SL":
                    if side.lower() == "buy":
                        limit_price = stop_price * 1.003
                    else:
                        limit_price = stop_price * 0.997
                    order_data["limitPrice"] = round(limit_price, 1)

            for attempt in range(retries):
                try:
                    self.logger.info(f"Placing order (attempt {attempt + 1}/{retries}): {json.dumps(order_data)}")
                    response = self.fyers.place_order(data=order_data)
                    if response.get('s') == "ok":
                        order_id = response["id"]
                        self.logger.info(f"Order placed successfully: {order_id}")
                        with self.lock:
                            self.order_id_to_symbol[order_id] = symbol
                        if self.verify_order_execution(order_id):
                            self.logger.info(f"Order executed: {symbol}, {side}, {qty}, Order ID: {order_id}")
                            return order_id
                        else:
                            self.logger.warning(f"Order placed but execution not verified: {order_id}")
                            return order_id
                    else:
                        error_msg = response.get('message', 'Unknown error')
                        self.logger.error(f"Order failed: {error_msg}")
                        if "insufficient balance" in error_msg.lower():
                            self.logger.error("Insufficient balance, not retrying")
                            return None
                        if attempt < retries - 1:
                            sleep_time = 2 ** attempt
                            self.logger.info(f"Retrying in {sleep_time} seconds...")
                            time.sleep(sleep_time)
                except Exception as e:
                    self.logger.error(f"Order placement error (attempt {attempt + 1}/{retries}): {e}")
                    if attempt < retries - 1:
                        time.sleep(2 ** attempt)
            self.logger.error("All order placement attempts failed")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in place_order: {e}")
            return None

    def verify_order_execution(self, order_id, max_retries=5):
        """Verify if an order has been executed."""

        for attempt in range(max_retries):
            try:
                response = self.fyers.orderbook()
                if response.get('s') != "ok":
                    self.logger.error(f"Failed to fetch orderbook: {response.get('message', 'Unknown error')}")
                    time.sleep(1)
                    continue
                orders = response.get('orderBook', [])
                for order in orders:
                    if order.get('id') == order_id:
                        status = order.get('status')
                        if status == 2:
                            avg_price = order.get('tradedPrice', 0)
                            qty = order.get('filledQty', 0)
                            self.logger.info(f"Order {order_id} executed: {qty} shares at ₹{avg_price}")
                            return True
                        elif status in [5, 6]:
                            self.logger.warning(f"Order {order_id} {status=}, reason: {order.get('message', 'Unknown')}")
                            return False
                if attempt == max_retries - 1:
                    self.logger.warning(f"Order {order_id} not found in orderbook after {max_retries} attempts")
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Error verifying order execution (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(1)
        return False

    def close_position(self, symbol):
        """Close an open position for the given symbol."""

        with self.lock:
            if symbol not in self.positions:
                self.logger.warning(f"Cannot close position for {symbol}: position not found")
                return False
            pos = self.positions[symbol]
            qty = pos["qty"]
            side = "sell" if pos["side"] == "buy" else "buy"
            close_price = self.live_data.get(symbol, {}).get("lp", pos["entry_price"])
            multiplier = 1 if pos["side"] == "buy" else -1
            price_diff = (close_price - pos["entry_price"]) * multiplier
            estimated_pnl = price_diff * qty
            self.logger.info(f"Closing position: {symbol}, Side: {side}, Qty: {qty}, Est. PnL: ₹{estimated_pnl:.2f}")
            order_id = self.place_order(symbol, side, qty)
            if order_id:
                cost = self.calculate_transaction_cost(close_price, qty)
                with self.lock:
                    self.daily_pnl += estimated_pnl - cost
                    pos_pnl = estimated_pnl - cost - pos.get("cost", 0)
                    if pos_pnl > 0:
                        self.logger.info(f"Profit on {symbol}: ₹{pos_pnl:.2f}")
                    else:
                        self.logger.info(f"Loss on {symbol}: ₹{pos_pnl:.2f}")
                    del self.positions[symbol]
                self.logger.info(f"Position closed: {symbol}, Order ID: {order_id}, PnL: ₹{pos_pnl:.2f}")
                return True
            else:
                self.logger.error(f"Failed to close position: {symbol}")
                return False

    def close_all_positions(self):
        """Close all open positions."""

        with self.lock:
            positions_to_close = list(self.positions.keys())
        if not positions_to_close:
            self.logger.info("No positions to close")
            return
        self.logger.info(f"Closing all positions: {positions_to_close}")
        for symbol in positions_to_close:
            self.close_position(symbol)
            time.sleep(0.5)

    def shutdown(self):
        """Perform cleanup tasks before shutting down."""

        if not self.running:
            return
        self.running = False
        self.logger.info("Closing all positions before shutdown...")
        self.close_all_positions()
        if self.monitor:
            self.monitor.stop()
        if self.ws:
            try:
                self.ws.close()
                self.logger.info("WebSocket connection closed")
            except Exception as e:
                self.logger.error(f"Error closing WebSocket: {e}")
        self._save_session_state()
        self.logger.info("Trading session shutdown complete")

    def _save_session_state(self):
        """Save the current session state."""

        state = {
            "positions": self.positions,
            "daily_pnl": self.daily_pnl,
            "account_value": self.account_value,
            "trade_count": self.trade_count,
            "timestamp": datetime.now().isoformat()
        }
        try:
            with open(os.path.join(BASE_DIR, 'trading_session_state.json'), 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2)
            self.logger.info("Session state saved successfully")
        except Exception as e:
            self.logger.error(f"Failed to save session state: {e}")

    def is_market_open(self):
        """Check if the market is currently open."""

        now = datetime.now()
        if now.weekday() > 4:  # 5 and 6 are Saturday and Sunday
            return False
        market_open = datetime.strptime(self.config['TRADE_START_TIME'], "%H:%M:%S").time()
        market_close = datetime.strptime(self.config['TRADE_END_TIME'], "%H:%M:%S").time()
        return market_open <= now.time() <= market_close

    def is_time_to_square_off(self):
        """Check if it's time to square off positions."""

        now = datetime.now()
        square_off_time = datetime.strptime(self.config['SQUARE_OFF_TIME'], "%H:%M:%S").time()
        return now.time() >= square_off_time

    def aggregate_ticks(self):
        """Aggregate market ticks into candlestick data."""

        batches = {sym: [] for sym in SYMBOLS + [VIX_SYMBOL]}
        last_batch_time = time.time()
        while self.running:
            try:
                try:
                    symbol, data = self.tick_queue.get(timeout=1)
                    if symbol not in batches:
                        batches[symbol] = []
                    batches[symbol].append(data)
                    self.tick_queue.task_done()
                except queue.Empty:
                    pass

                current_time = time.time()
                elapsed = current_time - last_batch_time
                if elapsed >= self.config['AGG_INTERVAL']:
                    for symbol, batch in batches.items():
                        if not batch:
                            continue
                        try:
                            df = pd.DataFrame(batch)
                            if len(df) < 2:
                                continue
                            df["timestamp"] = pd.to_datetime(df["feed_time"], unit="s")
                            df.set_index("timestamp", inplace=True)
                            candles = df.resample("1min").agg({
                                "lp": ["first", "max", "min", "last"],
                                "v": "sum" if "v" in df.columns else "count"
                            }).dropna()
                            if len(candles) == 0:
                                continue
                            candles.columns = ["open", "high", "low", "close", "volume"]
                            candles.reset_index(inplace=True)
                            with self.lock:
                                if symbol in self.historical_data:
                                    self.historical_data[symbol] = pd.concat([self.historical_data[symbol], candles]).drop_duplicates(subset=["timestamp"])
                                    self.historical_data[symbol] = self.historical_data[symbol].sort_values("timestamp").tail(500)
                        except Exception as e:
                            self.logger.error(f"Error processing batch for {symbol}: {e}")
                    batches = {sym: [] for sym in batches.keys()}
                    last_batch_time = current_time
            except Exception as e:
                self.logger.error(f"Error in aggregate_ticks: {e}")
                time.sleep(1)

    def calculate_indicators(self, df):
        """Calculate technical indicators for the given DataFrame."""

        if len(df) < 50:
            return df
        try:
            df["ema_fast"] = ta.EMA(df["close"], timeperiod=int(self.config['EMA_FAST']))
            df["ema_slow"] = ta.EMA(df["close"], timeperiod=int(self.config['EMA_SLOW']))
            df["rsi"] = ta.RSI(df["close"], timeperiod=int(self.config['RSI_PERIOD']))
            df["atr"] = ta.ATR(df["high"], df["low"], df["close"], timeperiod=int(self.config['ATR_PERIOD']))
            df["bb_upper"], df["bb_middle"], df["bb_lower"] = ta.BBANDS(
                df["close"],
                timeperiod=int(self.config['BB_PERIOD']),
                nbdevup=float(self.config['BB_STD_DEV']),
                nbdevdn=float(self.config['BB_STD_DEV'])
            )
            if "volume" in df.columns:
                df["obv"] = ta.OBV(df["close"], df["volume"])
                df["volume_ema"] = ta.EMA(df["volume"], timeperiod=20)
                df["volume_ratio"] = df["volume"] / df["volume_ema"]
                df["vwap"] = (df["volume"] * (df["high"] + df["low"] + df["close"]) / 3).cumsum() / df["volume"].cumsum()
        except Exception as e:
            self.logger.error(f"Error calculating indicators: {e}")
        return df

    def get_option_chain(self, symbol, refresh=False):
        """Get the option chain for a given symbol."""

        if not refresh:
            with self.lock:
                if (symbol in self.option_chain_cache and
                        (datetime.now() - self.last_chain_update).total_seconds() < 300):
                    return self.option_chain_cache.get(symbol)
        if symbol not in self.live_data:
            self.logger.warning(f"Cannot fetch option chain: {symbol} price not available")
            return None
        spot_price = self.live_data[symbol]["lp"]
        strike_step = 50 if "NIFTY50" in symbol else 100
        nearest_strike = round(spot_price / strike_step) * strike_step
        today = datetime.now().date()
        days_to_thursday = (3 - today.weekday()) % 7
        expiry_date = today + timedelta(days=days_to_thursday)
        expiry_str = expiry_date.strftime("%y%m%d")
        strikes = [nearest_strike + (i - 5) * strike_step for i in range(11)]
        index_code = "N" if "NIFTY50" in symbol else "B"
        option_symbols = []
        for strike in strikes:
            call_symbol = f"NSE:OPTIDX{index_code}E{expiry_str}{strike}CE"
            put_symbol = f"NSE:OPTIDX{index_code}E{expiry_str}{strike}PE"
            option_symbols.extend([call_symbol, put_symbol])
        try:
            quotes_data = {"symbols": ",".join(option_symbols)}
            response = self.fyers.quotes(quotes_data)
            if response.get('s') != "ok":
                self.logger.error(f"Failed to fetch option quotes: {response.get('message', 'Unknown error')}")
                return None
            quotes = response.get('d', [])
            option_chain = {
                "calls": {},
                "puts": {},
                "spot_price": spot_price,
                "nearest_strike": nearest_strike,
                "expiry_date": expiry_date.isoformat()
            }
            for quote in quotes:
                symbol = quote.get('n', '')
                if not symbol:
                    continue
                parts = symbol.split('E')
                if len(parts) < 2:
                    continue
                strike_info = parts[1]
                if len(strike_info) < 2:
                    continue
                option_type = strike_info[-2:]
                strike = int(strike_info[8:-2])
                option_data = {
                    "symbol": symbol,
                    "strike": strike,
                    "bid": quote.get('bid', 0),
                    "ask": quote.get('ask', 0),
                    "ltp": quote.get('lp', 0),
                    "volume": quote.get('v', 0),
                    "oi": quote.get('oi', 0) if 'oi' in quote else 0
                }
                if option_type == "CE":
                    option_chain["calls"][strike] = option_data
                elif option_type == "PE":
                    option_chain["puts"][strike] = option_data
            with self.lock:
                self.option_chain_cache[symbol] = option_chain
                self.last_chain_update = datetime.now()
            return option_chain
        except Exception as e:
            self.logger.error(f"Error fetching option chain: {e}")
            return None

    def calculate_transaction_cost(self, price, qty):
        """Calculate the total transaction cost for an order."""

        turnover = price * qty
        brokerage = min(turnover * FEES['BROKERAGE'], 20)
        stt = turnover * FEES['STT']
        exchange_charges = turnover * FEES['EXCHANGE']
        sebi_charges = turnover * FEES['SEBI']
        slippage = turnover * FEES['SLIPPAGE']
        total_cost = brokerage + stt + exchange_charges + sebi_charges + slippage
        gst = (brokerage + exchange_charges) * 0.18
        return total_cost + gst

    def analyze_index_movement(self, index_symbol):
        """Analyze the movement of an index and generate trading signals."""

        with self.lock:
            df = self.historical_data.get(index_symbol, None)
        if df is None or len(df) < 50:
            self.logger.warning(f"Not enough data for {index_symbol} analysis")
            return None
        df = self.calculate_indicators(df)
        latest = df.iloc[-1]
        vix_value = None
        with self.lock:
            if VIX_SYMBOL in self.live_data:
                vix_value = self.live_data[VIX_SYMBOL]["lp"]
        high_volatility = vix_value is not None and vix_value > self.config['VIX_THRESHOLD']
        signal = None
        signal_strength = 0
        trend = "neutral"
        if latest["ema_fast"] > latest["ema_slow"]:
            trend = "bullish"
        elif latest["ema_fast"] < latest["ema_slow"]:
            trend = "bearish"
        with self.lock:
            self.strategy_state[index_symbol] = {"trend": trend}
        if trend == "bullish":
            if (latest["close"] > latest["ema_fast"] and
                    latest["rsi"] > 40 and latest["rsi"] < 70):
                signal = "buy"
                signal_strength = min(70, latest["rsi"]) / 70.0
        elif trend == "bearish":
            if (latest["close"] < latest["ema_fast"] and
                    latest["rsi"] < 60 and latest["rsi"] > 30):
                signal = "sell"
                signal_strength = (70 - max(30, latest["rsi"])) / 40.0
        if latest["close"] < latest["bb_lower"] and latest["rsi"] < 30:
            signal = "buy"
            signal_strength = max(signal_strength, 0.7)
        elif latest["close"] > latest["bb_upper"] and latest["rsi"] > 70:
            signal = "sell"
            signal_strength = max(signal_strength, 0.7)
        if high_volatility and signal_strength > 0:
            signal_strength *= 0.5
            self.logger.info(f"High volatility ({vix_value}), reducing signal strength")
        if signal:
            self.logger.info(f"Signal for {index_symbol}: {signal} (strength: {signal_strength:.2f}, trend: {trend})")
            return {
                "symbol": index_symbol,
                "signal": signal,
                "strength": signal_strength,
                "trend": trend,
                "price": latest["close"],
                "vix": vix_value
            }
        return None

    def load_historical_data(self):
        """Load historical data for the symbols."""

        today = datetime.now().date()
        end_date = datetime.combine(today, datetime.min.time())
        start_date = end_date - timedelta(days=self.config['HISTORICAL_DAYS'])
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        for symbol in SYMBOLS:
            try:
                self.logger.info(f"Loading historical data for {symbol}: {start_str} to {end_str}")
                history_data = {
                    "symbol": symbol,
                    "resolution": "5",
                    "date_format": "1",
                    "range_from": start_str,
                    "range_to": end_str,
                    "cont_flag": "1"
                }
                response = self.fyers.history(history_data)
                if response.get('s') != "ok":
                    self.logger.error(f"Failed to fetch historical data: {response.get('message', 'Unknown error')}")
                    continue
                candles = response.get('candles', [])
                if not candles:
                    self.logger.warning(f"No historical data received for {symbol}")
                    continue
                df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
                df = self.calculate_indicators(df)
                with self.lock:
                    self.historical_data[symbol] = df
                self.logger.info(f"Loaded {len(df)} historical candles for {symbol}")
            except Exception as e:
                self.logger.error(f"Error loading historical data for {symbol}: {e}")
        self.history_loaded = True
        self.logger.info("Historical data loading complete")

    def monitor_account(self):
        """Monitor the account balance and P&L."""

        while self.running:
            try:
                funds_response = self.fyers.funds()
                if funds_response.get('s') != "ok":
                    self.logger.error(f"Failed to fetch account funds: {funds_response.get('message', 'Unknown error')}")
                else:
                    fund_limit = funds_response.get('fund_limit', [{}])[0]
                    with self.lock:
                        self.account_value = fund_limit.get('equityAmount', self.account_value)
                        if self.initial_account_value == 100000:
                            self.initial_account_value = self.account_value
                    drawdown_pct = ((self.account_value - self.initial_account_value) / self.initial_account_value) * 100 if self.initial_account_value > 0 else 0
                    self.logger.info(f"Account balance: ₹{self.account_value:.2f}, "
                                     f"Daily P&L: ₹{self.daily_pnl:.2f}, "
                                     f"Drawdown: {drawdown_pct:.2f}%")
                    if self.initial_account_value > 0:
                        if drawdown_pct <= self.config['MAX_DRAWDOWN_PCT']:
                            self.logger.warning(f"Maximum drawdown reached: {drawdown_pct:.2f}%")
                            self.close_all_positions()
                        if self.daily_pnl <= self.config['MAX_DAILY_LOSS']:
                            self.logger.warning(f"Maximum daily loss reached: ₹{self.daily_pnl:.2f}")
                            self.close_all_positions()
                time.sleep(60)
            except Exception as e:
                self.logger.error(f"Error monitoring account: {e}")
                time.sleep(30)

    def execute_strategy(self):
        """Execute the trading strategy."""

        self.logger.info("Starting strategy execution")
        max_wait = 60
        start_time = time.time()
        while not self.history_loaded and time.time() - start_time < max_wait:
            self.logger.info("Waiting for historical data...")
            time.sleep(5)
        if not self.history_loaded:
            self.logger.warning("Historical data not loaded, proceeding with limited data")

        while self.running:
            try:
                if not self.is_market_open():
                    if self.is_time_to_square_off():
                        if self.positions:
                            self.logger.info("Market closing, squaring off all positions")
                            self.close_all_positions()
                    time.sleep(30)
                    continue

                if self.is_time_to_square_off():
                    if self.positions:
                        self.logger.info("Square-off time reached, closing all positions")
                        self.close_all_positions()
                    time.sleep(30)
                    continue

                for symbol in SYMBOLS:
                    with self.lock:
                        if symbol in self.positions:
                            self.manage_open_position(symbol)
                            continue
                        if len(self.positions) >= self.config['MAX_POSITION_SIZE']:
                            self.logger.info(f"Maximum positions reached ({self.config['MAX_POSITION_SIZE']})")
                            break
                    signal_data = self.analyze_index_movement(symbol)
                    if signal_data and signal_data["strength"] > 0.5:
                        symbol_info = SYMBOL_MAPPINGS.get(symbol, {})
                        lot_size = symbol_info.get("lot_size", 50)
                        with self.lock:
                            current_price = self.live_data.get(symbol, {}).get("lp", signal_data["price"])
                        qty = lot_size
                        if signal_data["signal"] == "buy":
                            self.logger.info(f"BUY signal for {symbol}: strength={signal_data['strength']:.2f}")
                            order_id = self.place_order(symbol, "buy", qty)
                            if order_id:
                                with self.lock:
                                    cost = self.calculate_transaction_cost(current_price, qty)
                                    self.positions[symbol] = {
                                        "symbol": symbol,
                                        "side": "buy",
                                        "qty": qty,
                                        "entry_price": current_price,
                                        "entry_time": datetime.now().isoformat(),
                                        "cost": cost
                                    }
                                self.trade_count += 1
                        elif signal_data["signal"] == "sell":
                            self.logger.info(f"SELL signal for {symbol}: strength={signal_data['strength']:.2f}")
                            order_id = self.place_order(symbol, "sell", qty)
                            if order_id:
                                with self.lock:
                                    cost = self.calculate_transaction_cost(current_price, qty)
                                    self.positions[symbol] = {
                                        "symbol": symbol,
                                        "side": "sell",
                                        "qty": qty,
                                        "entry_price": current_price,
                                        "entry_time": datetime.now().isoformat(),
                                        "cost": cost
                                    }
                                self.trade_count += 1
                time.sleep(30)
            except Exception as e:
                self.logger.error(f"Error executing strategy: {e}")
                time.sleep(30)

    def manage_open_position(self, symbol):
        """Manage an open position, including trailing stop and profit taking."""

        with self.lock:
            if symbol not in self.positions or symbol not in self.live_data:
                return
            position = self.positions[symbol]
            current_price = self.live_data[symbol]["lp"]
            price_diff = (current_price - position["entry_price"]) * (1 if position["side"] == "buy" else -1)
            unrealized_pnl = price_diff * position["qty"]
            unrealized_pnl_pct = (price_diff / position["entry_price"]) * 100

            if unrealized_pnl_pct >= self.config['PROFIT_TAKING_THRESHOLD']:
                self.logger.info(f"Taking profit on {symbol}: {unrealized_pnl_pct:.2f}% gain")
                self.close_position(symbol)
                return

            if unrealized_pnl_pct >= self.config['TRAILING_STOP_TRIGGER_PCT']:
                if "trailing_stop" not in position:
                    trailing_stop = current_price * (1 - self.config['TRAILING_STOP_DISTANCE_PCT'] / 100) if position["side"] == "buy" else \
                                    current_price * (1 + self.config['TRAILING_STOP_DISTANCE_PCT'] / 100)
                    position["trailing_stop"] = trailing_stop
                    self.logger.info(f"Setting trailing stop for {symbol} at {trailing_stop:.2f}")
                else:
                    if position["side"] == "buy":
                        new_stop = current_price * (1 - self.config['TRAILING_STOP_DISTANCE_PCT'] / 100)
                        if new_stop > position["trailing_stop"]:
                            position["trailing_stop"] = new_stop
                            self.logger.info(f"Updating trailing stop for {symbol} to {new_stop:.2f}")
                    else:
                        new_stop = current_price * (1 + self.config['TRAILING_STOP_DISTANCE_PCT'] / 100)
                        if new_stop < position["trailing_stop"]:
                            position["trailing_stop"] = new_stop
                            self.logger.info(f"Updating trailing stop for {symbol} to {new_stop:.2f}")

                    if position["side"] == "buy" and current_price <= position["trailing_stop"]:
                        self.logger.info(f"Trailing stop hit for {symbol}: closing position at {current_price:.2f}")
                        self.close_position(symbol)
                    elif position["side"] == "sell" and current_price >= position["trailing_stop"]:
                        self.logger.info(f"Trailing stop hit for {symbol}: closing position at {current_price:.2f}")
                        self.close_position(symbol)

            if unrealized_pnl_pct <= -self.config['MAX_DRAWDOWN_PCT']:
                self.logger.warning(f"Maximum loss reached for {symbol}: {unrealized_pnl_pct:.2f}%, closing position")
                self.close_position(symbol)

    def run(self):
        """Start the trading session and all associated threads."""

        try:
            self.logger.info("Starting trading session")
            self.running = True

            # Load or generate access token
            self.access_token = self._load_token()
            if not self.access_token or not self._is_token_valid():
                self.access_token = self.generate_access_token()
                if not self.access_token:
                    self.logger.error("Failed to obtain access token. Exiting.")
                    return

            # Initialize Fyers API client
            self.fyers = FyersModelWrapper(self.client_id, self.access_token)

            # Verify API connection
            profile = self.get_profile()
            if not profile or profile.get('s') != 'ok':
                self.logger.error("Failed to verify API connection")
                return

            # Initialize order and position monitor
            self.monitor = OrderPositionMonitor(self)
            self.monitor.load_monitor_state()
            self.monitor.start()

            # Initialize WebSocket connection
            if not self.init_websocket():
                self.logger.error("Failed to initialize WebSocket")
                self.shutdown()
                return

            # Start background threads
            Thread(target=self.monitor_websocket, daemon=True).start()
            Thread(target=self.aggregate_ticks, daemon=True).start()
            Thread(target=self.monitor_account, daemon=True).start()
            Thread(target=self.execute_strategy, daemon=True).start()
            Thread(target=self.start_token_refresh_scheduler, daemon=True).start()

            # Load historical data
            Thread(target=self.load_historical_data, daemon=True).start()

            self.logger.info("Trading session fully initialized")

            # Keep the main thread running
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            while self.running:
                time.sleep(1)

        except Exception as e:
            self.logger.error(f"Error in run: {e}")
            self.shutdown()
        finally:
            self.shutdown()

    def _signal_handler(self, sig, frame):
        """Handle system signals for graceful shutdown."""

        self.logger.info(f"Received signal {sig}, initiating shutdown")
        self.shutdown()
        sys.exit(0)

def main():
    """Main entry point for the trading script."""

    try:
        print("Attempting to instantiate TradingSession")
        if 'TradingSession' not in globals():
            raise NameError("TradingSession class is not defined in this scope")
        trading_session = TradingSession()
        print("TradingSession instantiated successfully")
        trading_session.run()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        raise  # Re-raise to see full traceback

if __name__ == "__main__":
    main()
