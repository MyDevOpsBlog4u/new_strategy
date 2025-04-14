from fyers_apiv3 import fyersModel

class FyersModelWrapper:
    def __init__(self, client_id, access_token, log_path="logs/"):
        self.fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, log_path=log_path)

    def get_profile(self):
        return self.fyers.get_profile()

    def orderbook(self):
        return self.fyers.orderbook()

    def positions(self):
        return self.fyers.positions()

    def place_order(self, data):
        return self.fyers.place_order(data)

    def exit_position(self, data):
        return self.fyers.exit_position(data)

    def funds(self):
        return self.fyers.funds()

    def quotes(self, data):
        return self.fyers.quotes(data)

    def history(self, data):
        return self.fyers.history(data)
