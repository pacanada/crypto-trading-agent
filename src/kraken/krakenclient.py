import requests
import pandas as pd
import json
import time
import sys
import platform
import time
import base64
import hashlib
import hmac
from src.base.platformclient import PlatformClient
# TODO: Remove assert, only Exceptions



class KrakenClient(PlatformClient):
    def __init__(self, api_private_key=None, api_public_key=None):
        self.api_private_key=api_private_key
        self.api_public_key=api_public_key
        
    def get_last_historical_data(self, pair_name: str, interval: int):
        """TODO: add description"""
        
        if interval not in [1, 5, 15, 30, 60, 240, 1440, 10080, 21600]:
            raise ValueError("Interval is not supported")
        data = self._get_historical_data_from_crypto(pair_name=pair_name, interval=interval)
        df = self.from_dict_to_df(data=data)
        #df = self.fix_columns_type(df)
        #df = self.set_datetime_as_index(df)
        return df
    
    def _get_historical_data_from_crypto(self, pair_name: str, interval: int):
        """TODO: add description"""
        output=self.krakenapi_func(
            ["","OHLC", f"pair={pair_name.lower()}", f"interval={interval}"],
            api_private_key=None,
            api_public_key=None
            
        )
        output_dict = eval(output)
        return output_dict
    
    def from_dict_to_df(self, data: dict):
        """TODO: add description"""
        result_name = list(data["result"].keys())[0]
        df_raw = pd.DataFrame.from_dict(data=data["result"][result_name])
        df = pd.DataFrame()
        df[["time", "open", "high", "low", "close", "vwap", "volume", "count"]] = df_raw.copy()

        return df
    def fix_columns_type(self, df: pd.DataFrame):
        """TODO: add description"""
        df = df.astype(float).copy()
        df[["time", "count"]] = df[["time", "count"]].astype(int).copy()
        return df
        
        
    def execute_order(self, order_type, volume, pair_name):
        assert order_type in ["sell", "buy"], "Unknown order_type"
        output=self.krakenapi_func(
            sysargs=[
                        "",
                        "AddOrder",
                        f"pair={pair_name.lower()}",
                        f"type={order_type.lower()}",
                        "ordertype=market",
                        f"volume={volume}"
            ],
            api_private_key=self.api_private_key,
            api_public_key=self.api_public_key
            )
        
        print(output)
        id_order = self.get_id_order(output)
        finalized_order = self.wait_until_fulfilled(id_order, order_type, pair_name)
        assert finalized_order["Id"] == id_order
        return finalized_order

    def _get_order_book_response(self, pair_name, since=None, count=10):
        resp = requests.get(f"https://api.kraken.com/0/public/Depth?pair={pair_name}&since={since}&count={count}")
        return resp.json()

    def _get_best_limit_price(self, order_type, pair_name):
        order_book = self._get_order_book_response(pair_name)
        if order_type=="buy":
            # {'error': [],
            #'result': {'XXLMZEUR': {'asks': [['0.28805700', '3104.111', 1637781170],
            output = order_book["result"][list(order_book["result"].keys())[0]]["asks"][0][0]
        elif order_type=="sell":
            output = order_book["result"][list(order_book["result"].keys())[0]]["bids"][0][0]
        return float(output)


    def execute_limit_market_order(self, order_type, volume, pair_name):
        """TODO: It will break when it cannot make the trade at the limit specified, 
        it is not probably a problem for crypto with low volume of trades but still, 
        we have to change the wait_until_ function to handle that """
        assert order_type in ["sell", "buy"], "Unknown order_type"
        # get best posible limit price
        limit_price = self._get_best_limit_price(order_type=order_type, pair_name=pair_name)
        output=self.krakenapi_func(
            sysargs=[
                        "",
                        "AddOrder",
                        f"pair={pair_name.lower()}",
                        f"type={order_type.lower()}",
                        "ordertype=limit",
                        f"volume={volume}",
                        f"price={limit_price}"
            ],
            api_private_key=self.api_private_key,
            api_public_key=self.api_public_key
            )
        
        print(output)
        id_order = self.get_id_order(output)
        finalized_order = self.wait_until_fulfilled(id_order, order_type, pair_name)
        assert finalized_order["Id"] == id_order
        return finalized_order

    def execute_order_leverage(self, trade_type, volume, pair_name, order_type, leverage, price=None ):
        assert trade_type in ["Sell", "Buy"], "Unknown order_type"
        sysargs = [
                        "",
                        "AddOrder",
                        f"pair={pair_name.lower()}",
                        f"type={trade_type.lower()}",
                        f"ordertype={order_type}",
                        f"volume={volume}",
                        f"leverage={leverage}"
            ]
        if price is not None:
            # for market type limit or stoploss or takeprofit
            sysargs = sysargs + [f"price={price}"]
        
        output=self.krakenapi_func(
            sysargs=sysargs,
            api_private_key=self.api_private_key,
            api_public_key=self.api_public_key
            )
        
        print(output)
        id_order = self.get_id_order(output)
        if order_type not in ["take-profit", "stop-loss"]:
            finalized_order = self.wait_until_fulfilled(id_order, order_type, pair_name)
        else:
            print("We dont have to wait until fulfilled")
            finalized_order = {"Id": id_order,
                                "Price": price}
        assert finalized_order["Id"] == id_order
        return finalized_order

    def cancel_order(self, txid):
        output=self.krakenapi_func(
            sysargs=[
                        "",
                        "CancelOrder",
                        f"txid={txid}",
            ],
            api_private_key=self.api_private_key,
            api_public_key=self.api_public_key
            )
        return output

    def get_closed_order(self,):
        output=self.krakenapi_func(
            sysargs=[
                        "",
                        "ClosedOrders",
            ],
            api_private_key=self.api_private_key,
            api_public_key=self.api_public_key
            )
        output = eval(output.replace("null", "None"))
        return output
    def get_closed_order_from_start(self, start_id):
        output=self.krakenapi_func(
            sysargs=[
                        "",
                        "ClosedOrders",
                        f"start={start_id}",
            ],
            api_private_key=self.api_private_key,
            api_public_key=self.api_public_key
            )
        output = eval(output.replace("null", "None"))
        return output

    
    def execute_mock_order(self, order_type, volume, pair_name):
        """mock order to simulate and not have to make a trade"""
        assert order_type in ["Sell", "Buy"], "Unknown order_type"
        finalized_order = {'Id': 'OPJZJL-V76CB-ZWOYJK', 'Price': 0.331511, 'Action': 'Sell'}
        return finalized_order
        
    def get_id_order(self, output):
        id_order = eval(output)["result"]["txid"][0]
        return id_order
    def wait_until_fulfilled(self, id_order, order_type, pair_name ):
        while True:
            # Get last closed order
            output_closedorders = self.krakenapi_func(
                sysargs=[" ","ClosedOrders"],
                api_private_key=self.api_private_key,
                api_public_key=self.api_public_key)
            output_closeorders_json=json.loads(output_closedorders)
            finalized_id = list(output_closeorders_json["result"]["closed"].keys())[0]
            finalized_price = eval(output_closeorders_json["result"]["closed"][finalized_id]["price"])

            print("checking id",id_order,finalized_id)
            # Wait until id of last trade is recognized
            time.sleep(3)
            if id_order==finalized_id:
                finalized_order = {
                    "Id": id_order,
                    "Price": finalized_price,
                    "Action": order_type,
                    "Pair_Name": pair_name
                    }   
                #send_slack_message(text=str(finalized_order), channel=CHANNEL_NAME)
                #save_order(finalized_order, dir_data=dir_finalized_order, name_file=finalized_orders_file)
                break
        return finalized_order


    def krakenapi_func(self, sysargs: list, api_public_key: str, api_private_key: str ):
        """
        # Kraken Rest API
        #
        # Usage: ./krakenapi.py method [parameters]
        # Example: ./krakenapi.py Time
        # Example: ./krakenapi.py OHLC pair=xbtusd interval=1440
        # Example: ./krakenapi.py Balance
        # Example: ./krakenapi.py OpenPositions
        # Example: ./krakenapi.py AddOrder pair=xxbtzusd type=buy ordertype=market volume=0.003 leverage=5
        """
        if int(platform.python_version_tuple()[0]) > 2:
            import urllib.request as urllib2
        else:
            import urllib2

        api_public = {"Time", "Assets", "AssetPairs", "Ticker", "OHLC", "Depth", "Trades", "Spread"}
        api_private = {"Balance", "BalanceEx", "TradeBalance", "OpenOrders", "ClosedOrders", "QueryOrders", "TradesHistory", "QueryTrades", "OpenPositions", "Ledgers", "QueryLedgers", "TradeVolume", "AddExport", "ExportStatus", "RetrieveExport", "RemoveExport", "GetWebSocketsToken"}
        api_trading = {"AddOrder", "CancelOrder", "CancelAll"}
        api_funding = {"DepositMethods", "DepositAddresses", "DepositStatus", "WithdrawInfo", "Withdraw", "WithdrawStatus", "WithdrawCancel", "WalletTransfer"}

        api_domain = "https://api.kraken.com"
        api_data = ""

        if len(sysargs) < 2:
            api_method = "Time"
        elif len(sysargs) == 2:
            api_method = sysargs[1]
        else:
            api_method = sysargs[1]
            for count in range(2, len(sysargs)):
                if count == 2:
                    api_data = sysargs[count]
                else:
                    api_data = api_data + "&" + sysargs[count]

        if api_method in api_private or api_method in api_trading or api_method in api_funding:
            api_path = "/0/private/"
            api_nonce = str(int(time.time()*1000))
            try:
                api_key = api_public_key#open("API_Public_Key").read().strip()
                api_secret = base64.b64decode(api_private_key)#open("API_Private_Key").read().strip())
            except:
                print("API public key and API private (secret) key must be in text files called API_Public_Key and API_Private_Key")
                #sys.exit(1)
            api_postdata = api_data + "&nonce=" + api_nonce
            api_postdata = api_postdata.encode('utf-8')
            api_sha256 = hashlib.sha256(api_nonce.encode('utf-8') + api_postdata).digest()
            api_hmacsha512 = hmac.new(api_secret, api_path.encode('utf-8') + api_method.encode('utf-8') + api_sha256, hashlib.sha512)
            api_request = urllib2.Request(api_domain + api_path + api_method, api_postdata)
            api_request.add_header("API-Key", api_key)
            api_request.add_header("API-Sign", base64.b64encode(api_hmacsha512.digest()))
            api_request.add_header("User-Agent", "Kraken REST API")
        elif api_method in api_public:
            api_path = "/0/public/"
            api_request = urllib2.Request(api_domain + api_path + api_method + '?' + api_data)
            print(api_domain + api_path + api_method + '?' + api_data)
            api_request.add_header("User-Agent", "Kraken REST API")
        else:
            print("Usage: %s method [parameters]" % sysargs[0])
            print("Example: %s OHLC pair=xbtusd interval=1440" % sysargs[0])
            #sys.exit(1)

        try:
            api_reply = urllib2.urlopen(api_request).read()
        except Exception as error:
            print("API call failed (%s)" % error)

        try:
            api_reply = api_reply.decode()
        except Exception as error:
            if api_method == 'RetrieveExport':
                sys.stdout.buffer.write(api_reply)

            print("API response invalid (%s)" % error)
            #sys.exit(1)
        
        if '"error":[]' in api_reply:
            output = api_reply
            
        else:
            print(api_reply)
        return output
