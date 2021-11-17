import time
from src.datafetch.datafetch import DataFetch

from src.features.utils import feature_pipeline
from src.kraken.krakenclient import KrakenClient
from src.logger.slackclient import SlackClient
import pickle

def execute_order_and_log(order_type, pair_name, volume, logger, print_to_stdout=False):
    try:
        output = kraken_client.execute_order(
            order_type=order_type,
            volume=volume,
            pair_name=pair_name)
        logger.send_message(
                text=str(output),
                channel="#trading-finalized-orders",
                title="Finalized order")
    except Exception as e:
        logger.send_message(
                text=str(e),
                channel="#errors",
                title="Errors")
class Config:
    pair_name = "algoeur"
    model_dir = "/Users/pablocanadapereira/Desktop/wow_20.pickle"
    threshold = 2.5
    volume = 20


config = Config()
api_public_key = open("API_Public_Key").read().strip()
api_private_key = open("API_Private_Key").read().strip()
slack_url = open("slack_url").read().strip()

next_action = "buy"
kraken_client = KrakenClient(api_private_key=api_private_key, api_public_key=api_public_key)
slack_client = SlackClient(slack_url)
datafetch = DataFetch(config.pair_name, platform_client=kraken_client)

# Load model
with open(config.model_dir ,'rb') as f:
    model = pickle.load(f)
while True:
    df = datafetch.fetch_data(interval=1)

    # Preprocess it
    df, _=feature_pipeline(df, include_target=False)


    # Make predictions
    columns_features = [col for col in df.columns if col.startswith("feature")]
    df["preds"] = model.predict(df[columns_features])
    last_pred = df.tail(1).preds.values[0]
    last_date = df.tail(1).date.values[0]
    last_open = df.tail(1).open.values[0]
    print(f"Prediction of {config.pair_name} for {last_date} is {last_pred}")
    print(f"Open at {last_open}")

    if (-last_pred> config.threshold) and (next_action=="buy"):
        # Buy
        print("Buying")
        execute_order_and_log(order_type="buy", pair_name=config.pair_name, volume=config.volume, logger=slack_client)
        next_action = "sell"
    elif (-last_pred < -config.threshold) and (next_action=="sell"):
        # Sell
        print("Selling")
        execute_order_and_log(order_type="sell", pair_name=config.pair_name, volume=config.volume, logger=slack_client)
        next_action = "buy"
    else:
        print("No action")

    time.sleep(30)




