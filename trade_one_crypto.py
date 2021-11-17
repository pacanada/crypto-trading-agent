import time
from src.datafetch.datafetch import DataFetch

from src.features.utils import feature_pipeline
from src.kraken.krakenclient import KrakenClient
from src.logger.slackclient import SlackClient
import pickle
import click


def execute_order_and_log(order_type, pair_name, volume, logger, kraken_client):
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

@click.command()
@click.option(
    "--pair_name",
    type=click.Choice(
        ["xlmeur", "bcheur","compeur","xdgeur", "etheur", "algoeur", "bateur", "adaeur","xrpeur"]),
        prompt="Pair name to trade"
        )
@click.option('--model_dir', prompt="Directory of the model")
@click.option('--threshold', type=float, prompt="Threshold for the model")
@click.option('--volume', type=float, prompt="Volume to trade")
def main(pair_name, model_dir, threshold, volume):

    api_public_key = open("API_Public_Key").read().strip()
    api_private_key = open("API_Private_Key").read().strip()
    slack_url = open("slack_url").read().strip()

    next_action = "buy"
    kraken_client = KrakenClient(api_private_key=api_private_key, api_public_key=api_public_key)
    slack_client = SlackClient(slack_url)
    datafetch = DataFetch(pair_name, platform_client=kraken_client)

    # Load model
    with open(model_dir ,'rb') as f:
        model = pickle.load(f)
    while True:
        # Fetch data
        df = datafetch.fetch_data(interval=1)
        # Preprocess it
        df, _=feature_pipeline(df, include_target=False)
        # Make predictions
        columns_features = [col for col in df.columns if col.startswith("feature")]
        df["preds"] = model.predict(df[columns_features])
        last_pred = df.tail(1).preds.values[0]
        last_date = df.tail(1).date.values[0]
        last_open = df.tail(1).open.values[0]
        print(f"Prediction of {pair_name} for {last_date} is {last_pred}")
        print(f"Open at {last_open}")

        if (-last_pred > threshold) and (next_action=="buy"):
            print("Buying")
            execute_order_and_log(
                order_type="buy",
                pair_name=pair_name,
                volume=volume, 
                logger=slack_client,
                kraken_client=kraken_client)
            next_action = "sell"
        elif (-last_pred < -threshold) and (next_action=="sell"):
            print("Selling")
            execute_order_and_log(
                order_type="sell",
                pair_name=pair_name,
                volume=volume, 
                logger=slack_client,
                kraken_client=kraken_client)
            next_action = "buy"
        else:
            print("No action")

        time.sleep(30)

if __name__ == "__main__":
    main()




