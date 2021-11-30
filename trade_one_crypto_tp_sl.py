import time

import pandas as pd
from src.datafetch.datafetch import DataFetch

from src.features.utils import feature_pipeline
from src.kraken.krakenclient import KrakenClient
from src.logger.slackclient import SlackClient
import pickle
import click


def execute_order_and_log(order_type, pair_name, volume, logger, kraken_client):
    try:
        output = kraken_client.execute_limit_market_order(
            order_type=order_type, volume=volume, pair_name=pair_name)
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
@click.option('--volume', type=float, prompt="Volume to trade")
@click.option('--next_action', type=str, prompt="Next action to execute", default="buy")
@click.option(
    '--previous_price',
    type=str,
    prompt="Previous price, only use with next_action=sell, otherwise it will activate take_profit",
    default=0
    )
def main(pair_name, model_dir, volume, next_action, previous_price):

    # config
    params={'take_profit_pct': 0.05876069829164293,
  'stop_loss_pct': 0.029986100049156885,
  'lim_pred_buy': -8.92214825484428
  }
    
    base_columns = ["date", "open", "close", "low", "high", "vwap", "volume", "preds"]

    api_public_key = open("API_Public_Key").read().strip()
    api_private_key = open("API_Private_Key").read().strip()
    slack_url = open("slack_url").read().strip()

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
        df["next_action"] = next_action
        # introduce lag of -1 to avoid values per minute that may change
        last_pred = df.iloc[-2].preds #df.tail(1).preds.values[0]
        last_date = df.iloc[-2].date #df.tail(1).date.values[0]
        last_open = df.iloc[-2].open #df.tail(1).open.values[0]
        # log
        try:
            df_log = pd.read_csv(f"runs/run_{pair_name}_{volume}.csv")
        except FileNotFoundError:
            df_log = pd.DataFrame()
            df_log.to_csv(f"runs/run_{pair_name}_{volume}.csv", index=False)

        #feature_columns = [col for col in df.columns if col.startswith("feature")]
        df_log = pd.concat([df_log, df[base_columns].tail(2)])
        #df_log[base_columns+feature_columns]= df_log[base_columns+feature_columns].astype("float32").copy()
        df_log.drop_duplicates().to_csv(f"runs/run_{pair_name}_{volume}.csv", index=False)
        print(f"Prediction of {pair_name} for {last_date} is {last_pred}")
        print(f"Open at {last_open}")


        if (last_pred < params["lim_pred_buy"]) and (next_action=="buy"):
            print("Buying")
            execute_order_and_log(
                order_type="buy",
                pair_name=pair_name,
                volume=volume, 
                logger=slack_client,
                kraken_client=kraken_client)

            previous_price = last_open
            next_action = "sell"
            
        elif (last_open>(1+params["take_profit_pct"])*previous_price) and (next_action=="sell"):
            print("Selling take profit")
            execute_order_and_log(
                order_type="sell",
                pair_name=pair_name,
                volume=volume, 
                logger=slack_client,
                kraken_client=kraken_client)

            previous_price = last_open
            next_action = "buy"
        elif (last_open<(1-params["stop_loss_pct"])*previous_price) and (next_action=="sell"):
            print("Selling stop loss")
            execute_order_and_log(
                order_type="sell",
                pair_name=pair_name,
                volume=volume, 
                logger=slack_client,
                kraken_client=kraken_client)
            previous_price = last_open
            next_action = "buy"
        else:
            print("No action")

        time.sleep(60)

if __name__ == "__main__":
    main()




