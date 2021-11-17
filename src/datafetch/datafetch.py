from src.base.platformclient import PlatformClient
import pandas as pd

class DataFetch:
    """Class for fetching crypto"""
    def __init__(self, pair_name, platform_client: PlatformClient):
        self.pair_name = pair_name
        self.platform_client = platform_client
        self.float_columns =["open", "close", "low", "high", "vwap", "volume"]

    def fetch_data(self, interval: int = 1):
        """TODO: check why we need so many steps with dates, I do not remember"""
        df_temp = self.platform_client.get_last_historical_data(pair_name=self.pair_name, interval=interval)
        df_temp = self._set_datetime_as_index(df_temp)
        df_temp = self._set_columns_type(df_temp)
        return df_temp

    def _set_datetime_as_index(self, df: pd.DataFrame):
        """TODO: add description"""
        df["date"] = pd.to_datetime(df['time'], unit='s')
        df = df.set_index(pd.DatetimeIndex(df["date"])).copy()
        return df

    def _set_columns_type(self, df: pd.DataFrame):
        df[self.float_columns]= df[self.float_columns].astype(float).copy()
        return df