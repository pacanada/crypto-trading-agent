import numpy as np
from sklearn.model_selection import train_test_split
import pandas as pd
from itertools import permutations

def perform_operation(df, columns, operation):
    df = df.copy()
    if operation=="sum":
        out = df[columns[0]] + df[columns[1]]
    elif operation=="substract":
        out = df[columns[0]] - df[columns[1]]
    elif operation=="divide":
        out = df[columns[0]] / df[columns[1]]
    elif operation=="multiply":
        out = df[columns[0]] * df[columns[1]]
    return out

def fill_na(df):
    df = df.copy()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df.fillna(0)
def normalize(df, column_to_normalize):
    df = df.copy()
    for col in ["open", "close", "high", "low"]:
        df[f"{col}_norm"] = df[col] / df[column_to_normalize]
    
    return df
def generate_features(df):
    cont = 0
    list_feature_recipe = []
    base_columns = ["open_norm", "close_norm", "high_norm", "low_norm", "volume"]
    operations = ["sum", "substract", "divide", "multiply"]
    list_permutations_columns = list(permutations(base_columns, 2))
    # First level
    for columns in list_permutations_columns:
        for operation in operations:
            df[f"feature_{cont}"] = perform_operation(df, columns, operation)
            list_feature_recipe.append((cont, columns ,operation))
            cont +=1
    # second level
    #list_permutations_second_columns = list(permutations([col for col in df.columns if col.startswith("feature")],2))
    #for columns in list_permutations_second_columns:
    #    for operation in operations:
    #        #print(cont, columns, operations)
    #        df[f"feature_{cont}"] = perform_operation(df, columns, operation)
    #        cont +=1
                                       
    return df, list_feature_recipe

def to_float32(df):
    df = df.copy()
    feature_columns = [col for col in df.columns if col.startswith("feature")]
    df[feature_columns] = df[feature_columns].astype("float32")
    return df

def add_target(df, column_to_apply, target_list=None):
    if target_list is None:
        targets = [1,2,5,10,20,50,80,100]
    else:
        targets = target_list
    for target in targets:
        df[f"target_{target}"] = df[column_to_apply].pct_change(-target)
    return df



def feature_pipeline(df, include_target=True, target_list=None):
    column_to_apply="open"
    #df = pd.read_csv(get_project_root() / "data" / "historical" / f"{pair_name}.csv")
    df = normalize(df, column_to_normalize=column_to_apply)
    df, list_feature_recipe = generate_features(df)
    df = fill_na(df)
    df = to_float32(df)
    if include_target:
        df = add_target(df, column_to_apply, target_list)
    df = add_domain_features(df, column_to_apply)
    #df_validation=df.iloc[int(df.shape[0]*0.8):].dropna().copy()
    #df = df.iloc[0:int(df.shape[0]*0.8)].copy()
    df = df.dropna().reset_index(drop=True)
    
    return df, list_feature_recipe
    
def get_training_df(df):
    return df.dropna().reset_index().copy()

def get_df_info(run_info):
    # process run_info
    scores = [score for (score,_,_) in run_info]
    features = [features for (_,features,_) in run_info]
    importance = [importance for (_,_,importance) in run_info]
    pass

def update_info(run_info, df_info):
    scores = [score for (score,_,_) in run_info]
    features = [features for (_,features,_) in run_info]
    importance = [importance for (_,_,importance) in run_info]
    idx = scores.index(max(scores))
    df_info[features[idx]] += importance[idx]
    
    return df_info
            
def split_train_test(X, y):
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)
    return X_train, X_test, y_train, y_test 

def get_best_columns(df_info, n):
    sorted_array=np.array(df_info.T.values).reshape(-1)
    df_info.T.iloc[sorted_array.argsort()[-n::]].T.columns
    return df_info.T.iloc[sorted_array.argsort()[-n::]].T.columns

def compute_RSI(df: pd.DataFrame, column_to_apply: str, interval_type="100s"):
    delta = df[column_to_apply].diff(1)
    delta.dropna(inplace=True)
    positive = delta.copy()
    negative = delta.copy()
    positive[positive<0] = 0
    negative[negative>0] = 0
    average_gain = positive.rolling(interval_type).mean()
    average_loss = abs(negative.rolling(interval_type).mean())
    relative_strength = average_gain /average_loss
    RSI = 100.0-(100.0/(1.0+relative_strength))
    return RSI.copy()
def compute_MACD(df: pd.DataFrame, column_to_apply: str, min_half_life: str, max_half_life: str, mid_half_life:str):
    exp1 = df[column_to_apply].ewm(halflife=min_half_life).mean()
    exp2 = df[column_to_apply].ewm(halflife=max_half_life).mean()
    macd = exp1-exp2
    exp3 = macd.ewm(halflife=mid_half_life).mean()
    #df[f"macd"] = macd
    #df[f"signal"] = exp3
    #df["macd"] = macd.values-exp3.values
    #df["dummy_sign"]= df["macd_minus_signal"].apply(lambda x: 1 if x>0 else -1)
    #df[f"macd_minus_signal_diff_{min_half_life}_{max_half_life}_{mid_half_life}"] = df["dummy_sign"].diff(1)
    return macd.values-exp3.values

def compute_ewm(df, period, column_to_apply):
    
    out = df[column_to_apply].ewm(halflife=period,min_periods=0,adjust=False,ignore_na=False).mean()
    return out

def compute_pct_change(df, period, column_to_apply):

    out = df[column_to_apply].pct_change(period)
    return out


def add_domain_features(df, column_to_apply):
    df = df.copy()
    cont=0
    ewm_list=[5,10,15,20,40,60,80,100,120,140,200,250,300]
    rsi_list=[20, 30, 50, 70, 100, 200, 300, 400]
    macd_list = [(20,40,100), (40,80,200), (10,20,50), (80,160,100),(5,10,30)]
    pct_change_list = [1,2,3,5,10,20,40,60,100,200,300,400]
    
    for ewm_coef in ewm_list:
        df[f"feature_domain_{cont}"]=compute_ewm(df=df, period=ewm_coef, column_to_apply=column_to_apply) / df[column_to_apply]
        cont+=1
        
    for rsi_coef in rsi_list:
        df[f"feature_domain_{cont}"]=compute_RSI(df=df, column_to_apply=column_to_apply, interval_type=rsi_coef)
        cont+=1
    for (min_half_life, max_half_life, mid_half_life) in macd_list:
        df[f"feature_domain_{cont}"]=compute_MACD(
            df=df,
            column_to_apply=column_to_apply,
            min_half_life=min_half_life,
            max_half_life=max_half_life,
            mid_half_life=mid_half_life
        )
        cont+=1 

    for pct_coef in pct_change_list:
        df[f"feature_domain_{cont}"]=compute_pct_change(df=df, period=pct_coef, column_to_apply=column_to_apply)
        cont+=1
    return df
