import src.mlflow_utils as mlflow_utils
import src.parquet_utils as parquet_utils
import hashlib
import pandas as pd
import mlflow
import re

from sklearn.preprocessing import MinMaxScaler, StandardScaler

def _remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    rows_before = df.shape[0]
    df.drop_duplicates(inplace=True)
    rows_after = df.shape[0]
    mlflow.log_param("Removed duplicates", rows_before - rows_after)
    mlflow_utils.log_artifact(df=df, name="remove_duplicates")
    return df

def _null_treatment(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["Model"])
    mlflow.log_param("remove-null-Model", True)

    df = df.dropna(subset=["Year"])
    mlflow.log_param("remove-null-Year", True)

    df = df.dropna(subset=["Region"])
    mlflow.log_param("remove-null-Region", True)

    color_mode = df["Color"].mode()[0]
    mlflow.log_param("imput-Color", "mode")

    fuel_type_mode = df["Fuel_Type"].mode()[0]
    mlflow.log_param("imput-Fuel_Type", "mode")

    transmission_mode = df["Transmission"].mode()[0]
    mlflow.log_param("imput-Transmission", "mode")

    engine_size_l_median = df["Engine_Size_L"].median()
    mlflow.log_param("imput-Engine_Size_L", "median")

    mileage_km_median = df["Mileage_KM"].median()
    mlflow.log_param("imput-Mileage_KM", "median")

    sales_volume_median = df["Sales_Volume"].median()
    mlflow.log_param("imput-Sales_Volume", "median")

    sales_classification_mode = df["Sales_Classification"].mode()[0]
    mlflow.log_param("imput-Sales_Classification", "mode")

    # Aplica os tratamentos
    coluns_tretament = {
        "Color": color_mode,
        "Fuel_Type": fuel_type_mode,
        "Transmission": transmission_mode,
        "Engine_Size_L": engine_size_l_median,
        "Mileage_KM": mileage_km_median,
        "Sales_Volume": sales_volume_median,
        "Sales_Classification": sales_classification_mode
    }
    df = df.fillna(coluns_tretament)

    mlflow_utils.log_artifact(df=df, name="null_treatment")
    return df

def _create_id(df: pd.DataFrame) -> pd.DataFrame:
    natural_keys_columns = ["Model", "Year", "Region", "Color", "Fuel_Type", "Transmission", "Engine_Size_L"]

    def _norm(x):
        if pd.isna(x):
            return ""
        x = str(x).strip().upper()
        x = re.sub(r"\s+", " ", x)
        return x

    df_norm = df[natural_keys_columns].astype(str).applymap(_norm)

    # Concatena tudo em uma string única por linha
    key_str = df_norm.agg("|".join, axis=1)

    # Hash SHA256 e pega 16 primeiros caracteres (suficiente p/ unicidade)
    df["vehicle_id"] = key_str.apply(lambda x: "veh_" + hashlib.sha256(x.encode("utf-8")).hexdigest()[:16])
    mlflow.log_param("insert-id", "sha256[:16]")
    mlflow_utils.log_artifact(df, "create_id")

    return df

def _category_transform(df: pd.DataFrame) -> pd.DataFrame:
    df_year_dummie = pd.get_dummies(df["Year"], prefix="dummie")
    mlflow.log_param("insert-Year", "dummies")

    df_region_dummie = pd.get_dummies(df["Region"], prefix="dummie")
    mlflow.log_param("insert-Region", "dummies")

    df_color_dummie = pd.get_dummies(df["Color"], prefix="dummie")
    mlflow.log_param("insert-Color", "dummies")

    df_fuel_type_dummie = pd.get_dummies(df["Fuel_Type"], prefix="dummie")
    mlflow.log_param("insert-Fuel_Type", "dummies")

    df_transmission_dummie = pd.get_dummies(df["Transmission"], prefix="dummie")
    mlflow.log_param("insert-Transmission", "dummies")

    df_sales_classification_dummie = pd.get_dummies(df["Sales_Classification"], prefix="dummie")
    mlflow.log_param("insert-Sales_Classification", "dummies")

    dfs_to_concat = [df, df_year_dummie, df_region_dummie, df_color_dummie, df_fuel_type_dummie, df_transmission_dummie, df_sales_classification_dummie]

    df = pd.concat(dfs_to_concat, axis=1)

    mlflow_utils.log_artifact(df, "transformed_dummies")

    return df

def _numeric_normalization(df: pd.DataFrame) -> pd.DataFrame:
    # StandardScaler
    columns_to_scale_std = ["Engine_Size_L"]
    scaled_std_values = StandardScaler().fit_transform(df[columns_to_scale_std])
    mlflow.log_param("normalized-Engine_Size_L", "StandardScaler")

    std_scaled_columns = [f"StandardScalerScaled_{x}" for x in columns_to_scale_std]
    df_std_scaled_values = pd.DataFrame(scaled_std_values, columns=std_scaled_columns, index=df.index)

    # MinMaxScaler
    columns_to_scale_minmax = ['Mileage_KM', 'Price_USD', 'Sales_Volume']
    scaled_minmax_values = MinMaxScaler().fit_transform(df[columns_to_scale_minmax])

    mlflow.log_param("normalized-Mileage_KM", "MinMaxScaler")
    mlflow.log_param("normalized-Price_USD", "MinMaxScaler")
    mlflow.log_param("normalized-Sales_Volume", "MinMaxScaler")

    minmax_scaled_columns = [f"MinMaxScaled_{x}" for x in columns_to_scale_minmax]
    df_minmax_scaled_values = pd.DataFrame(scaled_minmax_values, columns=minmax_scaled_columns, index=df.index)

    df = pd.concat([df, df_minmax_scaled_values, df_std_scaled_values], axis=1)

    mlflow_utils.log_artifact(df, "scaled")
    return df

def transform(run_id: str):
    with mlflow_utils.use_run(run_id=run_id):
        df_raw = parquet_utils.load_data_frame(name="raw")

        df_clear = df_raw.copy()
        df_clear = _remove_duplicates(df=df_clear)
        df_clear = _null_treatment(df=df_clear)
        df_clear = _create_id(df=df_clear)
        df_clear = _category_transform(df=df_clear)
        df_clear = _numeric_normalization(df=df_clear)

        mlflow_utils.log_artifact(df=df_clear, name="clear")
