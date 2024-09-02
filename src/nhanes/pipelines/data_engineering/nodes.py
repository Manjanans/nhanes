"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.19.8
"""

import pandas as pd

def carga_demografica() -> pd.DataFrame:
    nhanes_datos = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_DEMO.XPT")

    return nhanes_datos
