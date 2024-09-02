"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.19.8
"""

import pandas as pd

def carga_datasets() -> pd.DataFrame:
    demografia = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_DEMO.XPT")
    colesterol = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_TCHOL.XPT")
    insulina = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_INS.XPT")

    return demografia, colesterol, insulina
