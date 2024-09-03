"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.19.8
"""

import pandas as pd
#Funciones de carga de datos
def carga_datasets() -> pd.DataFrame:
    demografia = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_DEMO.XPT")
    colesterol = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_TCHOL.XPT")
    insulina = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_INS.XPT")
    depresion = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_DPQ.XPT")
    proteinaC = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_HSCRP.XPT")

    return demografia, colesterol, insulina, depresion, proteinaC
