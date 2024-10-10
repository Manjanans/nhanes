"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.19.8
"""
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PowerTransformer
from sklearn.impute import KNNImputer
#Funciones de carga de datos
def carga_datasets() -> pd.DataFrame:
    demografia = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_DEMO.XPT")
    colesterol = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_TCHOL.XPT")
    insulina = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_INS.XPT")
    depresion = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_DPQ.XPT")
    proteinaC = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_HSCRP.XPT")
    perfilBioquimico = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_BIOPRO.XPT")
    presionArterial = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_BPXO.XPT")
    medidasCorporales = pd.read_sas("https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/P_BMX.XPT")

    demografia = demografia.rename(columns={
        'SEQN': 'ID',
        'SDDSRVYR': 'Ciclo de liberación de datos',
        'RIDSTATR': 'Estado de entrevista/examen',
        'RIAGENDR': 'Género',
        'RIDAGEYR': 'Edad en años al momento del examen',
        'RIDAGEMN': 'Edad en meses al momento del examen - 0 a 24 meses',
        'RIDRETH1': 'Raza/Origen hispano',
        'RIDRETH3': 'Raza/Origen hispano con asiáticos no hispanos',
        'RIDEXMON': 'Período de seis meses',
        'DMDBORN4': 'País de nacimiento',
        'DMDYRUSZ': 'Tiempo en los EE.UU.',
        'DMDEDUC2': 'Nivel educativo - Adultos 20+',
        'DMDMARTZ': 'Estado civil',
        'RIDEXPRG': 'Estado de embarazo en el examen',
        'SIALANG': 'Idioma de la entrevista del participante',
        'SIAPROXY': '¿Se utilizó un apoderado en la entrevista del participante?',
        'SIAINTRP': '¿Se utilizó un intérprete en la entrevista del participante?',
        'FIALANG': 'Idioma de la entrevista familiar',
        'FIAPROXY': '¿Se utilizó un apoderado en la entrevista familiar?',
        'FIAINTRP': '¿Se utilizó un intérprete en la entrevista familiar?',
        'MIALANG': 'Idioma de la entrevista MEC',
        'MIAPROXY': '¿Se utilizó un apoderado en la entrevista MEC?',
        'MIAINTRP': '¿Se utilizó un intérprete en la entrevista MEC?',
        'AIALANGA': 'Idioma de la entrevista ACASI',
        'WTINTPRP': 'Peso de la entrevista de muestra completa',
        'WTMECPRP': 'Peso del examen MEC de muestra completa',
        'SDMVPSU': 'PSU pseudo-enmascarado para varianza',
        'SDMVSTRA': 'Estrato pseudo-enmascarado para varianza',
        'INDFMPIR': 'Relación de ingresos familiares con la pobreza'
    })

    colesterol = colesterol.rename(columns={
        'SEQN': 'ID',
        'LBXTC': 'Colesterol Total (mg/dL)',
        'LBDTCSI': 'Colesterol Total (mmol/L)'     
    })

    insulina = insulina.rename(columns={
        'SEQN': 'ID',
        'WTSAFPRP': 'Peso de Submuestra en Ayunas',
        'LBXIN': 'Insulina (μU/mL)',
        'LBDINSI': 'Insulina (pmol/L)',
        'LBDINLC': 'Código de Comentario de Insulina'
    })

    depresion = depresion.rename(columns={
        'SEQN': 'ID',
        'DPQ010': 'Poco Interés en Hacer Cosas',
        'DPQ020': 'Sentirse Deprimido o Sin Esperanza',
        'DPQ030': 'Problemas para Dormir',
        'DPQ040': 'Cansancio o Poca Energía',
        'DPQ050': 'Poco Apetito o Comer en Exceso',
        'DPQ060': 'Sentirse Mal Acerca de Uno Mismo',
        'DPQ070': 'Problemas de Concentración',
        'DPQ080': 'Movimientos o Hablar Lento o Rápido',
        'DPQ090': 'Pensamientos de Muerte o Autolesión',
        'DPQ100': 'Dificultad que Estos Problemas Causan'
    })

    proteinaC = proteinaC.rename(columns={
        'SEQN': 'ID',
        'LBXHSCRP': 'Proteína C Reactiva (mg/L)',
        'LBDHRPLC': 'Código de Comentario de Proteína C Reactiva'
    })

    perfilBioquimico = perfilBioquimico.rename(columns={
        "SEQN": "ID",
        "LBXSATSI": "Alanina Aminotransferasa (ALT) (U/L)",
        "LBDSATLC": "Código de comentario de ALT",
        "LBXSAL": "Albúmina, suero refrigerado (g/dL)",
        "LBDSALSI": "Albúmina, suero refrigerado (g/L)",
        "LBXSAPSI": "Fosfatasa Alcalina (ALP) (U/L)",
        "LBXSASSI": "Aspartato Aminotransferasa (AST) (U/L)",
        "LBXSC3SI": "Bicarbonato (mmol/L)",
        "LBXSBU": "Nitrógeno Ureico en Sangre (mg/dL)",
        "LBDSBUSI": "Nitrógeno Ureico en Sangre (mmol/L)",
        "LBXSCLSI": "Cloruro (mmol/L)",
        "LBXSCK": "Fosfoquinasa de Creatina (CPK) (IU/L)",
        "LBXSCR": "Creatinina, suero refrigerado (mg/dL)",
        "LBDSCRSI": "Creatinina, suero refrigerado (umol/L)",
        "LBXSGB": "Globulina (g/dL)",
        "LBDSGBSI": "Globulina (g/L)",
        "LBXSGL": "Glucosa, suero refrigerado (mg/dL)",
        "LBDSGLSI": "Glucosa, suero refrigerado (mmol/L)",
        "LBXSGTSI": "Gamma Glutamil Transferasa (GGT) (U/L)",
        "LBDSGTLC": "Código de comentario de GGT",
        "LBXSIR": "Hierro, suero refrigerado (ug/dL)",
        "LBDSIRSI": "Hierro, suero refrigerado (umol/L)",
        "LBXSLDSI": "Deshidrogenasa de Lactato (LDH) (U/L)",
        "LBXSOSSI": "Osmolalidad (mmol/Kg)",
        "LBXSPH": "Fósforo (mg/dL)",
        "LBDSPHSI": "Fósforo (mmol/L)",
        "LBXSKSI": "Potasio (mmol/L)",
        "LBXSNASI": "Sodio (mmol/L)",
        "LBXSTB": "Bilirrubina Total (mg/dL)",
        "LBDSTBSI": "Bilirrubina Total (umol/L)",
        "LBDSTBLC": "Código de comentario de Bilirrubina",
        "LBXSCA": "Calcio Total (mg/dL)",
        "LBDSCASI": "Calcio Total (mmol/L)",
        "LBXSCH": "Colesterol Total, suero refrigerado (mg/dL)",
        "LBDSCHSI": "Colesterol Total, suero refrigerado (mmol/L)",
        "LBXSTP": "Proteína Total (g/dL)",
        "LBDSTPSI": "Proteína Total (g/L)",
        "LBXSTR": "Triglicéridos, suero refrigerado (mg/dL)",
        "LBDSTRSI": "Triglicéridos, suero refrigerado (mmol/L)",
        "LBXSUA": "Ácido Úrico (mg/dL)",
        "LBDSUASI": "Ácido Úrico (umol/L)"
    })

    presionArterial = presionArterial.rename(columns={
        'SEQN': 'ID',
        'BPAOARM': 'Brazo seleccionado - oscilométrico',
        'BPAOCSZ': 'Tamaño del manguito codificado - oscilométrico',
        'BPXOSY1': 'Presión sistólica - 1ra lectura oscilométrica',
        'BPXODI1': 'Presión diastólica - 1ra lectura oscilométrica',
        'BPXOSY2': 'Presión sistólica - 2da lectura oscilométrica',
        'BPXODI2': 'Presión diastólica - 2da lectura oscilométrica',
        'BPXOSY3': 'Presión sistólica - 3ra lectura oscilométrica',
        'BPXODI3': 'Presión diastólica - 3ra lectura oscilométrica',
        'BPXOPLS1': 'Pulso - 1ra lectura oscilométrica',
        'BPXOPLS2': 'Pulso - 2da lectura oscilométrica',
        'BPXOPLS3': 'Pulso - 3ra lectura oscilométrica'
    })

    medidasCorporales = medidasCorporales.rename(columns={
        'SEQN': 'ID',
        'BMDSTATS': 'Código de estado del componente de medidas corporales',
        'BMXWT': 'Peso (kg)',
        'BMIWT': 'Comentario sobre el peso',
        'BMXRECUM': 'Longitud recumbente (cm)',
        'BMIRECUM': 'Comentario sobre la longitud recumbente',
        'BMXHEAD': 'Circunferencia de la cabeza (cm)',
        'BMIHEAD': 'Comentario sobre la circunferencia de la cabeza',
        'BMXHT': 'Altura de pie (cm)',
        'BMIHT': 'Comentario sobre la altura de pie',
        'BMXBMI': 'Índice de masa corporal (kg/m²)',
        'BMDBMIC': 'Categoría de IMC - Niños/Jóvenes',
        'BMXLEG': 'Longitud del muslo (cm)',
        'BMILEG': 'Comentario sobre la longitud del muslo',
        'BMXARML': 'Longitud del brazo superior (cm)',
        'BMIARML': 'Comentario sobre la longitud del brazo superior',
        'BMXARMC': 'Circunferencia del brazo (cm)',
        'BMIARMC': 'Comentario sobre la circunferencia del brazo',
        'BMXWAIST': 'Circunferencia de la cintura (cm)',
        'BMIWAIST': 'Comentario sobre la circunferencia de la cintura',
        'BMXHIP': 'Circunferencia de la cadera (cm)',
        'BMIHIP': 'Comentario sobre la circunferencia de la cadera'
    })

    return demografia, colesterol, insulina, depresion, proteinaC, perfilBioquimico, presionArterial, medidasCorporales

def demografia_edad(demografia_imp:pd.DataFrame)->pd.DataFrame:
    datos = demografia_imp[['Edad en años al momento del examen']]
    datos.loc[datos['Edad en años al momento del examen']<=5] = 0
    datos.loc[(datos['Edad en años al momento del examen']>0) & (datos['Edad en años al momento del examen']<=15)] = 1
    datos.loc[(datos['Edad en años al momento del examen']>1) & (datos['Edad en años al momento del examen']<=45)] = 2
    datos.loc[(datos['Edad en años al momento del examen']>2) & (datos['Edad en años al momento del examen']<=65)] = 3
    datos.loc[datos['Edad en años al momento del examen']>3] = 4
    return datos

def demografia_pobreza(demografia_imp:pd.DataFrame)->pd.DataFrame:
    datos = demografia_imp[['Relación de ingresos familiares con la pobreza']]
    datos.loc[datos['Relación de ingresos familiares con la pobreza']<=0.8] = 0
    datos.loc[(datos['Relación de ingresos familiares con la pobreza']>0) & (datos['Relación de ingresos familiares con la pobreza']<=1.5)] = 1
    datos.loc[(datos['Relación de ingresos familiares con la pobreza']>1) & (datos['Relación de ingresos familiares con la pobreza']<=3)] = 2
    datos.loc[(datos['Relación de ingresos familiares con la pobreza']>2) & (datos['Relación de ingresos familiares con la pobreza']<=4.9)] = 3
    datos.loc[(datos['Relación de ingresos familiares con la pobreza']>3) & (datos['Relación de ingresos familiares con la pobreza']<80)] = 4
    return datos

def demografia_completa(demografia:pd.DataFrame)->pd.DataFrame:
    knn_imputer = KNNImputer(n_neighbors=10, weights='uniform')
    demografia_imp = pd.DataFrame(knn_imputer.fit_transform(demografia), columns=demografia.columns)
    pobreza = demografia_pobreza(demografia_imp)
    edad = demografia_edad(demografia_imp)
    demografia_clean = pd.DataFrame()
    demografia_clean['ID'] = demografia[['ID']]
    demografia_clean['Nivel Pobreza'] = pobreza
    demografia_clean['Edad'] = edad
    demografia_clean['Sexo'] = demografia[['Género']]
    return demografia_clean

