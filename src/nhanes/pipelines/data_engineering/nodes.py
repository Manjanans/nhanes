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
    """
    This function takes no arguments.
    First node in this pipeline.
    This function returns multiple Pandas DataFrames. These dataframes will be taken as raw data for further processing.
    Said dataframes are:
    demografia: Demographic Variables and Sample Weights (P_DEMO),
    colesterol: Cholesterol - Total (P_TCHOL),
    insulina: Insulin (P_INS),
    depresion: Mental Health - Depression Screener (P_DPQ),
    proteinaC: High-Sensitivity C-Reactive Protein (P_HSCRP),
    perfilBioquimico: Standard Biochemistry Profile (P_BIOPRO),
    presionArterial: Blood Pressure - Oscillometric Measurement (P_BPXO),
    medidasCorporales: Body Measures (P_BMX).
    """
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

    presion = presionArterial.rename(columns={
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

    presionArterial = presion.drop(labels='Brazo seleccionado - oscilométrico', axis=1)


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

def imputacion(dataframe:pd.DataFrame) -> pd.DataFrame:
    """
    This function does the following:

    1. Imputes missing values using KNN
    2. Returns imputed dataframe

    Args:
        dataframe (pd.DataFrame): DataFrame to impute

    Returns:
        pd.DataFrame: Imputed dataframe
    """
    knn_imputer = KNNImputer(n_neighbors=10, weights='uniform')
    imputado = pd.DataFrame(knn_imputer.fit_transform(dataframe), columns=dataframe.columns)
    return imputado

def multicolumn_IQR(dataframe:pd.DataFrame) -> pd.DataFrame:
    """
    This function does the following:

    1. Copies the ID, age, sex and Income columns
    2. Calculates the IQR for each column
    3. Filter out the outliers for the current column only
    4. Merge filtered column back into a DataFrame, using an 'ID' column

    Args:
        dataframe (pd.DataFrame): DataFrame to perform IQR

    Returns:
        pd.DataFrame: DataFrame with outliers removed
    """
    dato = dataframe[['ID','Edad','Sexo','Nivel Pobreza']].copy()
    for columna in dataframe.columns[4:]:
        Q1 = dataframe[columna].quantile(0.25)
        Q3 = dataframe[columna].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        # Filter out the outliers for the current column only
        col_limpia = dataframe[~((dataframe[columna] < lower_bound) | (dataframe[columna] > upper_bound))][['ID', columna]]
        # Merge filtered column back into the 'dato' DataFrame on the 'ID' column
        dato = dato.merge(col_limpia, on='ID', how='left')
    return dato

def multicolumn_scale(dataframe:pd.DataFrame) -> pd.DataFrame:
    """
    This function does the following:

    1. Copies the ID, age, sex and Income columns
    2. Applies StandardScaler to each column
    3. Merge scaled columns back into a DataFrame, using an 'ID' column

    Args:
        dataframe (pd.DataFrame): DataFrame to perform scaling

    Returns:
        pd.DataFrame: DataFrame with scaled columns
    """
    scaler = StandardScaler()
    dato = dataframe[['ID','Edad','Sexo','Nivel Pobreza']].copy()
    for columna in dataframe.columns[4:]:
        col_limpia = pd.DataFrame()
        col_limpia['ID'] = dataframe[['ID']].copy()
        col_limpia[columna] = scaler.fit_transform(dataframe[[columna]])
        dato = dato.merge(col_limpia, on='ID', how='left')
    return dato

def multicolumn_minmax(dataframe:pd.DataFrame) -> pd.DataFrame:
    """
    This function does the following:

    1. Copies the ID, age, sex and Income columns
    2. Applies MinMaxScaler to each column
    3. Merge scaled columns back into a DataFrame, using an 'ID' column

    Args:
        dataframe (pd.DataFrame): DataFrame to perform scaling

    Returns:
        pd.DataFrame: DataFrame with scaled columns
    """
    minmax = MinMaxScaler()
    dato = dataframe[['ID','Edad','Sexo','Nivel Pobreza']].copy()
    for columna in dataframe.columns[4:]:
        col_limpia = pd.DataFrame()
        col_limpia['ID'] = dataframe[['ID']].copy()
        col_limpia[columna] = minmax.fit_transform(dataframe[[columna]])
        dato = dato.merge(col_limpia, on='ID', how='left')
    return dato

def multicolumn_PowerTransformer(dataframe:pd.DataFrame) -> pd.DataFrame:
    """
    This function does the following:

    1. Copies the ID, age, sex and Income columns
    2. Applies PowerTransformer to each column
    3. Merge scaled columns back into a DataFrame, using an 'ID' column

    Args:
        dataframe (pd.DataFrame): DataFrame to perform scaling

    Returns:
        pd.DataFrame: DataFrame with scaled columns
    """

    pt = PowerTransformer(method='yeo-johnson')
    dato = dataframe[['ID','Edad','Sexo','Nivel Pobreza']].copy()
    for columna in dataframe.columns[4:]:
        col_limpia = pd.DataFrame()
        col_limpia['ID'] = dataframe[['ID']].copy()
        col_limpia[columna] = pt.fit_transform(dataframe[[columna]])
        dato = dato.merge(col_limpia, on='ID', how='left')
    return dato

def demografia_edad(demografia_imp:pd.DataFrame)->pd.DataFrame:
    """
    This function does the following:

    1. Categorizes the age in 5-year groups:
        0: 0-5
        1: 6-15
        2: 16-45
        3: 46-65
        4: 65+

    2. Returns the dataframe with the age in 5-year groups.

    Args:
        demografia_imp (pd.DataFrame): Demographic data

    Returns:
        pd.DataFrame: Demographic data with the age in 5-year groups
    """
    datos = demografia_imp[['Edad en años al momento del examen']]
    datos.loc[datos['Edad en años al momento del examen']<=5] = 0
    datos.loc[(datos['Edad en años al momento del examen']>0) & (datos['Edad en años al momento del examen']<=15)] = 1
    datos.loc[(datos['Edad en años al momento del examen']>1) & (datos['Edad en años al momento del examen']<=45)] = 2
    datos.loc[(datos['Edad en años al momento del examen']>2) & (datos['Edad en años al momento del examen']<=65)] = 3
    datos.loc[datos['Edad en años al momento del examen']>3] = 4
    return datos

def demografia_pobreza(demografia_imp:pd.DataFrame)->pd.DataFrame:
    """
    This function does the following:

    1. Categorizes the income in 5 groups:
        0: <= 0.8
        1: >0.8 and <= 1.5
        2: >1.5 and <= 3
        3: >3 and <= 4.9
        4: >4.9

    2. Returns the dataframe with the income in 5 groups.

    Args:
        demografia_imp (pd.DataFrame): Demographic data

    Returns:
        pd.DataFrame: Demographic data with the income in 5-year groups
    """
    datos = demografia_imp[['Relación de ingresos familiares con la pobreza']]
    datos.loc[datos['Relación de ingresos familiares con la pobreza']<=0.8] = 0
    datos.loc[(datos['Relación de ingresos familiares con la pobreza']>0) & (datos['Relación de ingresos familiares con la pobreza']<=1.5)] = 1
    datos.loc[(datos['Relación de ingresos familiares con la pobreza']>1) & (datos['Relación de ingresos familiares con la pobreza']<=3)] = 2
    datos.loc[(datos['Relación de ingresos familiares con la pobreza']>2) & (datos['Relación de ingresos familiares con la pobreza']<=4.9)] = 3
    datos.loc[(datos['Relación de ingresos familiares con la pobreza']>3) & (datos['Relación de ingresos familiares con la pobreza']<80)] = 4
    return datos

def demografia_completa(demografia:pd.DataFrame)->pd.DataFrame:
    """
    This function does the following:

    1. Imputes missing values using KNN
    2. Categorizes the income using demografia_pobreza.
    3. Categorizes the age using demografia_edad.
    4. A new dataframe is created, using this columns:
        'ID': ID number; 
        'Nivel Pobreza': Income in 5 groups, specified in demografia_pobreza; 
        'Edad': Age in 5 groups, specified in demografia_edad;
        'Sexo': Gender: 0 for women, 1 for men
    5. Returns dataframe created in 4.

    Args:
        demografia (pd.DataFrame): Demographic data

    Returns:
        pd.DataFrame: Demographic data with the income in 5-year groups and the age in 5-year groups
    """
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

def demografia_insulina(demografia_limpia:pd.DataFrame, insulina:pd.DataFrame)->pd.DataFrame:
    """
    This function does the following:

    1. Merges demographic data with insulina data
    2. A new dataframe is created, using this columns:
        'ID': ID number; 
        'Nivel Pobreza': Income in 5 groups, specified in demografia_pobreza; 
        'Edad': Age in 5 groups, specified in demografia_edad;
        'Sexo': Gender: 0 for women, 1 for men
        'Nivel Insulina': Insulina data
    3. Imputes missing values using KNN
    4. Deletion of outliers
    5. Data transformation using standard scaler and power transformer
    6. Returns clean dataframe.

    Args:
        demografia_limpia (pd.DataFrame): Demographic data
        insulina (pd.DataFrame): Insulin data

    Returns:
        pd.DataFrame: Demographic data with the income in 5-year groups and the age in 5-year groups
    """
    #Creation of scaler and transformer
    scaler = StandardScaler()
    pt = PowerTransformer(method='yeo-johnson')
    #Merging of demographic and insulina data
    demografia_insulina = pd.merge(demografia_limpia, insulina, on='ID', how='inner')
    #Creation of dataframe
    insulina_muestra = pd.DataFrame()
    insulina_muestra['ID'] = demografia_insulina['ID']
    insulina_muestra['Edad'] = demografia_insulina['Edad']
    insulina_muestra['Sexo'] = demografia_insulina['Sexo']
    insulina_muestra['Nivel Pobreza'] = demografia_insulina['Nivel Pobreza']
    insulina_muestra['Nivel Insulina'] = demografia_insulina['Insulina (μU/mL)']
    #Imputation of missing values
    knn_imputer = KNNImputer(n_neighbors=10, weights='uniform')
    insulina_imputada = pd.DataFrame(knn_imputer.fit_transform(insulina_muestra), columns=insulina_muestra.columns)
    #IQR for outliers
    Q1 = insulina_imputada["Nivel Insulina"].quantile(0.25)
    Q3 = insulina_imputada["Nivel Insulina"].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    insulina_clean = pd.DataFrame((insulina_imputada[~((insulina_imputada["Nivel Insulina"] < lower_bound) | (insulina_imputada["Nivel Insulina"] > upper_bound))]),columns=insulina_imputada.columns)
    #Transformation of insulina_clean data
    insulina_clean["Nivel Insulina"] = scaler.fit_transform(insulina_clean[["Nivel Insulina"]])
    insulina_clean["Nivel Insulina"] = pt.fit_transform(insulina_clean[["Nivel Insulina"]])
    return insulina_clean

def demografia_perfil_bioquimico(demografia_limpia:pd.DataFrame, perfilB:pd.DataFrame)->pd.DataFrame:
    """
    This function does the following:

    1. Merges the demographic data and the biochemistry data
    2. Creation of a new dataframe with the following columns:
        'ID': ID number; 
        'Nivel Pobreza': Income in 5 groups, specified in demografia_pobreza; 
        'Edad': Age in 5 groups, specified in demografia_edad;
        'Sexo': Gender: 0 for women, 1 for men
        'Alanina  ALT (U/L)': Alanine Aminotransferase
        'Albúmina suero (g/L)': Serum Albumin
        'Fosfata ALP (U/L)': Alanine Aminotransferase
        'Aspartato  AST (U/L)': Aspartate Aminotransferase
        'Bicarbonato (mmol/L)': Total Bicarbonate
        'Nitrógeno Ureico en Sangre (mmol/L)': Urea Nitrogen
        'Cloruro (mmol/L)': Total Chloride
        'Cloruro (mmol/L)'] = combinacion['Cloruro (mmol/L)']
        'Fosfoquinasa Creatina (IU/L)': Creatinine Phosphokinase 
        'Creatinina, suero refrigerado (umol/L)': Refrigerated Serum Creatinine
        'Globulina (g/L)': Globulin
        'Glucosa (mmol/L)'/L)': Total Glucose
        'Gamma Glutamil Transferasa (U/L)' : Gamma Glutamil Transferase
        'Hierro (umol/L)'/L)': Total Iron
        'Deshidrogenasa de Lactato (U/L)': Lactate Dehydrogenase
        'Osmolalidad (mmol/Kg)': Osmolality
        'Fósforo (mmol/L)': Total Phosphorus
        'Potasio (mmol/L)': Potassium
        'Sodio (mmol/L)': Sodium
        'Bilirrubina Total (umol/L)': Total Bilirubin
        'Calcio Total (mmol/L)': Total Calcium
        'Colesterol Total (mmol/L)': Total Cholesterol
        'Proteína Total (g/L)': Total Protein
        'Triglicéridos (mmol/L)': Total Triglycerides
        'Ácido Úrico (umol/L)': Uric Acid
    3. Imputation of missing values in raw data
    4. Deletion of outliers in raw data
    5. Imputates data again, in case of deletion
    6. Deletion of outliers, in clean data
    7. Imputates data again, in case of deletion of clean data
    8. Returns the clean dataframe from 6.

    Args:
        demografia_limpia (pd.DataFrame): Demographic data
        perfilB (pd.DataFrame): Biochemistry data

    Returns:
        pd.DataFrame: Dataframe with outliers removed
    """
    #Merging of demographic and biochemistry data
    combinacion = pd.merge(demografia_limpia, perfilB, on='ID', how='inner')
    #Creation of dataframe
    perfilBioquimico = pd.DataFrame()
    perfilBioquimico['ID'] = combinacion['ID']
    perfilBioquimico['Edad'] = combinacion['Edad']
    perfilBioquimico['Sexo'] = combinacion['Sexo']
    perfilBioquimico['Nivel Pobreza'] = combinacion['Nivel Pobreza']
    perfilBioquimico['Alanina ALT (U/L)'] = combinacion['Alanina Aminotransferasa (ALT) (U/L)']
    perfilBioquimico['Albúmina suero (g/L)'] = combinacion['Albúmina, suero refrigerado (g/L)']
    perfilBioquimico['Fosfata ALP (U/L)'] = combinacion['Fosfatasa Alcalina (ALP) (U/L)']
    perfilBioquimico['Aspartato AST(U/L)'] = combinacion['Aspartato Aminotransferasa (AST) (U/L)']
    perfilBioquimico['Bicarbonato (mmol/L)'] = combinacion['Bicarbonato (mmol/L)']
    perfilBioquimico['Nitrógeno Ureico en Sangre (mmol/L)'] = combinacion['Nitrógeno Ureico en Sangre (mmol/L)']
    perfilBioquimico['Cloruro (mmol/L)'] = combinacion['Cloruro (mmol/L)']
    perfilBioquimico['Fosfoquinasa Creatina (IU/L)'] = combinacion['Fosfoquinasa de Creatina (CPK) (IU/L)']
    perfilBioquimico['Creatinina, suero refrigerado (umol/L)'] = combinacion['Creatinina, suero refrigerado (umol/L)']
    perfilBioquimico['Globulina (g/L)'] = combinacion['Globulina (g/L)']
    perfilBioquimico['Glucosa (mmol/L)'] = combinacion['Glucosa, suero refrigerado (mmol/L)']
    perfilBioquimico['Gamma Glutamil Transferasa (U/L)'] = combinacion['Gamma Glutamil Transferasa (GGT) (U/L)']
    perfilBioquimico['Hierro (umol/L)'] = combinacion['Hierro, suero refrigerado (umol/L)']
    perfilBioquimico['Deshidrogenasa de Lactato (U/L)'] = combinacion['Deshidrogenasa de Lactato (LDH) (U/L)']
    perfilBioquimico['Osmolalidad (mmol/Kg)'] = combinacion['Osmolalidad (mmol/Kg)']
    perfilBioquimico['Fósforo (mmol/L)'] = combinacion['Fósforo (mmol/L)']
    perfilBioquimico['Potasio (mmol/L)'] = combinacion['Potasio (mmol/L)']
    perfilBioquimico['Sodio (mmol/L)'] = combinacion['Sodio (mmol/L)']
    perfilBioquimico['Bilirrubina Total (umol/L)'] = combinacion['Bilirrubina Total (umol/L)']
    perfilBioquimico['Calcio Total (mmol/L)'] = combinacion['Calcio Total (mmol/L)']
    perfilBioquimico['Colesterol Total (mmol/L)'] = combinacion['Colesterol Total, suero refrigerado (mmol/L)']
    perfilBioquimico['Proteína Total (g/L)'] = combinacion['Proteína Total (g/L)']
    perfilBioquimico['Triglicéridos (mmol/L)'] = combinacion['Triglicéridos, suero refrigerado (mmol/L)']
    perfilBioquimico['Ácido Úrico (umol/L)'] = combinacion['Ácido Úrico (umol/L)']
    #Imputation
    perfil_limpio = imputacion(perfilBioquimico)
    #IQR
    primer_IQR = multicolumn_IQR(perfil_limpio)
    #Imputation
    perfil_imputado = imputacion(primer_IQR)
    #IQR
    segundo_IQR = multicolumn_IQR(perfil_imputado)
    perfil_clean = imputacion(segundo_IQR)
    return perfil_clean

def demografia_medidas_corporales(demografia_limpia:pd.DataFrame, medidas_corporales:pd.DataFrame) -> pd.DataFrame:
    """
    This function does the following:

    1. Merges demography and body measurements dataframes.
    2. Creates a new dataframe, with the following columns:
        'ID': ID number;
        'Edad': Age;
        'Sexo': Gender: 0 for women, 1 for men;
        'Nivel Pobreza': Income in 5 groups, specified in demografia_pobreza;
        'Peso (kg)': Weight in kg;
        'Altura (cm)': Height in cm;
        'IMC (kg/m²)': Index of Body Mass; 
        'Longitud muslo (cm)': Thigh length in cm;
        'Longitud brazo superior (cm)': Upper arm length in cm; 
        'Circunferencia brazo (cm)': Arm circumference in cm;
        'Circunferencia cintura (cm)': Waist circumference in cm;
        'Circunferencia cadera (cm)': Hip circumference in cm;
    3. Cleans the merged dataframe.
    4. Returns a clean dataframe.

    Args:
        demografia_limpia (pd.DataFrame): Demographic data
        medidas_corporales (pd.DataFrame): Body measurements data

    Returns:
        pd.DataFrame: Dataframe Power Transformed
    """

    #Merge
    body_measures = pd.merge(demografia_limpia, medidas_corporales, on='ID', how='inner')

    #Creation of dataframe
    corporal = pd.DataFrame()
    corporal['ID'] = body_measures['ID']
    corporal['Edad'] = body_measures['Edad']
    corporal['Sexo'] = body_measures['Sexo']
    corporal['Nivel Pobreza'] = body_measures['Nivel Pobreza']
    corporal['Peso (kg)'] = body_measures['Peso (kg)']
    corporal['Altura (cm)'] = body_measures['Altura de pie (cm)']
    corporal['IMC (kg/m²)'] = body_measures['Índice de masa corporal (kg/m²)']
    corporal['Longitud muslo (cm)'] = body_measures['Longitud del muslo (cm)']
    corporal['Longitud brazo superior (cm)'] = body_measures['Longitud del brazo superior (cm)']
    corporal['Circunferencia brazo (cm)'] = body_measures['Circunferencia del brazo (cm)']
    corporal['Circunferencia cintura (cm)'] = body_measures['Circunferencia de la cintura (cm)']
    corporal['Circunferencia cadera (cm)'] = body_measures['Circunferencia de la cadera (cm)']

    #Imputation
    medidas = imputacion(corporal)

    #Power Transformed
    medidas_corporales_clean = multicolumn_PowerTransformer(medidas)
    return medidas_corporales_clean

def demografia_presion_arterial(demografia_limpia:pd.DataFrame, presion_arterial:pd.DataFrame) -> pd.DataFrame:
    """
    This function does the following:

    1. Merges demography and blood pressure dataframes.
    2. Creates a new dataframe, with the following columns:
        'ID': ID number;
        'Edad': Age;
        'Sexo': Gender: 0 for women, 1 for men;
        'Nivel Pobreza': Income in 5 groups, specified in demografia_pobreza;
        'Tamaño del manguito codificado - oscilométrico': Coded cuff size - oscillometric;
        'Presión sistólica - 1ra lectura oscilométrica': Systolic blood pressure - 1st reading;
        'Presion diastólica - 1ra lectura oscilométrica': Diastolic blood pressure - 1st reading;
        'Presión sistólica - 2da lectura oscilométrica': Systolic blood pressure - 2nd reading;
        'Presion diastólica - 2da lectura oscilométrica': Diastolic blood pressure - 2nd reading;
        'Presión sistólica - 3ra lectura oscilométrica': Systolic blood pressure - 3rd reading;
        'Presion diastólica - 3ra lectura oscilométrica': Diastolic blood pressure - 3rd reading;
        'Pulso - 1ra lectura oscilométrica': Pulse - 1st reading;
        'Pulso - 2da lectura oscilométrica': Pulse - 2nd reading;
        'Pulso - 3ra lectura oscilométrica': Pulse - 3rd reading;
    3. Imputates the merged dataframe.
    4. Makes an IQR filter on the merged dataframe.
    5. Drops rows with missing values.
    6. Applies the PowerTransformer on the merged dataframe.
    7. Returns a clean dataframe.

    Args:
        demografia_limpia (pd.DataFrame): Demographic data
        presion_arterial (pd.DataFrame): Blood pressure data

    Returns:
        pd.DataFrame: Dataframe Power Transformed
    
    """
    #Merge
    presion = pd.merge(demografia_limpia, presion_arterial, on='ID', how='inner')

    #Imputation
    presion_imp = imputacion(presion)

    #IQR Filter
    presion_IQR = multicolumn_IQR(presion_imp)

    #Drop rows with missing values
    presion_dropna = presion_IQR.dropna()

    #Power Transformed
    presion_clean = multicolumn_PowerTransformer(presion_dropna)
    return presion_clean

def intermediate_data(demografia_limpia:pd.DataFrame, insulina:pd.DataFrame, perfilB:pd.DataFrame, medida_corporales:pd.DataFrame, presion_arterial:pd.DataFrame) -> pd.DataFrame:
    """
    This function does the following:
        1. Takes demography, insulina, perfilB, medida_corporales and presion_arterial dataframes.
        2. Merges them, using each function previously made.
        3. Returns each dataframe cleaned and ready to be worked with.

    Args:
        demografia_limpia (pd.DataFrame): Demographic data
        insulina (pd.DataFrame): Insulin data
        perfilB (pd.DataFrame): Blood profile data
        medida_corporales (pd.DataFrame): Body measurements data
        presion_arterial (pd.DataFrame): Blood pressure data

    Returns:
        pd.DataFrame: Clean dataframes

    """
    insulina_clean = demografia_insulina(demografia_limpia, insulina)
    perfilB_clean = demografia_perfil_bioquimico(demografia_limpia, perfilB)
    medidas_corporales_clean = demografia_medidas_corporales(demografia_limpia, medida_corporales)
    presion_clean = demografia_presion_arterial(demografia_limpia, presion_arterial)

    return insulina_clean, perfilB_clean, medidas_corporales_clean, presion_clean
