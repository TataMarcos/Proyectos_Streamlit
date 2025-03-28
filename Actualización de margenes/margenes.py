import pandas as pd

def margenes (df_actual:pd.DataFrame, df_update:pd.DataFrame):
    #Descargamos df
    mg_nuevo = df_actual.merge(df_update, how='outer')
    mg_nuevo.loc[mg_nuevo[~mg_nuevo['MARGEN OBJETIVO'].isna()].index,
                 'MG'] = mg_nuevo.loc[mg_nuevo[~mg_nuevo['MARGEN OBJETIVO'].isna()].index,
                                      'MARGEN OBJETIVO']
    return mg_nuevo