import pandas as pd
#Funciones de redondeo
def red (r:float):
    allowed_numbers = [0.2, 0.25, 0.4, 0.5, 0.6, 0.75, 0.8, 1.0, 1.2, 1.25, 1.4, 1.5, 1.6, 1.75, 1.8, 2.0,
                       2.2, 2.25, 2.4, 2.5, 2.6, 2.75, 2.8, 3.0, 3.2, 3.25, 3.4, 3.5, 3.6, 3.75, 3.8, 4.0]
    closest_number = min(allowed_numbers, key=lambda x: abs(x - r))
    return closest_number

def red3 (r:float):
    allowed_numbers = [1.0, 2.0, 3.0, 4.0]
    closest_number = min(allowed_numbers, key=lambda x: abs(x - r))
    return closest_number

def red7 (r:float):
    allowed_numbers = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]
    closest_number = min(allowed_numbers, key=lambda x: abs(x - r))
    return closest_number

#Funcion para cerrar la participación
def cerrar (df:pd.DataFrame):
    if df['PARTICIPACION'].sum() > 5.1:
        if df.shape[0] <= 3:
            df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                 ascending=False).head(1)['ITEM'].values[0]].index,
                   'PARTICIPACION'] = df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                                           ascending=False).head(1)['ITEM'].values[0]].index,
                                             'PARTICIPACION'] - 1
            df = cerrar(df)
            return df
        elif df.shape[0] <= 7:
            df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                 ascending=False).head(1)['ITEM'].values[0]].index,
                   'PARTICIPACION'] = df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                                           ascending=False).head(1)['ITEM'].values[0]].index,
                                             'PARTICIPACION'] - 0.5
            df = cerrar(df)
            return df
        else:
            df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                 ascending=False).head(1)['ITEM'].values[0]].index,
                   'PARTICIPACION'] = df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                                           ascending=False).head(1)['ITEM'].values[0]].index,
                                             'PARTICIPACION'] - 0.2
            df = cerrar(df)
            return df
    elif df['PARTICIPACION'].sum() < 4.9:
        if df.shape[0] <= 3:
            df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                 ascending=False).head(1)['ITEM'].values[0]].index,
                   'PARTICIPACION'] = df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                                           ascending=False).head(1)['ITEM'].values[0]].index,
                                             'PARTICIPACION'] + 1
            df = cerrar(df)
            return df
        elif df.shape[0] <= 7:
            df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                 ascending=False).head(1)['ITEM'].values[0]].index,
                   'PARTICIPACION'] = df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                                           ascending=False).head(1)['ITEM'].values[0]].index,
                                             'PARTICIPACION'] + 0.5
            df = cerrar(df)
            return df
        else:
            df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                 ascending=False).head(1)['ITEM'].values[0]].index,
                   'PARTICIPACION'] = df.loc[df[df['ITEM']==df.sort_values('PARTICIPACION',
                                                                           ascending=False).head(1)['ITEM'].values[0]].index,
                                             'PARTICIPACION'] + 0.2
            df = cerrar(df)
            return df
    else:
        return df

#Función para obtener la participación
def participacion (cab:pd.DataFrame):
    #Algoritmo que define la participación por puntera
    df_final = pd.DataFrame()
    for l in cab['GEOG_LOCL_DESC'].unique():
        df = cab[(cab['GEOG_LOCL_DESC']==l)].drop_duplicates()
        for p in df['PUNTERA'].unique():
            try:
                df = df[df['PUNTERA']==p].drop_duplicates()
                df['PARTICIPACION'] = np.NaN
                if df.shape[0] == 1:
                    df['PARTICIPACION'] = 5
                elif df.shape[0] <= 3:
                    t = df['DOH_CARGA_MAX'].sum()
                    df['PARTICIPACION'] = t/df['DOH_CARGA_MAX']
                    t = df['PARTICIPACION'].sum()
                    df['PARTICIPACION'] = 5*df['PARTICIPACION']/t
                    df['PARTICIPACION'] = df['PARTICIPACION'].apply(red3)
                elif df.shape[0] <= 7:
                    t = df['DOH_CARGA_MAX'].sum()
                    df['PARTICIPACION'] = t/df['DOH_CARGA_MAX']
                    t = df['PARTICIPACION'].sum()
                    df['PARTICIPACION'] = 5*df['PARTICIPACION']/t
                    df['PARTICIPACION'] = df['PARTICIPACION'].apply(red7)
                else:
                    t = df['DOH_CARGA_MAX'].sum()
                    df['PARTICIPACION'] = t/df['DOH_CARGA_MAX']
                    t = df['PARTICIPACION'].sum()
                    df['PARTICIPACION'] = 5*df['PARTICIPACION']/t
                    df['PARTICIPACION'] = df['PARTICIPACION'].apply(red)
                df = cerrar(df=df)
                df_final = pd.concat([df_final, df], ignore_index=True)
            except:
                pass
    
    #Agregamos los faltantes
    df_final['CARGA_MAX_POR_PARTICIPACION'] = df_final['PARTICIPACION']*df_final['CANT_MAX_X_ESTANTE']
    df_final['CARGA_MIN_POR_PARTICIPACION'] = df_final['PARTICIPACION']*df_final['CANT_MIN_X_ESTANTE']

    df_final = df_final[['FECHA_INICIO', 'FECHA_FIN', 'SECCION', 'GEOG_LOCL_COD', 'GEOG_LOCL_DESC',
                         'PUNTERA', 'DURACION_PUNTERA', 'GRUPO', 'CLASE', 'SUBCLASE', 'ITEM', 'ARTC_ARTC_DESC',
                         'CANT_MAX_X_ESTANTE', 'CANT_MIN_X_ESTANTE', 'VENTA_BASAL', 'ACELERACION',
                         'VENTA_ACELERADA_DIARIA', 'CARGA_SUGERIDA_X_VENTA', 'DOH_CARGA_MAX', 'DOH_CARGA_MIN',
                         'PARTICIPACION', 'CARGA_MAX_POR_PARTICIPACION', 'CARGA_MIN_POR_PARTICIPACION']]
    return df_final