import pandas as pd
import numpy as np
from utils import descargar_segmento

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

def participacion (cursor, punt:pd.DataFrame, fecha_inicio:str):
    #Descargamos df
    dim = descargar_segmento(cursor=cursor, query='Dimensiones',
                             cond=f" and ip.fecha_inicio = '{fecha_inicio}';")
    ac = descargar_segmento(cursor=cursor, query='Aceleraciones').astype('str').replace('nan', pd.NA)

    #Cambiamos formato de columnas
    dim = dim.astype({'ITEM':'str', 'LOCAL':'str', 'ITEMS_POR_FRENTE':'int64',
                      'ITEMS_POR_LATERAL':'int64', 'CANT_MAX_X_ESTANTE':'int64', 'CANT_MIN_X_ESTANTE':'int64'})
    ac = ac.astype({'ACELERACION_ART':'float64', 'ACELERACION_SUBCLASE':'float64', 'ACELERACION_CLASE':'float64'})
    
    #Armamos df principal
    cab = dim.merge(ac[['LOCAL', 'ITEM', 'ACELERACION_ART']],
                    how='left').merge(ac[['LOCAL', 'SUBCLASE', 'ACELERACION_SUBCLASE']],
                                      how='left').merge(ac[['LOCAL', 'CLASE', 'ACELERACION_CLASE']],
                                                        how='left').drop_duplicates().reset_index(drop=True)
    #Definimos aceleración
    cab['ACELERACION'] = cab['ACELERACION_ART']
    cab.loc[cab[cab['ACELERACION'].isna()].index,
            'ACELERACION'] = cab.loc[cab[cab['ACELERACION'].isna()].index, 'ACELERACION_SUBCLASE']
    cab.loc[cab[cab['ACELERACION'].isna()].index,
            'ACELERACION'] = cab.loc[cab[cab['ACELERACION'].isna()].index, 'ACELERACION_CLASE']
    
    #Donde no tengamos dato de aceleración o donde esta sea menor a 1 asumimos que no se acelera
    cab.fillna({'ACELERACION':1}, inplace=True)
    cab.loc[cab[cab['ACELERACION']<1].index, 'ACELERACION'] = 1

    #Definimos unidades diarias y DOH
    cab['UNIDADES_DIARIAS_POR_ACELERACION'] = cab['AVG_BASAL_180']*cab['ACELERACION']
    cab['DOH_ESTANTE_MIN'] = round(cab['CANT_MIN_X_ESTANTE']/cab['UNIDADES_DIARIAS_POR_ACELERACION'])
    cab['DOH_ESTANTE_MAX'] = round(cab['CANT_MAX_X_ESTANTE']/cab['UNIDADES_DIARIAS_POR_ACELERACION'])

    #Limpiamos un poco el df de cabeceras
    cab.drop(index=cab[~cab['ITEM'].isin(punt['ITEM'].unique())].index, inplace=True)

    #Tomamos columnas que interesan
    cab = cab[['LOCAL', 'NOMBRE_TIENDA', 'GRUPO', 'CLASE', 'SUBCLASE', 'ITEM', 'ARTC_ARTC_DESC', 'AVG_BASAL_180',
               'ACELERACION', 'UNIDADES_DIARIAS_POR_ACELERACION', 'PROFUNDIDAD', 'FRENTE', 'ALTURA',
               'CANT_MAX_X_ESTANTE', 'CANT_MIN_X_ESTANTE', 'DOH_ESTANTE_MIN', 'DOH_ESTANTE_MAX']]

    #Algoritmo que define la participación por puntera
    df_final = pd.DataFrame()
    for l in punt['LOCAL'].unique():
        df = punt[(punt['LOCAL']==l)].drop_duplicates()
        for p in df['PUNTERA'].unique():
            try:
                df = punt[(punt['LOCAL']==l) &
                          (punt['PUNTERA']==p)].drop(columns='DESCRIPCION').merge(cab).drop_duplicates()
                df['PARTICIPACION'] = np.NaN
                if df[df['DOH_ESTANTE_MAX'] <= 31].shape[0] >= 5:
                    #print('ESTANTES INSUFICIENTES')
                    if df.shape[0] >= 7:
                        it_may_vta = df.sort_values('DOH_ESTANTE_MAX').head(3)['ITEM'].values
                        it_h_vta = df.sort_values('DOH_ESTANTE_MAX').head()['ITEM'].values
                        df.loc[df[df['ITEM'].isin(it_may_vta)].index, 'PARTICIPACION'] = 1
                        df.loc[df[(~df['ITEM'].isin(it_may_vta)) &
                                    (df['ITEM'].isin(it_h_vta))].index, 'PARTICIPACION'] = 0.5
                        df.loc[df[~df['ITEM'].isin(it_h_vta)].index,
                                'PARTICIPACION'] = 1/df[~df['ITEM'].isin(it_h_vta)].shape[0]
                    elif df.shape[0] == 6:
                        it_may_vta = df.sort_values('DOH_ESTANTE_MAX').head(4)['ITEM'].values
                        df.loc[df[df['ITEM'].isin(it_may_vta)].index, 'PARTICIPACION'] = 1
                        df.loc[df[(~df['ITEM'].isin(it_may_vta))].index, 'PARTICIPACION'] = 0.5
                    else:
                        df.loc[df.index, 'PARTICIPACION'] = 1
                elif df.shape[0] == 1:
                    df['PARTICIPACION'] = 5
                elif df.shape[0] <= 3:
                    t = df['DOH_ESTANTE_MAX'].sum()
                    df['PARTICIPACION'] = t/df['DOH_ESTANTE_MAX']
                    t = df['PARTICIPACION'].sum()
                    df['PARTICIPACION'] = 5*df['PARTICIPACION']/t
                    df['PARTICIPACION'] = df['PARTICIPACION'].apply(red3)
                elif df.shape[0] <= 7:
                    t = df['DOH_ESTANTE_MAX'].sum()
                    df['PARTICIPACION'] = t/df['DOH_ESTANTE_MAX']
                    t = df['PARTICIPACION'].sum()
                    df['PARTICIPACION'] = 5*df['PARTICIPACION']/t
                    df['PARTICIPACION'] = df['PARTICIPACION'].apply(red7)
                else:
                    t = df['DOH_ESTANTE_MAX'].sum()
                    df['PARTICIPACION'] = t/df['DOH_ESTANTE_MAX']
                    t = df['PARTICIPACION'].sum()
                    df['PARTICIPACION'] = 5*df['PARTICIPACION']/t
                    df['PARTICIPACION'] = df['PARTICIPACION'].apply(red)
                df = cerrar(df=df)
                df_final = pd.concat([df_final, df], ignore_index=True)
            except:
                pass
    
    #Vemos los items que no teniamos datos de aceleración/dimensiones
    df_final['DURACION'] = (pd.to_datetime(df_final['FECHA_FIN']) -
                            pd.to_datetime(df_final['FECHA_INICIO'])).mean().days + 1
    #Agregamos los faltantes
    df_final['UNIDADES_CARGA_POR_VENTA'] = df_final['DURACION']*df_final['UNIDADES_DIARIAS_POR_ACELERACION']
    df_final['UNIDADES_CARGA_MAX_POR_PARTICIPACION'] = df_final['PARTICIPACION']*df_final['CANT_MAX_X_ESTANTE']
    df_final['UNIDADES_CARGA_MIN_POR_PARTICIPACION'] = df_final['PARTICIPACION']*df_final['CANT_MIN_X_ESTANTE']

    df_final = df_final[['FECHA_INICIO', 'FECHA_FIN', 'SECCION', 'LOCAL', 'NOMBRE_TIENDA', 'PUNTERA',
                         'GRUPO', 'CLASE', 'SUBCLASE', 'ESTADISTICO', 'ITEM', 'ARTC_ARTC_DESC',
                         'AVG_BASAL_180', 'ACELERACION', 'UNIDADES_DIARIAS_POR_ACELERACION',
                         'PROFUNDIDAD', 'FRENTE', 'ALTURA', 'CANT_MAX_X_ESTANTE', 'CANT_MIN_X_ESTANTE',
                         'DOH_ESTANTE_MIN', 'DOH_ESTANTE_MAX', 'PARTICIPACION', 'DURACION', 'UNIDADES_CARGA_POR_VENTA',
                         'UNIDADES_CARGA_MAX_POR_PARTICIPACION', 'UNIDADES_CARGA_MIN_POR_PARTICIPACION']]
    return df_final