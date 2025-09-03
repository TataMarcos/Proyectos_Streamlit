import pandas as pd
from datetime import date
import calendar
from utils import snowflake_login, carga_snow_generic, descargar_segmento
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import re
import streamlit as st
import numpy as np

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

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

# keyboard.press_and_release('ctrl+w')        #Close the window

credentials = Credentials.from_service_account_file('api_credentials_2.json', scopes=scopes)

gc = gspread.authorize(credentials)
gauth = GoogleAuth()
drive = GoogleDrive(gauth)

#Realizamos conexión a snowflake
try:
    if 'snow' not in st.session_state:
        user, cursor, snow = snowflake_login()
        st.session_state.user = user
        st.session_state.cursor = cursor
        st.session_state.snow = snow
    else:
        snow = st.session_state.snow  # Reuse the existing Snowflake session
        user = st.session_state.user
        cursor = st.session_state.cursor
except:
    st.write('Aún no se ingresaron las credenciales')
    st.stop()

def renombrar_columnas(col_name):
    if re.search(r'EXHIBI', col_name, re.IGNORECASE):
        return f'PUNTERA'
    elif re.search(r'^ESTAD', col_name, re.IGNORECASE):
        return f'ESTADISTICO'
    elif re.search(r'^TEM', col_name, re.IGNORECASE):
        return f'ITEM'
    elif re.search(r'^Descripc', col_name, re.IGNORECASE):
        return f'DESCRIPCION'
    else:
        return col_name
try:
    url = st.text_input('Ingrese url del drive de cabeceras')
    sheet = url.split('/')[-2]
    gs = gc.open_by_key(sheet)
except PermissionError:
    st.write('Recordar dar permisos al siguiente usuario:')
    st.write('cuenta-para-flujo-promos@copper-eye-403311.iam.gserviceaccount.com')
    st.stop()
except:
    st.write('Aún no se ingresó una url correcta')
    st.stop()

if 'df_cabeceras' not in st.session_state:
    meses = {"Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4, "Mayo": 5, "Junio": 6, "Julio": 7,
            "Agosto": 8, "Septiembre": 9, "Octubre": 10, "Noviembre": 11, "Diciembre": 12}
    mes = st.selectbox('Seleccione mes: ', meses.keys())
    fecha = date.today().replace(month=meses[mes])

    if fecha <= date.today():
        fecha = fecha.replace(year=date.today().year + 1)

    fecha_inicio = fecha.replace(day=1).strftime('%Y-%m-%d')
    fecha_fin = fecha.replace(day=calendar.monthrange(date.today().replace(month=fecha.month).year,
                                                    date.today().replace(month=fecha.month).month)[1]).strftime('%Y-%m-%d')
    st.write('Fecha inicio: ' + fecha_inicio)
    st.write('Fecha fin: ' + fecha_fin)

    df_total = pd.DataFrame()

    # Obtener todas las hojas en el Google Sheet
    hojas = gs.worksheets()

    # Iterar sobre cada hoja
    for hoja in hojas:
        # Convertir la hoja actual a DataFrame
        worksheetL = gs.worksheet(hoja.title)
        juli0 = get_as_dataframe(worksheetL, header=None)
        
        # Encontrar la fila que contiene la palabra "EXHIBICION" en la primera columna
        col_0 = juli0.iloc[:, 0].astype(str)  # Ensure column 0 is all strings

        mask = col_0.str.contains("EXHIBICIÓN", na=False)

        condicion = juli0[mask].index[0] if mask.any() else None

        if condicion is not None:
            # Filtrar las filas a partir de la condición
            df = juli0.loc[condicion:, 0:5]
            df.columns = df.iloc[0]  # Definir la primera fila como encabezado
            df = df[1:].reset_index(drop=True)  # Reiniciar el índice
            
            # Renombrar columnas del DataFrame
            df.rename(columns=renombrar_columnas, inplace=True)

            df = df.dropna(subset=['PUNTERA'])
            
            # Limpiar columnas antes de convertir a enteros
            for col in ['ESTADISTICO', 'ITEM', 'LOCAL']:
                df[col] = df[col].astype(str)
                df[col] = df[col].str.replace('.', '', regex=False).fillna('0')
                df[col] = df[col].str.replace('nan', '0', regex=False).fillna('0')
                #df[col] = df[col].astype(int)
            df['SECCION'] = hoja.title

            # Concatenar el DataFrame actual con el total
            df_total = pd.concat([df_total, df], ignore_index=True).drop_duplicates()

    #Definimos las columnas que nos interesan
    df_total = df_total[['PUNTERA', 'ESTADISTICO', 'ITEM', 'DESCRIPCION', 'LOCAL', 'SECCION']]

    # Mostrar las primeras filas del DataFrame total
    st.write('Cantidad de registros: ' + str(df_total.shape[0]))
    df_total['FECHA_INICIO'] = fecha_inicio
    df_total['FECHA_FIN'] = fecha_fin
    st.dataframe(df_total)

    st.write('')
    st.write('Articulos repetidos en puntera:')
    rep = df_total[['LOCAL', 'PUNTERA', 'ITEM']]
    if len(df_total.loc[rep[rep.duplicated(keep=False)].index].sort_values(['LOCAL', 'PUNTERA', 'ITEM'])) > 0:
        st.dataframe(df_total.loc[rep[rep.duplicated(keep=False)].index].sort_values(['LOCAL', 'PUNTERA',
                                                                                    'ITEM']))
    else:
        st.write('No hay')

    st.write('')
    st.write('Articulos en varias punteras de un solo local:')
    rep = df_total[['LOCAL', 'ITEM']]
    if len(df_total.loc[rep[rep.duplicated(keep=False)].index].sort_values(['LOCAL', 'ITEM'])) > 0:
        st.dataframe(df_total.loc[rep[rep.duplicated(keep=False)].index].sort_values(['LOCAL', 'ITEM']))
    else:
        st.write('No hay')
    cont = st.button('Continuar con la tabla anterior')
    if cont:
        st.session_state.df_cabeceras = df_total

if 'df_cabeceras' not in st.session_state:
    st.stop()

if 'cabeceras_carga' not in st.session_state:
    fecha_inicio = st.session_state.df_cabeceras['FECHA_INICIO'].values[0]
    cursor.execute("SELECT * FROM SANDBOX_PLUS.DWH.INPUT_PUNTERAS WHERE FECHA_INICIO = '" + fecha_inicio + "';")
    df_old = cursor.fetch_pandas_all()
    if len(df_old) > 0:
        st.write('Ya existen registros cargados en la base de datos de cabeceras para este período')
        b = st.button('Pisar los datos')
        if b:
            cursor.execute("DELETE FROM SANDBOX_PLUS.DWH.INPUT_PUNTERAS WHERE FECHA_INICIO = '" +
                        fecha_inicio + "';")
            
            st.write('REGISTROS BORRADOS')
            success = carga_snow_generic(st.session_state.df_cabeceras.astype({'ESTADISTICO':'int64',
                                                                               'ITEM':'int64', 'LOCAL':'int64'}),
                                        ctx=snow, database='SANDBOX_PLUS', table='INPUT_PUNTERAS', schema='DWH')
            if success:
                st.write('TABLA CARGADA')
                st.session_state.cabeceras_carga = success
    else:
        b = st.button('Cargar la tabla')
        if b:
            success = carga_snow_generic(st.session_state.df_cabeceras.astype({'ESTADISTICO':'int64',
                                                                               'ITEM':'int64', 'LOCAL':'int64'}),
                                        ctx=snow, database='SANDBOX_PLUS', table='INPUT_PUNTERAS', schema='DWH')
            if success:
                st.write('TABLA CARGADA')
                st.session_state.cabeceras_carga = success

if 'cabeceras_carga' not in st.session_state:
    st.stop()

secciones = st.multiselect('Seleccione las secciones que desea procesar', st.session_state.df_cabeceras['SECCION'].unique(),
                           default=st.session_state.df_cabeceras['SECCION'].unique())
p = st.button('Obtener la participacion')
if p:
    df_final = participacion(cursor=cursor, fecha_inicio=st.session_state.df_cabeceras['FECHA_INICIO'].values[0],
                             punt=st.session_state.df_cabeceras[(st.session_state.df_cabeceras['SECCION'].isin(secciones)) &
                                                                (~st.session_state.df_cabeceras['PUNTERA'].str.contains('EXH'))])
    st.dataframe(df_final)