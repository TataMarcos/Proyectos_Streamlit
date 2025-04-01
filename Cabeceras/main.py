import pandas as pd
from datetime import date
import calendar
from utils import snowflake_login, carga_snow_generic
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import re
import streamlit as st
from cabeceras import participacion
import os
import psutil
import time
import keyboard

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

credentials = Credentials.from_service_account_file('api_credentials_2.json', scopes=scopes)

gc = gspread.authorize(credentials)
gauth = GoogleAuth()
drive = GoogleDrive(gauth)

#user, cursor, snow = snowflake_login()
if 'snow' not in st.session_state:
    user, cursor, snow = snowflake_login()
    st.session_state.user = user
    st.session_state.cursor = cursor
    st.session_state.snow = snow
else:
    snow = st.session_state.snow  # Reuse the existing Snowflake session
    user = st.session_state.user
    cursor = st.session_state.cursor

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

sheet = st.text_input('Ingrese drive de cabeceras')
gs = gc.open_by_key(sheet)

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
    condicion = juli0[juli0[0].str.contains("EXHIBICIÓN", na=False)].index[0] if any(juli0[0].str.contains("EXHIBICIÓN", na=False)) else None

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
st.dataframe(df_total.head())

df_total['FECHA_INICIO'] = fecha_inicio
df_total['FECHA_FIN'] = fecha_fin

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

cursor.execute("SELECT * FROM SANDBOX_PLUS.DWH.INPUT_PUNTERAS WHERE FECHA_INICIO = '" +
                        fecha_inicio + "';")
df_old = cursor.fetch_pandas_all()
if len(df_old) > 0:
    st.write('Ya existen registros cargados en la base de datos de cabeceras para este período')
    b = st.button('Pisar los datos')
    if b:
        cursor.execute("DELETE FROM SANDBOX_PLUS.DWH.INPUT_PUNTERAS WHERE FECHA_INICIO = '" +
                       fecha_inicio + "';")
        
        st.write('REGISTROS BORRADOS')
        success = carga_snow_generic(df_total.astype({'ESTADISTICO':'int64', 'ITEM':'int64',
                                                      'LOCAL':'int64'}), ctx=snow,
                                     database='SANDBOX_PLUS', table='INPUT_PUNTERAS', schema='DWH')
        if success:
            st.write('TABLA CARGADA')
else:
    b = st.button('Cargar la tabla')
    if b:
        success = carga_snow_generic(df_total.astype({'ESTADISTICO':'int64', 'ITEM':'int64',
                                                      'LOCAL':'int64'}), ctx=snow,
                                     database='SANDBOX_PLUS', table='INPUT_PUNTERAS', schema='DWH')
        if success:
            st.write('TABLA CARGADA')

secciones = st.multiselect('Seleccione las secciones que desea procesar', df_total['SECCION'].unique(),
                           default=df_total['SECCION'].unique())
p = st.button('Obtener la participacion')
if p:
    df_final = participacion(cursor=cursor, punt=df_total[(df_total['SECCION'].isin(secciones)) &
                                                          (~df_total['PUNTERA'].str.contains('EXH'))],
                             fecha_inicio=fecha_inicio)
    st.dataframe(df_final)

exit_app = st.button("Cerrar el programa.")
if exit_app:
    st.write('Cerrando el programa...')
    # Give a bit of delay for user experience
    time.sleep(5)
    keyboard.press_and_release('ctrl+w')        #Close the window
    # Terminate streamlit python process
    pid = os.getpid()
    p = psutil.Process(pid)
    p.terminate()