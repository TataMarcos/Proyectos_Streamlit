from utils import get_credentials, snowflake_login, enviar_email
import pandas as pd
import numpy as np
from snowflake.connector.pandas_tools import write_pandas
from config import *
import streamlit as st
from datetime import datetime, timedelta

# Realizamos conexion
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

#Aramamos listado de fechas
fechas = []
for i in range((datetime.today().replace(year=datetime.today().date().year + 1).date() -
                datetime.today().date()).days):
    fechas.append("'" + (datetime.today().date()
                         + timedelta(days=i)).strftime('%Y-%m-%d') + "'")

#Seleccionamos las fechas iniciales y finales
fini = st.selectbox('Seleccione fecha:', options=fechas, key=1)
ffin = st.selectbox('Seleccione fecha:', options=fechas, key=2)
try:
    if datetime.strptime(fini.strip("'"), "%Y-%m-%d") < datetime.strptime(ffin.strip("'"), "%Y-%m-%d"):
        st.write('Fecha inicial: ', fini)
        st.write('Fecha final: ', ffin)
    else:
        st.stop()
except:
    st.stop()

#Cargamos el archivo
st.write('Arrastrá el archivo excel con las siguientes columnas [GEOG_LOCL_COD,ITEM]')
uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

#Lo leemos
if uploaded_file is None:
    st.stop()
if uploaded_file is not None:
    df = pd.read_excel(uploaded_file)

#Damos formato a las columnas
df.columns = df.columns.str.upper()
df.columns = df.columns.str.strip()
cols = {'ITEM':'ORIN', 'LOCAL':'GEOG_LOCL_COD', 'LOCALES':'GEOG_LOCL_COD'}
df.rename(columns=cols, inplace=True)

if 'GEOG_LOCL_COD' in df.columns:
    evento = df[['ORIN', 'GEOG_LOCL_COD']].drop_duplicates().astype('str')
else:
    evento = df[['ORIN']].drop_duplicates()
    evento['aux'] = 1
    locales = pd.read_excel('LOCALES.xlsx')
    locales['aux'] = 1
    evento = evento.merge(locales).drop(columns='aux').astype('str')

#Descargamos estadisticos
cursor.execute('SELECT ARTC_ARTC_COD, ORIN FROM MSTRDB.DWH.LU_ARTC_ARTICULO;')
art = cursor.fetch_pandas_all().astype('str')

#Incluimos
evento = evento.merge(art)

#Agregamos columnas
evento['PROM_FECHA_INICIO'] = fini
evento['PROM_FECHA_INICIO'] = pd.to_datetime(evento['PROM_FECHA_INICIO'])
evento['PROM_FECHA_FIN'] = ffin
evento['PROM_FECHA_FIN'] = pd.to_datetime(evento['PROM_FECHA_FIN'])
evento['EVENTO_ID'] = 2547
evento['PRONOSTICO_VENTA'] = 0
evento['STOCK_INICIAL_PROMO'] = 0
evento['PROM_PVP_OFERTA'] = 0
evento['PROM_LOCAL_ACTIVO'] = 0
evento['PROM_ESTIBA'] = 0

#Armamos tabla final
evento_final = evento[['PROM_FECHA_INICIO', 'PROM_FECHA_FIN', 'ARTC_ARTC_COD', 'EVENTO_ID', 'PRONOSTICO_VENTA',
                       'STOCK_INICIAL_PROMO', 'GEOG_LOCL_COD', 'PROM_PVP_OFERTA', 'PROM_LOCAL_ACTIVO', 'PROM_ESTIBA',
                       'ORIN']].astype({'ARTC_ARTC_COD':'str', 'GEOG_LOCL_COD':'str', 'ORIN':'str'})
st.write('')
st.write('Evento a cargar:')
st.dataframe(evento_final)

#Exportamos
try:
    evento_final.to_csv('G:/Unidades compartidas/Inteligencia de Negocio/Promos/Eventos adhoc/Evento adhoc.csv')
except:
    st.write('')
    st.write('Descargar tabla y cargarla en https://drive.google.com/drive/folders/1tjs9xYhatKZ8e9MnTWPZhrovlcVyp53b')