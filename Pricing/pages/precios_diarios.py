import pandas as pd
import os
from utils import snowflake_login, descargar_segmento
import streamlit as st
import time
import psutil

st.set_page_config(page_title='Consulta de precios')

st.title('Consulta de precios')

if 'snow' not in st.session_state:
    user, cursor, snow = snowflake_login()
    st.session_state.user = user
    st.session_state.cursor = cursor
    st.session_state.snow = snow
else:
    snow = st.session_state.snow  # Reuse the existing Snowflake session
    user = st.session_state.user
    cursor = st.session_state.cursor

#Cargamos el archivo
uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

if uploaded_file is not None:
    price = pd.read_excel(uploaded_file)
    price.columns = price.columns.str.upper()
try:
    loc = '('
    for l in price['LOCAL'].unique():
        loc += str(l) + ','
    loc = loc.strip(',')
    loc += ')'

    items = "('"
    for i in price['ITEM'].unique():
        items += str(i) + "','"
    items = items.strip(",'")
    items += "')"

    c = ' AND LGL.GEOG_LOCL_COD IN ' + loc + " AND LAA.ORIN IN " + items + ';'
    df = descargar_segmento(cursor=cursor, query='PRECIOS', cond=c).astype({'ORIN':str, 'LOCAL':str})

    price['ORIN'] = price['ITEM'].apply(str)
    price['LOCAL'] = price['LOCAL'].apply(str)

    df_final = price[['ORIN', 'LOCAL']].merge(df, how='left')

    #Mostramos el dataframe final
    st.dataframe(df_final)
except:
    st.write('Todavía no se cargó el archivo o se acargó un archivo con un formato erróneo')

#Armamos bloque para cerrar el programa
exit_app = st.button("Cerrar el programa.")
if exit_app:
    st.write('Cerrando el programa...')
    # Give a bit of delay for user experience
    time.sleep(1)
    # Terminate streamlit python process
    pid = os.getpid()
    p = psutil.Process(pid)
    p.terminate()