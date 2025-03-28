import pandas as pd
from utils import snowflake_login, carga_snow_generic, descargar_segmento, clean_table
import streamlit as st
from margenes import margenes
import os
import psutil
import time
from datetime import date

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
uploaded_file = st.file_uploader("Cargar el archivo", type="excel")

if uploaded_file is not None:
    mg_update = pd.read_excel(uploaded_file)[['ORIN', 'MARGEN OBJETIVO']].astype({'ORIN':'str'})

    #Descargamos tabla de margenes actuales 
    mg_actual = descargar_segmento(cursor=cursor, query='Margenes').astype({'ORIN':'str'})

    p = st.button('Obtener nuevos márgenes')
    if p:
        df_final = margenes(df_actual=mg_actual, df_update=mg_update)
        st.dataframe(df_final)
        sub = st.button('Subir márgenes actualizados')
    if sub:
        df_hist = df_final.copy()
        df_hist['ULTIMA_ACTUALIZACION'] = date.today().strftime('%Y-%m-%d')
        df_hist['REALIZADA_POR'] = user
        success = carga_snow_generic(df=df_final, ctx=snow, database='SANDBOX_PLUS', schema='DWH',
                                     table='INPUT_PRICING_MARGEN_OBJETIVO_HISTORICO')
        clean_table(cursor=cursor, table='INPUT_PRICING_MARGEN_OBJETIVO')
        success = carga_snow_generic(df=df_final, ctx=snow, table='INPUT_PRICING_MARGEN_OBJETIVO',
                                     database='SANDBOX_PLUS', schema='DWH')
        if success:
            st.write('Márgenes actualizados')

    #Armamos bloque para cerrar el programa
    if success or not(sub):
        exit_app = st.button("Cerrar el programa.")
    if exit_app:
        st.write('Cerrando el programa. Espere 10 segundos antes de cerrar la ventana.')
        # Give a bit of delay for user experience
        time.sleep(1)
        # Terminate streamlit python process
        pid = os.getpid()
        p = psutil.Process(pid)
        p.terminate()
else:
    st.write('Aún no se cargó el archivo')