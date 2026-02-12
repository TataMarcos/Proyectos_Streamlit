import pandas as pd
from utils import carga_snow_generic, descargar_segmento, clean_table, get_credentials, snowflake_login
import streamlit as st
from datetime import date

st.title('Actualización de márgenes objetivo')

#Conectamos a snowflake
credentials_snowflake = get_credentials("snow")

try:
    if 'snow' not in st.session_state:
        user, cursor, snow = snowflake_login(
                                    user = credentials_snowflake['USER'],
                                    password = credentials_snowflake['PASS'],
                                    account = credentials_snowflake['ACCOUNT']
                                    )
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

#Cargamos el archivo
st.write('Arrastrá el archivo excel con las siguientes columnas [ORIN,MARGEN OBJETIVO]')
uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

if uploaded_file is not None:
    try:
        mg_update = pd.read_excel(uploaded_file)[['ORIN', 'MARGEN OBJETIVO']].astype({'ORIN':'str'})
    except:
        st.write('El archivo no tiene el formato solicitado.')
        st.stop()
else:
    st.stop()

#Descargamos tabla de margenes actuales 
mg_actual = descargar_segmento(cursor=cursor, query='MARGENES').astype({'ORIN':'str'})

p = st.button('Obtener nuevos márgenes')
if p:
    df_final = mg_actual.merge(mg_update, how='outer')
    df_final.loc[df_final[~df_final['MARGEN OBJETIVO'].isna()].index,
                'MG'] = df_final.loc[df_final[~df_final['MARGEN OBJETIVO'].isna()].index,
                                    'MARGEN OBJETIVO']
    st.write('Margenes ingresados')
    st.dataframe(df_final.dropna())
    st.write('Margenes actualizados')
    st.dataframe(df_final[['ORIN', 'MG']])
    st.session_state.df_final = df_final[['ORIN', 'MG']]
else:
    st.stop()

sub = st.button('Subir márgenes actualizados')
if sub:
    df_final = st.session_state.df_final
    df_hist = df_final.copy()
    df_hist['ULTIMA_ACTUALIZACION'] = date.today().strftime('%Y-%m-%d')
    df_hist['REALIZADA_POR'] = user
    st.write('Subiendo los datos. No cierre el programa.')
    success = carga_snow_generic(df=df_hist, ctx=snow, database='SANDBOX_PLUS', schema='DWH',
                                 table='INPUT_PRICING_MARGEN_OBJETIVO_HISTORICO')
    clean_table(cursor=cursor, table='INPUT_PRICING_MARGEN_OBJETIVO')
    success = carga_snow_generic(df=df_final, ctx=snow, table='INPUT_PRICING_MARGEN_OBJETIVO',
                                 database='SANDBOX_PLUS', schema='DWH')
    if success:
        st.write('Márgenes actualizados')