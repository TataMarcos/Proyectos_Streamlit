import pandas as pd
from utils import carga_snow_generic, descargar_segmento, clean_table, get_credentials, snowflake_login
import streamlit as st
from datetime import date

st.set_page_config(page_title="Márgenes Objetivo", page_icon="🎯", layout="wide")

st.title("🎯 Actualización de márgenes objetivo")
st.divider()

# Conexión a Snowflake
credentials_snowflake = get_credentials("snow")

try:
    if 'snow' not in st.session_state:
        user, cursor, snow = snowflake_login(
            user=credentials_snowflake['USER'],
            password=credentials_snowflake['PASS'],
            account=credentials_snowflake['ACCOUNT']
        )
        st.session_state.user = user
        st.session_state.cursor = cursor
        st.session_state.snow = snow
    else:
        snow = st.session_state.snow
        user = st.session_state.user
        cursor = st.session_state.cursor
except:
    st.error('Error de conexión. Verificá las credenciales de Snowflake.')
    st.stop()

st.info('Arrastrá el archivo Excel con las columnas: **[ORIN, MARGEN OBJETIVO]**')
uploaded_file = st.file_uploader("Cargar archivo", type="xlsx")

if uploaded_file is not None:
    try:
        mg_update = pd.read_excel(uploaded_file)[['ORIN', 'MARGEN OBJETIVO']].astype({'ORIN': 'str'})
    except:
        st.error('El archivo no tiene el formato solicitado.')
        st.stop()
else:
    st.stop()

st.success(f"Archivo cargado: {len(mg_update)} registros.")
mg_actual = descargar_segmento(cursor=cursor, query='MARGENES').astype({'ORIN': 'str'})

if st.button('Obtener nuevos márgenes', use_container_width=False):
    df_final = mg_actual.merge(mg_update, how='outer')
    df_final.loc[df_final[~df_final['MARGEN OBJETIVO'].isna()].index, 'MG'] = \
        df_final.loc[df_final[~df_final['MARGEN OBJETIVO'].isna()].index, 'MARGEN OBJETIVO']
    st.session_state.margenes = df_final[['ORIN', 'MG']]

    col_inp, col_out = st.columns(2)
    with col_inp:
        st.subheader("Márgenes ingresados")
        st.dataframe(df_final.dropna(), use_container_width=True)
    with col_out:
        st.subheader("Márgenes actualizados")
        st.dataframe(df_final[['ORIN', 'MG']], use_container_width=True)
else:
    if 'margenes' not in st.session_state:
        st.stop()

st.divider()
if st.button('Subir márgenes actualizados', use_container_width=False):
    df_final = st.session_state.margenes
    df_hist = df_final.copy()
    df_hist['ULTIMA_ACTUALIZACION'] = date.today().strftime('%Y-%m-%d')
    df_hist['REALIZADA_POR'] = user

    with st.spinner('Subiendo datos. No cierre el programa...'):
        success = carga_snow_generic(df=df_hist, ctx=snow, database='SANDBOX_PLUS', schema='DWH',
                                     table='INPUT_PRICING_MARGEN_OBJETIVO_HISTORICO')
        clean_table(cursor=cursor, table='INPUT_PRICING_MARGEN_OBJETIVO')
        success = carga_snow_generic(df=df_final, ctx=snow, table='INPUT_PRICING_MARGEN_OBJETIVO',
                                     database='SANDBOX_PLUS', schema='DWH')

    if success:
        st.divider()
        st.success('Márgenes actualizados correctamente.')
