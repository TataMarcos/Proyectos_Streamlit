import pandas as pd
import time
from datetime import datetime
import streamlit as st
from utils import descargar_segmento, snowflake_login, get_credentials, carga_snow_generic

st.set_page_config(page_title="Familias", page_icon="👥", layout="wide")

st.title("👥 Carga de familias")
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

col_dl, col_act = st.columns(2)

with col_dl:
    if st.button("Descargar familias actuales", use_container_width=True):
        with st.spinner('Descargando familias...'):
            familias_previas = descargar_segmento(cursor, 'FAMILIAS')
            familias_previas["ITEM"] = familias_previas["ITEM"].astype('int64')
        st.metric("Familias actuales", len(familias_previas))
        st.dataframe(familias_previas.head(10), use_container_width=True)
        st.download_button(
            label='⬇️ Descargar tabla',
            data=familias_previas.to_csv(index=False),
            file_name='Familias.csv',
            mime='text/csv'
        )

with col_act:
    if st.button("Actualizar familias", use_container_width=True):
        f = datetime.now().strftime('%Y%m%d_%H%M%S')
        st.session_state.actualizar_familias = f

if 'actualizar_familias' not in st.session_state:
    st.stop()

st.divider()
st.info('Arrastrá el archivo Excel con las columnas: **[ITEM, FAMILIA]**')
uploaded_file = st.file_uploader("Cargar archivo", type="xlsx")

if uploaded_file is not None:
    try:
        df = pd.read_excel(uploaded_file)[['ITEM', 'FAMILIA']].astype({'ITEM': 'str'})
        df.columns = df.columns.str.upper()
    except:
        st.error('El archivo no tiene el formato solicitado.')
        st.stop()
else:
    st.stop()

st.dataframe(df, use_container_width=True)

df = df.dropna().drop_duplicates()

try:
    df["ITEM"] = df["ITEM"].astype('int64').astype('str')
except:
    st.error('La columna ITEM tiene valores no numéricos. Revisá el archivo.')
    st.stop()

st.success("Archivo validado correctamente.")
st.metric("Combinaciones a cargar", len(df))

df['TABLA'] = st.session_state.actualizar_familias
df['USUARIO'] = user
df = df[['ITEM', 'FAMILIA', 'TABLA', 'USUARIO']]

with st.expander("Vista previa del archivo a cargar"):
    st.dataframe(df, use_container_width=True)

if 'familias_carga' not in st.session_state:
    with st.spinner('Cargando tabla auxiliar en Snowflake...'):
        try:
            success, nchunks, nrows, _ = carga_snow_generic(
                df=df, ctx=snow, database='SANDBOX_PLUS', schema='DWH',
                table='INPUT_RELACIONES_ITEM_PARENT_ACTUALIZADOS'
            )
            st.success(f"Tabla cargada: {nrows} filas insertadas.")
            st.session_state.familias_carga = True
        except Exception as e:
            st.error(f"Error al cargar en Snowflake: {e}")
            st.stop()

if 'familias_carga' not in st.session_state:
    st.stop()

with st.spinner('Actualizando familias en la base de datos...'):
    time.sleep(3)
    qa = f"""
MERGE INTO SANDBOX_PLUS.DWH.RELACIONES_ITEM_PARENT_ACTUALIZADOS AS TARGET
USING (
    SELECT DISTINCT ITEM, FAMILIA
    FROM SANDBOX_PLUS.DWH.INPUT_RELACIONES_ITEM_PARENT_ACTUALIZADOS
    WHERE TABLA='{st.session_state.actualizar_familias}'
) AS SOURCE
ON TARGET.ITEM = SOURCE.ITEM
WHEN MATCHED THEN
    UPDATE SET TARGET.FAMILIA = SOURCE.FAMILIA
WHEN NOT MATCHED THEN
    INSERT (ITEM, FAMILIA) VALUES (SOURCE.ITEM, SOURCE.FAMILIA);"""
    result = cursor.execute(qa)

st.divider()
st.success(f"Se actualizaron {result.rowcount} líneas.")
