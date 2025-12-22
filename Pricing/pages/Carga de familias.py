import pandas as pd
import time
from datetime import datetime
import streamlit as st
from utils import snowflake_login, descargar_segmento
from snowflake.connector.pandas_tools import write_pandas

st.title('Carga de familias')

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

familias = st.button("Descargar familias actuales.")

if familias:
    
    familias_previas = descargar_segmento(cursor, 'FAMILIAS')

    # Convertir ITEM a entero
    familias_previas["ITEM"] = familias_previas["ITEM"].astype('int64')
    
    #Mostramos las primeras filas del dataframe y botón de descarga
    st.write('Familias actuales (primeras 10 filas):')
    st.write('')
    st.dataframe(familias_previas.head(10))
    csv = familias_previas.to_csv(index=False)
    st.download_button(label='Descargar tabla', data=csv, file_name='Familias.csv', mime='text/csv')

continuar = st.button("Actualizar familias")
if continuar:
    f = datetime.now().strftime('%Y%m%d_%H%M%S')
    st.session_state.actualizar_familias = f

if 'actualizar_familias' not in st.session_state:
    st.stop()
else:
    st.write(st.session_state.actualizar_familias)

#Cargamos el archivo
st.write('')
st.write('Arrastrá el archivo excel con las siguientes columnas [ITEM, FAMILIA]')
uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

if uploaded_file is not None:
    try:
        df = pd.read_excel(uploaded_file)[['ITEM', 'FAMILIA']].astype({'ITEM':'str'})
        df.columns = df.columns.str.upper()
    except:
        st.write('El archivo no tiene el formato solicitado.')
        st.stop()
else:
    st.stop()

st.dataframe(df)

df = df.dropna().drop_duplicates()

st.write("\n✅ Archivo validado correctamente.")
st.write('')
st.write('El archivo tiene ', len(df), ' combinaciones.')

try:
    df["ITEM"] = df["ITEM"].astype('int64').astype('str')
except:
    st.write('La columna item tiene valores no numéricos. Revisar.')
    st.stop()

df['TABLA'] = st.session_state.actualizar_familias
df['USUARIO'] = user

df = df[['ITEM', 'FAMILIA', 'TABLA', 'USUARIO']]
st.write('')
st.write('El archivo a cargar queda asi:')
st.dataframe(df)

if 'familias_carga' not in st.session_state:
    try:
        success, nchunks, nrows, _ = write_pandas(snow, df, database='SANDBOX_PLUS', schema='DWH',
                                                table_name='INPUT_RELACIONES_ITEM_PARENT_ACTUALIZADOS')
        st.write(f"Éxito: {success}, Chunks: {nchunks}, Filas insertadas: {nrows}")
        st.session_state.familias_carga = True
    except Exception as e:
        st.write(f"Error al cargar en Snowflake: {e}")
        st.stop()

if 'familias_carga' in st.session_state:
    st.write('')
    st.write('Ahora voy a actualizar la info en la base de datos')
    time.sleep(3)
else:
    st.stop()

qa=f"""
MERGE INTO SANDBOX_PLUS.DWH.RELACIONES_ITEM_PARENT_ACTUALIZADOS AS TARGET
USING (
    SELECT
        DISTINCT ITEM, FAMILIA
    FROM
        SANDBOX_PLUS.DWH.INPUT_RELACIONES_ITEM_PARENT_ACTUALIZADOS
    WHERE
        TABLA='{st.session_state.actualizar_familias}') AS SOURCE
ON
    TARGET.ITEM = SOURCE.ITEM
WHEN MATCHED THEN 
    UPDATE SET TARGET.FAMILIA = SOURCE.FAMILIA
WHEN NOT MATCHED THEN 
    INSERT (ITEM, FAMILIA) VALUES (SOURCE.ITEM, SOURCE.FAMILIA);"""

result = cursor.execute(qa)
st.write("Se actualizaron ", result.rowcount, " lineas")

st.write('')
st.write("Programa finalizado. Manteniéndose abierto...")