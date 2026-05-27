import pandas as pd
from utils import carga_snow_generic, enviar_email, get_credentials, snowflake_login
import streamlit as st
import json
import os

st.set_page_config(page_title="Posicionamiento", page_icon="📊", layout="wide")

st.title("📊 Posicionamiento")
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

st.info('Cargá el archivo Excel de posicionamiento (con hoja "general" y una hoja por canasta).')
uploaded_file = st.file_uploader("Cargar archivo", type="xlsx")

if uploaded_file is None:
    st.stop()

sheet = pd.ExcelFile(uploaded_file)

try:
    margen_min_default = {'referente': 0.00, 'mercado': 0.05, 'surtido': 0.05}
    margen_max_default = 'margen_max_default'
    variacion_max = 0.5
    n_de_cambios_max = 5

    estructura = {}
    snow_up = pd.DataFrame()

    for c in sheet.sheet_names[1:]:
        df = pd.read_excel(uploaded_file, sheet_name=c).astype({'GEOG_LOCL_ID': 'str'})
        df_gen = pd.read_excel(uploaded_file, sheet_name='general')
        df_gen = df_gen[df_gen['Canasta'] == c]
        df.loc[df[df['price_gap_pond'].isna()].index, 'price_gap_pond'] = \
            float(str(df_gen[(df_gen['Canasta'] == c)]['Ponderado_canasta'].values[0]).replace(',', '.')) / 100
        for f in df_gen.columns[1:-2]:
            if str(df_gen[f].values[0]).find('-') != -1:
                df.loc[df[(df['GRUPO'] == f) & (df['pos_min'].isna())].index, 'pos_min'] = \
                    int(str(df_gen[(df_gen['Canasta'] == c)][f].values[0]).split('-')[0]) / 100
                df.loc[df[(df['GRUPO'] == f) & (df['pos_max'].isna())].index, 'pos_max'] = \
                    int(str(df_gen[(df_gen['Canasta'] == c)][f].values[0]).split('-')[1]) / 100
            else:
                df.loc[df[(df['GRUPO'] == f) & (df['pos_min'].isna())].index, 'pos_min'] = \
                    int(str(df_gen[(df_gen['Canasta'] == c)][f].values[0])) / 100
                df.loc[df[(df['GRUPO'] == f) & (df['pos_max'].isna())].index, 'pos_max'] = \
                    int(str(df_gen[(df_gen['Canasta'] == c)][f].values[0])) / 100

        aux = df[['GEOG_LOCL_ID', 'GEOG_LOCL_COD', 'pos_min', 'pos_max']]
        aux['CANASTA'] = c
        snow_up = pd.concat([snow_up, aux], ignore_index=True)

        for g in df['GEOG_LOCL_ID'].values:
            if g not in estructura:
                estructura[g] = {}
            estructura[g][c] = {
                "margen_min": margen_min_default[c],
                "margen_max": margen_max_default,
                "pos_min": df[df['GEOG_LOCL_ID'] == g]["pos_min"].values[0],
                "pos_max": df[df['GEOG_LOCL_ID'] == g]["pos_max"].values[0],
                "variacion_max": variacion_max,
                "n_de_cambios_max": n_de_cambios_max,
                "price_gap_pond": df[df['GEOG_LOCL_ID'] == g]["price_gap_pond"].values[0]
            }

    st.divider()
    st.subheader("Estructura generada")
    with st.expander("Ver JSON completo"):
        st.json(estructura)

    output_path = "posicionamiento.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(estructura, f, indent=4, ensure_ascii=False)

    with st.spinner('Enviando JSON por correo...'):
        enviar_email(
            sender='marcos.larran@tata.com.uy',
            subject='Posicionamiento nuevo',
            files=[output_path],
            receiver=["ds-team@gdn.com.uy", 'marcela.moreira@tata.com.uy', 'nahuel.hartwig@tata.com.uy'],
            body='Se envía posicionamiento nuevo. Corregir y enviar a Fernando Salvarezza.'
        )
    os.remove(output_path)

    st.success("Estructura guardada y enviada por correo.")
    st.warning(
        'Recordá corregir el archivo recibido: reemplazá `"margen_max_default"` '
        'por margen_max_default (sin comillas). Luego enviárselo a Fernando Salvarezza.'
    )

    st.divider()
    st.subheader("Tabla de posicionamiento a subir")
    snow_up['POSICIONAMIENTO_OBJETIVO'] = pd.NA
    for i in range(snow_up.shape[0]):
        if snow_up.loc[i, 'pos_min'] == snow_up.loc[i, 'pos_max']:
            snow_up.loc[i, 'POSICIONAMIENTO_OBJETIVO'] = str(snow_up.loc[i, 'pos_min'])
        else:
            snow_up.loc[i, 'POSICIONAMIENTO_OBJETIVO'] = (str(snow_up.loc[i, 'pos_min']) + '-' +
                                                          str(snow_up.loc[i, 'pos_max']))
    snow_up.columns = snow_up.columns.str.upper()
    st.dataframe(snow_up, use_container_width=True)

    with st.spinner('Actualizando tabla de posicionamiento en Snowflake...'):
        cursor.execute('DELETE FROM SANDBOX_PLUS.DWH.POSICIONAMIENTO;')
        success = carga_snow_generic(df=snow_up, ctx=snow, table='POSICIONAMIENTO',
                                     database='SANDBOX_PLUS', schema='DWH')

    if success:
        st.divider()
        st.success('Tabla de posicionamiento actualizada correctamente.')

except Exception as e:
    st.error(f'El archivo tiene un formato erróneo: {e}')
