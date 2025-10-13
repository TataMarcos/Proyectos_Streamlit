import pandas as pd
from utils import snowflake_login, carga_snow_generic, enviar_email
import streamlit as st
import json
import os

st.title('Posicionamiento')

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

#Cargamos el archivo
uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

#Lo leemos
if uploaded_file is None:
    st.stop()
if uploaded_file is not None:
    sheet = pd.ExcelFile(uploaded_file)
try:
    # Definir valores fijos
    margen_min_default = {'referente': 0.00, 'mercado': 0.05, 'surtido': 0.05}
    margen_max_default = 'margen_max_default'
    variacion_max = 0.5
    n_de_cambios_max = 5

    # Construir la estructura
    estructura = {}
    snow_up = pd.DataFrame()

    for c in sheet.sheet_names[1:]:
        df = pd.read_excel(uploaded_file, sheet_name=c).astype({'GEOG_LOCL_ID':'str'})
        df_gen = pd.read_excel(uploaded_file, sheet_name='general')
        df_gen = df_gen[df_gen['Canasta']==c]
        df.loc[df[df['price_gap_pond'].isna()].index,
            'price_gap_pond'] = float(str(df_gen[(df_gen['Canasta']==c)]['Ponderado_canasta'].values[0]))/100
        for f in df_gen.columns[1:-2]:
            if str(df_gen[f].values[0]).find('-') != -1:
                df.loc[df[(df['GRUPO']==f) & (df['pos_min'].isna())].index,
                    'pos_min'] = int(str(df_gen[(df_gen['Canasta']==c)][f].values[0]).split('-')[0])/100
                df.loc[df[(df['GRUPO']==f) & (df['pos_max'].isna())].index,
                    'pos_max'] = int(str(df_gen[(df_gen['Canasta']==c)][f].values[0]).split('-')[1])/100
            else:
                df.loc[df[(df['GRUPO']==f) & (df['pos_min'].isna())].index,
                    'pos_min'] = int(str(df_gen[(df_gen['Canasta']==c)][f].values[0]))/100
                df.loc[df[(df['GRUPO']==f) & (df['pos_max'].isna())].index,
                    'pos_max'] = int(str(df_gen[(df_gen['Canasta']==c)][f].values[0]))/100
        
        aux = df[['GEOG_LOCL_ID', 'GEOG_LOCL_COD', 'pos_min', 'pos_max']]
        aux['CANASTA'] = c
        snow_up = pd.concat([snow_up, aux], ignore_index=True)
        for g in df['GEOG_LOCL_ID'].values:
            geog_id = g
            canasta = c
            
            # Si el geog_local_id no existe, inicializarlo
            if geog_id not in estructura:
                estructura[geog_id] = {}
            
            # Agregar la canasta con sus valores
            estructura[geog_id][canasta] = {
                "margen_min": margen_min_default[canasta],
                "margen_max": margen_max_default,
                "pos_min": df[df['GEOG_LOCL_ID']==g]["pos_min"].values[0],
                "pos_max": df[df['GEOG_LOCL_ID']==g]["pos_max"].values[0],
                "variacion_max": variacion_max,
                "n_de_cambios_max": n_de_cambios_max,
                "price_gap_pond": df[df['GEOG_LOCL_ID']==g]["price_gap_pond"].values[0]
            }
    #Mostramos el json final
    st.json(estructura)

    # Guardar la estructura en un archivo JSON
    # output_path = "posicionamiento.json"
    # with open(output_path, "w", encoding="utf-8") as f:
    #     json.dump(estructura, f, indent=4, ensure_ascii=False)

    # st.write(f"Estructura guardada en {output_path} y enviada por mail")

    # enviar_email(sender='marcos.larran@tata.com.uy', subject='Posicionamiento nuevo', files=[output_path],
    #              receiver=["ds-team@gdn.com.uy", 'marcela.moreira@tata.com.uy', 'nahuel.hartwig@tata.com.uy'],
    #              body='Se envía posicionamiento nuevo. Corregir y enviar a Fernando Salvarezza.')
    # os.remove(output_path)

    #Realizamos correcciones manuales
    st.write('')
    st.write('Corregir archivo recibido reemplazando "margen_max_default" por margen_max_default (sin comillas)')
    st.write('Luego, enviar a Fernando Salvarezza.')

    #Armamos tabla para subir a snowflake
    snow_up['POSICIONAMIENTO_OBJETIVO'] = pd.NA
    for i in range(snow_up.shape[0]):
        if snow_up.loc[i, 'pos_min'] == snow_up.loc[i, 'pos_max']:
            snow_up.loc[i, 'POSICIONAMIENTO_OBJETIVO'] = str(snow_up.loc[i, 'pos_min'])
        else:
            snow_up.loc[i, 'POSICIONAMIENTO_OBJETIVO'] = str(snow_up.loc[i, 'pos_min']) + '-' + str(snow_up.loc[i, 'pos_max'])
    
    snow_up.columns = snow_up.columns.str.upper()
    
    #Mostramos el dataframe a subir
    st.write('')
    st.write('Tabla a subir:')
    st.dataframe(snow_up)

    #Eliminamos lo que está cargado actualmente
    cursor.execute('DELETE FROM SANDBOX_PLUS.DWH.POSICIONAMIENTO;')
    
    #Subimos nueva tabla
    success = carga_snow_generic(df=snow_up, ctx=snow, table='POSICIONAMIENTO',
                                 database='SANDBOX_PLUS', schema='DWH')
    if success:
        st.write('Tabla subida')
except Exception as e:
    st.write('Se cargó un archivo con un formato erróneo: ', e)