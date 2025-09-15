import pandas as pd
from utils import snowflake_login, descargar_segmento
import streamlit as st
from datetime import datetime, timedelta

st.title('Consulta de precios')

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
for i in range((datetime.today().date() - datetime.today().replace(year=datetime.today().date().year - 1).date()).days):
    fechas.append("'" + (datetime.today().replace(year=datetime.today().date().year - 1).date()
                         + timedelta(days=i)).strftime('%Y-%m-%d') + "'")
fechas.reverse()

#Seleccionamos la fecha
fecha = st.selectbox('Seleccione fecha:', options=fechas)
st.write(fecha)

#Cargamos el archivo
st.write('Arrastrá el archivo excel con las siguientes columnas [LOCAL,ITEM]')
uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

#Lo leemos
if uploaded_file is None:
    st.stop()
if uploaded_file is not None:
    price = pd.read_excel(uploaded_file)
    price.columns = price.columns.str.upper()

#Hacemos listado de locales y orines
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

    #Descargamos de snow
    c = ' AND LGL.GEOG_LOCL_COD IN ' + loc + " AND LAA.ORIN IN " + items + ';'
    conds = []
    conds.append(c)
    conds.append(fecha)
    df = descargar_segmento(cursor=cursor, query='PRECIOS', conds=conds).astype({'ORIN':str, 'LOCAL':str})

    price['ORIN'] = price['ITEM'].apply(str)
    price['LOCAL'] = price['LOCAL'].apply(str)

    df_final = price[['ORIN', 'LOCAL']].merge(df, how='left')

    #Mostramos el dataframe final
    st.dataframe(df_final)
except:
    st.write('Se cargó un archivo con un formato erróneo')