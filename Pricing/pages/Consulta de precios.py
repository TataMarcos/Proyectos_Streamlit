import pandas as pd
from utils import descargar_segmento, get_credentials, snowflake_login
import streamlit as st
from datetime import datetime, timedelta

st.set_page_config(page_title="Consulta de Precios", page_icon="🔍", layout="wide")

st.title("🔍 Consulta de precios")
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

# Listado de fechas (últimos 365 días)
fechas = []
for i in range((datetime.today().date() - datetime.today().replace(year=datetime.today().date().year - 1).date()).days):
    fechas.append("'" + (datetime.today().replace(year=datetime.today().date().year - 1).date()
                         + timedelta(days=i)).strftime('%Y-%m-%d') + "'")
fechas.reverse()

col_fecha, col_prog = st.columns(2)
with col_fecha:
    fecha = st.selectbox('Fecha de consulta:', options=fechas)
with col_prog:
    prog = st.selectbox('Tipo de consulta:', ['ACTIVOS', 'I+D'])

st.divider()

if prog == 'ACTIVOS':
    st.info('Arrastrá el archivo Excel con las columnas: **[LOCAL, ITEM]**')
    uploaded_file = st.file_uploader("Cargar archivo", type="xlsx")

    if uploaded_file is None:
        st.stop()

    price = pd.read_excel(uploaded_file)
    price.columns = price.columns.str.upper()
    price.dropna(subset=['ITEM', 'LOCAL'], inplace=True)
    price['ITEM'] = price['ITEM'].astype('int64')
    price['LOCAL'] = price['LOCAL'].astype('int64')

    with st.spinner('Consultando precios en Snowflake...'):
        try:
            loc = "('" + "','".join(str(l) for l in price['LOCAL'].unique()) + "')"
            items = "('" + "','".join(str(i) for i in price['ITEM'].unique()) + "')"
            c = 'WHERE LGL.GEOG_LOCL_COD IN ' + loc + " AND LAA.ORIN IN " + items + ';'
            conds = [c, fecha]
            df = descargar_segmento(cursor=cursor, query='PRECIOS', conds=conds).astype({'ORIN': str, 'LOCAL': str})

            price['ORIN'] = price['ITEM'].apply(str)
            price['LOCAL'] = price['LOCAL'].apply(str)
            df_final = price[['ORIN', 'LOCAL']].merge(df, how='left')
            csv = df_final.to_csv(index=False)

            st.metric("Registros encontrados", len(df_final))
            st.dataframe(df_final.head(10), use_container_width=True)
            st.download_button(
                label='⬇️ Descargar tabla',
                data=csv,
                file_name='Precios.csv',
                mime='text/csv'
            )
        except Exception as e:
            st.error(f'El archivo tiene un formato erróneo. Verificá las columnas LOCAL e ITEM. {e}')

if prog == 'I+D':
    st.info('Arrastrá el archivo Excel con la columna: **[ITEM]**')
    uploaded_file = st.file_uploader("Cargar archivo", type="xlsx")

    if uploaded_file is None:
        st.stop()

    price = pd.read_excel(uploaded_file)
    price.columns = price.columns.str.upper()
    price.dropna(subset='ITEM', inplace=True)
    price['ITEM'] = price['ITEM'].astype('int64')

    with st.spinner('Consultando precios I+D en Snowflake...'):
        try:
            items = "('" + "','".join(str(i).strip() for i in price['ITEM'].unique()) + "')"
            c = "WHERE LAA.ORIN IN " + items
            conds = [c, fecha]
            df = descargar_segmento(cursor=cursor, query='PRECIOS - I+D', conds=conds).astype({'ORIN': str})
            csv = df.to_csv(index=False)

            st.metric("Registros encontrados", len(df))
            st.dataframe(df.head(10), use_container_width=True)
            st.download_button(
                label='⬇️ Descargar tabla',
                data=csv,
                file_name='Precios.csv',
                mime='text/csv'
            )
        except:
            st.error('El archivo tiene un formato erróneo. Verificá la columna ITEM.')
